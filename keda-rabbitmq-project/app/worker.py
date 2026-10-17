
import os
import time
import json
import copy
import logging

import pika
from prometheus_client import Counter, Histogram, start_http_server

from task_ledger import (
    initialize_schema,
    claim_task,
    record_retry,
    record_processed,
    record_failed,
    record_received,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("worker")

MESSAGES_RECEIVED = Counter(
    "worker_messages_received_total",
    "Message deliveries observed by this worker",
)

MESSAGES_PROCESSED = Counter(
    "worker_messages_processed_total",
    "Successful task completions observed by this worker",
)

MESSAGES_FAILED = Counter(
    "worker_messages_failed_total",
    "Task processing attempts that failed",
)

PROCESSING_TIME = Histogram(
    "worker_message_processing_seconds",
    "Time spent processing each message",
)

host = os.getenv(
    "RABBITMQ_HOST",
    "rabbitmq.messaging.svc.cluster.local",
)
username = os.environ["RABBITMQ_USER"]
password = os.environ["RABBITMQ_PASSWORD"]
queue = os.getenv("QUEUE_NAME", "tasks")

MAX_RETRIES = 3
RETRY_DELAY_MS = 10_000

RETRY_QUEUE = f"{queue}.retry"
DLQ_QUEUE = f"{queue}.dlq"


def process_task(task):
    """Replace this simulation with your real task handler."""
    log.info("Processing task_id=%s", task["task_id"])
    time.sleep(5)

    if str(task.get("task", "")).startswith("FAIL"):
        raise ValueError("Simulated processing failure")

    log.info("Task completed: %s", task["task_id"])

def publish_reliably(
    channel,
    destination,
    body,
    properties,
    retry_count=None,
):
    """Publish a persistent message and require broker confirmation."""
    headers = dict(
        getattr(properties, "headers", None) or {}
    )

    if retry_count is not None:
        headers["retry_count"] = retry_count

    new_properties = copy.copy(
        properties if properties is not None
        else pika.BasicProperties()
    )
    new_properties.headers = headers
    new_properties.delivery_mode = 2

    # confirm_delivery() is enabled on this channel.
    # A broker NACK or unroutable publication raises an exception.
    channel.basic_publish(
        exchange="",
        routing_key=destination,
        body=body,
        properties=new_properties,
        mandatory=True,
    )


def declare_queues(channel):
    """Declare the durable work, retry, and dead-letter queues."""
    channel.queue_declare(
        queue=queue,
        durable=True,
    )

    channel.queue_declare(
        queue=RETRY_QUEUE,
        durable=True,
        arguments={
            "x-message-ttl": RETRY_DELAY_MS,
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": queue,
        },
    )

    channel.queue_declare(
        queue=DLQ_QUEUE,
        durable=True,
    )

    channel.basic_qos(prefetch_count=1)


def main():
    start_http_server(8000)
    log.info("Prometheus metrics available on port 8000")

    # Do not consume messages until PostgreSQL is available.
    while True:
        try:
            initialize_schema()
            log.info("PostgreSQL task ledger is ready")
            break
        except Exception:
            log.exception(
                "Cannot initialize task ledger; retrying"
            )
            time.sleep(5)

    while True:
        connection = None

        try:
            credentials = pika.PlainCredentials(
                username,
                password,
            )

            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=host,
                    port=5672,
                    virtual_host="/",
                    credentials=credentials,
                    heartbeat=30,
                    blocked_connection_timeout=60,
                    connection_attempts=3,
                    retry_delay=5,
                )
            )

            channel = connection.channel()

            # Publisher confirms protect retry/DLQ handoffs.
            channel.confirm_delivery()
            declare_queues(channel)

            def process_message(ch, method, properties, body):
                started = time.perf_counter()
                task_id = None

                MESSAGES_RECEIVED.inc()

                try:
                    task = json.loads(body.decode("utf-8"))

                    if not isinstance(task, dict):
                        raise ValueError(
                            "Task message must be a JSON object"
                        )

                    task_id = task.get("task_id")

                    if (
                        not isinstance(task_id, str)
                        or not task_id.strip()
                    ):
                        raise ValueError(
                            "Message must contain a non-empty task_id"
                        )

                    # Record task identity before executing work.
                    record_received(task_id)

                    # Do not repeat a task whose success is durable.
                    if not claim_task(task_id):
                        log.info(
                            "Task already terminal; ACK redelivery: %s",
                            task_id,
                        )
                        ch.basic_ack(
                            delivery_tag=method.delivery_tag
                        )
                        return

                except (
                    json.JSONDecodeError,
                    UnicodeDecodeError,
                    ValueError,
                ):
                    log.exception(
                        "Invalid task message; sending to DLQ"
                    )

                    try:
                        publish_reliably(
                            ch,
                            DLQ_QUEUE,
                            body,
                            properties,
                        )
                        ch.basic_ack(
                            delivery_tag=method.delivery_tag
                        )
                    except Exception:
                        log.exception(
                            "DLQ publication failed; requeueing original"
                        )
                        try:
                            ch.basic_nack(
                                delivery_tag=method.delivery_tag,
                                requeue=True,
                            )
                        except Exception:
                            log.exception(
                                "Could not requeue invalid message"
                            )
                    return

                except Exception:
                    log.exception(
                        "Cannot record/read task ledger"
                    )
                    try:
                        ch.basic_nack(
                            delivery_tag=method.delivery_tag,
                            requeue=True,
                        )
                    except Exception:
                        log.exception(
                            "Could not requeue task after ledger error"
                        )
                    return

                try:
                    process_task(task)

                except Exception as exc:
                    MESSAGES_FAILED.inc()
                    log.exception(
                        "Task processing attempt failed: %s",
                        task_id,
                    )

                    try:
                        headers = (
                            getattr(properties, "headers", None) or {}
                        )
                        retry_count = int(
                            headers.get("retry_count", 0)
                        )

                        if retry_count < MAX_RETRIES:
                            next_attempt = retry_count + 1

                            # Publish the retry with publisher confirmation.
                            publish_reliably(
                                ch,
                                RETRY_QUEUE,
                                body,
                                properties,
                                retry_count=next_attempt,
                            )

                            # Persist the retry count and error before ACK.
                            record_retry(
                                task_id,
                                next_attempt,
                                str(exc),
                            )

                            ch.basic_ack(
                                delivery_tag=method.delivery_tag
                            )

                            log.warning(
                                "Task %s scheduled for retry %s/%s",
                                task_id,
                                next_attempt,
                                MAX_RETRIES,
                            )

                        else:
                            # Confirm DLQ publication before marking terminal failure.
                            publish_reliably(
                                ch,
                                DLQ_QUEUE,
                                body,
                                properties,
                            )

                            record_failed(task_id, str(exc))

                            ch.basic_ack(
                                delivery_tag=method.delivery_tag
                            )

                            log.error(
                                "Task %s exhausted retries; sent to DLQ",
                                task_id,
                            )

                    except Exception:
                        log.exception(
                            "Retry/DLQ handoff failed; requeueing original"
                        )
                        try:
                            ch.basic_nack(
                                delivery_tag=method.delivery_tag,
                                requeue=True,
                            )
                        except Exception:
                            log.exception(
                                "Could not requeue failed task"
                            )

                    return

                else:
                    try:
                        # Persist success before acknowledging RabbitMQ.
                        record_processed(task_id)

                        ch.basic_ack(
                            delivery_tag=method.delivery_tag
                        )

                        MESSAGES_PROCESSED.inc()
                        log.info(
                            "Durable success recorded: %s",
                            task_id,
                        )

                    except Exception:
                        log.exception(
                            "Could not persist success/ACK task: %s",
                            task_id,
                        )
                        try:
                            ch.basic_nack(
                                delivery_tag=method.delivery_tag,
                                requeue=True,
                            )
                        except Exception:
                            log.exception(
                                "Could not requeue task"
                            )

                finally:
                    PROCESSING_TIME.observe(
                        time.perf_counter() - started
                    )

            channel.basic_consume(
                queue=queue,
                on_message_callback=process_message,
                auto_ack=False,
            )

            log.info(
                "Worker ready; consuming queue=%s",
                queue,
            )
            channel.start_consuming()

        except Exception:
            log.exception(
                "RabbitMQ connection or consumer failed"
            )
            time.sleep(5)

        finally:
            if connection is not None and connection.is_open:
                try:
                    connection.close()
                except Exception:
                    log.exception(
                        "Failed to close RabbitMQ connection"
                    )


if __name__ == "__main__":
    main()
