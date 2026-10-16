
import os
import time
import logging
import pika

from prometheus_client import Counter, Histogram, start_http_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("worker")

# Prometheus metrics
MESSAGES_RECEIVED = Counter(
    "worker_messages_received_total",
    "Total messages received by the worker",
)

MESSAGES_PROCESSED = Counter(
    "worker_messages_processed_total",
    "Total messages processed successfully",
)

MESSAGES_FAILED = Counter(
    "worker_messages_failed_total",
    "Total message processing attempts that failed",
)

PROCESSING_TIME = Histogram(
    "worker_message_processing_seconds",
    "Time spent processing each message",
)

host = os.getenv("RABBITMQ_HOST", "rabbitmq.messaging.svc.cluster.local")
username = os.environ["RABBITMQ_USER"]
password = os.environ["RABBITMQ_PASSWORD"]
queue = os.getenv("QUEUE_NAME", "tasks")

# Expose metrics at http://<pod-ip>:8000/metrics
start_http_server(8000)
log.info("Prometheus metrics available on port 8000")

while True:
    try:
        credentials = pika.PlainCredentials(username, password)
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
        channel.queue_declare(queue=queue, durable=True)
        channel.basic_qos(prefetch_count=1)

        def process_message(ch, method, properties, body):
            MESSAGES_RECEIVED.inc()
            started = time.perf_counter()

            try:
                log.info("Processing job: %s", body.decode())
                time.sleep(5)  # Simulate real processing
                if body.decode().startswith("FAIL"):
                 raise ValueError("Simulated failure for monitoring test")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                MESSAGES_PROCESSED.inc()
                log.info("Job completed")

            except Exception:
                MESSAGES_FAILED.inc()
                log.exception("Job processing failed")

                try:
                    ch.basic_nack(
                        delivery_tag=method.delivery_tag,
                        requeue=False,
                    )
                except Exception:
                    log.exception("Failed to reject/requeue message")

            finally:
                PROCESSING_TIME.observe(time.perf_counter() - started)

        channel.basic_consume(
            queue=queue,
            on_message_callback=process_message,
        )

        log.info("Worker ready; waiting for jobs")
        channel.start_consuming()

    except Exception:
        log.exception("RabbitMQ connection or consumer failed")
        time.sleep(5)
