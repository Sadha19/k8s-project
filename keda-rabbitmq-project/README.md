Event-Driven Autoscaling and Failure Handling for a RabbitMQ Worker Pool on Kubernetes
A reliability-focused reference build for a queue-driven worker system. KEDA scales workers on queue depth. Workers handle failures with bounded retries, a dead-letter queue, a durable PostgreSQL task ledger, and Kubernetes health probes. Prometheus and Alertmanager cover the signals an on-call engineer needs.

The project runs on Minikube. It is built to demonstrate operational design: how the system behaves under load, how it fails, how it recovers, and how it reports its state.
What this project demonstrates
Area
What is implemented
Autoscaling
KEDA ScaledObject drives the HPA from RabbitMQ queue length, with explicit scale-up and scale-down policies
Delivery guarantees
Durable queues, persistent messages, publisher confirms, manual acknowledgements, prefetch=1
Failure handling
Retry queue with TTL backoff, bounded retries (3), dead-letter queue for exhausted and malformed messages
Idempotency
PostgreSQL task ledger with a unique task_id. Completed tasks are acknowledged and skipped on redelivery
Health management
Startup, liveness, and readiness endpoints separate from dependency state
Observability
Prometheus metrics from the worker, RabbitMQ, and the ledger database. PrometheusRule alerts and Alertmanager email routing
Security baseline
Non-root containers, read-only root filesystem, dropped capabilities, seccomp RuntimeDefault, secrets kept out of Git

Architecture
flowchart LR

    LG[Load generator] -->|publish JSON, confirmed| Q[(tasks queue)]

    Q -->|deliver, prefetch=1| W[task-worker pods]

    W -->|ack / retry / DLQ| Q

    Q -->|expired after TTL| R[(tasks.retry queue)]

    R -->|dead-letter back| Q

    Q -->|retries exhausted or malformed| D[(tasks.dlq)]

    W -->|record state| DB[(PostgreSQL task ledger)]

    KEDA[KEDA ScaledObject] -->|queue length| Q

    KEDA -->|desired replicas| HPA[HPA] --> W

    W -->|/metrics :8000| P[Prometheus]

    Q -->|/metrics :15692| P

    DB -->|exporter :9187| P

    P -->|alert rules| AM[Alertmanager] -->|email| OC[On-call]

    P --> G[Grafana]
Components
Component
Kubernetes object
Responsibility
Load generator
Pod load-generator
Publishes JSON tasks in four load phases. Every tenth task is a forced failure to exercise the retry path
RabbitMQ
Deployment rabbitmq, Service rabbitmq, PVC
Durable work queue, retry queue, DLQ
Worker
Deployment task-worker
Consumes tasks, records state, acknowledges, exposes metrics and health endpoints
Task ledger
StatefulSet task-ledger-db (PostgreSQL 16)
Durable record of each task's state, attempt count, and error
Ledger exporter
Deployment task-ledger-exporter
Exposes ledger counts as Prometheus gauges
Autoscaler
ScaledObject task-worker-scaler, TriggerAuthentication
Scales workers from 1 to 10 replicas based on queue length
Alerting
PrometheusRule, AlertmanagerConfig
Backlog, worker availability, failure rate, replica ceiling

Message lifecycle
Every task moves through a defined set of states. Each transition is persisted before the RabbitMQ message is acknowledged, so a crash at any point causes a redelivery and not a loss.

Publish. The load generator sends a JSON body with task_id and task. The broker confirms the write before the publisher continues.
Receive. The worker decodes the message. A body that is not a JSON object, or has no task_id, goes to the DLQ at once.
Claim. claim_task inserts the task into the ledger, or returns False if the task is already processed or failed. A duplicate is acknowledged and skipped.
Execute. The task handler runs. The simulation sleeps for 5 seconds and fails for any task beginning with FAIL.
Success. The worker marks the task processed in the ledger, then acknowledges the message.
Failure, retries remaining. The worker publishes a copy to tasks.retry with an incremented retry_count header, waits for broker confirmation, records the retry in the ledger, then acknowledges the original. The retry queue's 10-second TTL dead-letters the message back to tasks.
Failure, retries exhausted. The worker publishes to tasks.dlq, marks the task failed in the ledger, then acknowledges the original.

Failure point
Behaviour
Worker crashes mid-task
Broker redelivers the unacknowledged message to another worker
Ledger database unreachable
Worker negatives the message with requeue and retries; no message is lost
Broker rejects a publish
Publisher confirms raise, the original is requeued
Malformed JSON
Sent to DLQ immediately, no retries
Task succeeds but ack is lost
Redelivery is detected by the ledger and acknowledged without running the task again
Worker's own state write fails after publishing retry
Original is requeued, which can produce one extra retry copy (see Known limitations)

Health and probes
The worker runs an HTTP server on port 8081 with four endpoints.

Endpoint
Meaning
Failing it causes
/startupz
Process has started and the health server is up
Startup probe waits before liveness begins
/livez
The consume loop has sent a heartbeat within LIVENESS_STALE_SECONDS (default 120)
Pod restart. Deliberately ignores dependency outages, so a database outage does not cause a restart loop
/readyz
Ledger schema initialized and RabbitMQ connection active
Pod removed from endpoints, no restart


Probe configuration for k8s/worker.yaml:

          ports:

            - name: health

              containerPort: 8081

          startupProbe:

            httpGet: { path: /startupz, port: health }

            periodSeconds: 5

            failureThreshold: 12

          livenessProbe:

            httpGet: { path: /livez, port: health }

            periodSeconds: 20

            failureThreshold: 3

          readinessProbe:

            httpGet: { path: /readyz, port: health }

            periodSeconds: 10

            failureThreshold: 3
Autoscaling
Setting
Value
Reasoning
Trigger
RabbitMQ QueueLength, target 5
Roughly one replica per five waiting tasks
pollingInterval
15 s
Balances reaction time against broker load
minReplicaCount / maxReplicaCount
1 / 10
Caps cost and protects the database connection pool
Scale-up
Up to 3 pods or 100 % every 30 s, whichever is larger, no stabilization window
Reacts quickly to bursts
Scale-down
Up to 2 pods every 60 s after a 60 s stabilization window
Avoids flapping during bursty load
fallback
2 replicas after 3 failed polls
Keeps a minimum capacity if KEDA cannot read the queue

Observability
Metrics
Source
Key metrics
Worker (:8000)
worker_messages_received_total, worker_messages_processed_total, worker_messages_failed_total, worker_message_processing_seconds (histogram)
RabbitMQ (:15692)
rabbitmq_queue_messages_ready, rabbitmq_queue_messages_unacked for tasks, tasks.retry, tasks.dlq
Ledger exporter (:9187)
task_ledger_counts_received, _processed, _failed, _pending
kube-state-metrics
kube_deployment_status_replicas_available, kube_deployment_spec_replicas for task-worker

Alerts
Alert
Condition
Severity
First check
RabbitMQHighQueueBacklog
Ready messages > 100 for 5 min
warning
Replica count, worker logs, processing latency
RabbitMQMessagesUnacknowledged
Unacked > 50 for 5 min
warning
Worker health and stuck consumers
TaskWorkerUnavailable
No available replicas for 2 min
critical
Pod events, probe failures, DB and broker connectivity
TaskWorkerProcessingFailures
At least 5 failed attempts in 5 min
warning
Worker logs, retry and DLQ counts
TaskWorkerReplicasAboveFive
Desired replicas > 4
warning
Whether backlog is legitimate or a stuck consumer
TaskWorkerAtMaximumReplicas
Desired replicas ≥ 10 for 5 min
critical
KEDA may be unable to keep up. Check capacity and backlog growth


Alertmanager sends warning and critical alerts by email through an AlertmanagerConfig in the messaging namespace, with resolved notifications enabled.
Suggested SLIs
SLI
Definition
Task success rate
processed / received over 1 hour
Queue latency
Time from publish to ack. Approximated by backlog divided by throughput
Availability
Fraction of minutes with at least one ready worker
Durability
Tasks in the ledger with no terminal state after the backlog clears. Target: zero

Deployment
Run from the repository root. Namespace and secrets are set up first, because nothing in the manifests creates them.
1. Cluster and operators
minikube start

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts

helm install monitoring prometheus-community/kube-prometheus-stack -n monitoring --create-namespace

# Install KEDA following https://keda.sh/docs/latest/deploy/

The ServiceMonitors and PrometheusRules use the label release: monitoring, so the Helm release name must be monitoring.
2. Namespace and secrets
Secrets are not stored in this repository. Create them from the command line as described in k8s/secret/README.md. The required secrets are:

Secret
Keys
Used by
rabbitmq-credentials
username, password, host
RabbitMQ, worker, load generator, KEDA trigger
task-ledger-db
POSTGRES_DB (set to taskledger), POSTGRES_USER, POSTGRES_PASSWORD
PostgreSQL, worker, exporter
docker-rs
Docker registry pull credentials
Worker and load generator images
gmail-smtp-credentials
password (Gmail App Password)
Alertmanager


rabbitmq-credentials must include a host key with the AMQP URL, because the KEDA TriggerAuthentication reads that key. Passwords containing @ must be URL-encoded inside the URL.
3. Data and messaging layer
kubectl apply -f k8s/namespace.yaml

kubectl apply -f k8s/rabbitmq.yaml

kubectl apply -f k8s/monitoring/rabbitmq-servicemonitor.yaml

kubectl apply -f k8s/postgres.yaml

kubectl apply -f k8s/ConfigMap/postgres-exporter.yaml

kubectl apply -f k8s/postgres-expoter.yaml

kubectl apply -f k8s/monitoring/task-ledger.yaml

Wait for rabbitmq-0 or the rabbitmq pod, and task-ledger-db-0, to report Ready.
4. Workers and autoscaling
kubectl apply -f k8s/worker.yaml

kubectl apply -f k8s/task-worker-service.yaml

kubectl apply -f k8s/monitoring/task-worker-servicemonitor.yaml

kubectl apply -f k8s/keda-autoscaling.yaml
5. Alerting
kubectl apply -f k8s/alert/alert-config.yaml

kubectl apply -f k8s/alert/alerts.yaml

Remove k8s/alert/email-test.yaml after verifying email delivery. It fires permanently by design.
Validation
Each scenario below checks one behaviour. Run them in order and record what you observe.

Scenario
How to run
Expected result
Normal scaling
kubectl apply -f k8s/load-generator.yaml and watch kubectl get hpa -n messaging -w
Replicas rise during HEAVY and fall after the backlog clears
Retry and DLQ
The load generator's FAIL- tasks
Retry logs appear, then Task ... sent to DLQ after 3 retries. Ledger shows failed
Malformed message
Publish a non-JSON body to tasks
Sent to tasks.dlq immediately
Worker crash
kubectl delete pod -l app.kubernetes.io/name=task-worker during load
No task is lost. Ledger count of processed plus failed matches published count once drained
Database outage
kubectl scale statefulset task-ledger-db -n messaging --replicas=0 for 2 minutes
/readyz fails, messages wait in the queue, no data loss after recovery
Broker restart
kubectl rollout restart deployment rabbitmq -n messaging
Workers reconnect, unacked messages are redelivered
Alert firing
Scale task-worker to 0
TaskWorkerUnavailable fires after 2 minutes and an email is sent


Verify the durable count:

kubectl exec -n messaging statefulset/task-ledger-db -- \

  psql -U "$POSTGRES_USER" -d taskledger \

  -c "SELECT status, COUNT(*) FROM task_ledger GROUP BY status;"
Operational runbook
Symptom
Likely cause
Action
RabbitMQHighQueueBacklog with replicas at maximum
Processing slower than arrival, or a stuck consumer
Check worker logs and processing histogram. Confirm unacked is not growing without bound
TaskWorkerUnavailable
Crash loop, failing probes, or DB or broker down
kubectl describe pod, then kubectl logs --previous. Check /readyz status
TaskWorkerProcessingFailures
Bad input or a failing dependency
Inspect ledger error_message for recent failed rows. Fix root cause before replaying
DLQ not empty
Permanent failures or malformed input
Inspect with rabbitmqadmin get queue=tasks.dlq requeue=false. Fix, then replay
Pods restart repeatedly
Liveness stale heartbeat
Check whether the consume loop is blocked. Review LIVENESS_STALE_SECONDS

Known limitations
These are documented deliberately. Each one is a known tradeoff or planned work.

Single RabbitMQ replica. A broker restart pauses processing. Messages are persistent, so none are lost, but throughput stops during recovery. A clustered broker with quorum queues is the planned fix.
Classic queues, not quorum queues. tasks and its companions use classic durable queues. Quorum queues would replicate messages across broker nodes.
Duplicate concurrent execution is possible. claim_task prevents re-running a completed task, but two workers can process the same task at the same time if a redelivery overlaps. The system is at-least-once with idempotent completion, not exactly-once.
One duplicate retry is possible. If recording the retry in the ledger fails after the retry copy is published, the original is requeued, so one extra retry copy can exist.
No PodDisruptionBudget. Node drains can evict several workers at once.
Simulated work. process_task is a 5-second sleep. Replace it with a real handler before using these patterns in production.
Metrics reset on restart. Worker counters are per-process. Durable totals come from the ledger exporter.
Local-only infrastructure. Minikube, local-path storage, and one node. No backup or restore for PostgreSQL.
Roadmap
Clustered RabbitMQ with quorum queues and PodDisruptionBudgets
Lease-based task claims to remove concurrent duplicate execution
Grafana dashboards committed as JSON
PostgreSQL backups with a tested restore procedure
CI pipeline: lint manifests, build and scan the image, run the validation scenarios in a kind cluster
Chaos experiments, such as broker and database failures, with measured recovery times
Repository layout
keda-rabbitmq-project/

├── README.md

├── app/

│   ├── worker.py               # Consumer, retry/DLQ logic, metrics, health endpoints

│   ├── task_ledger.py          # PostgreSQL ledger: claim, record, count

│   └── Dockerfile              # python:3.12-slim, non-root UID 10001

└── k8s/

    ├── namespace.yaml

    ├── rabbitmq.yaml           # Broker Deployment, PVC, Service

    ├── postgres.yaml           # Ledger StatefulSet and Services

    ├── postgres-expoter.yaml   # Ledger exporter Deployment and Service

    ├── ConfigMap/postgres-exporter.yaml   # Custom ledger queries

    ├── worker.yaml             # task-worker Deployment

    ├── task-worker-service.yaml

    ├── keda-autoscaling.yaml   # TriggerAuthentication and ScaledObject

    ├── load-generator.yaml     # Test traffic and forced failures

    ├── secret/README.md        # How to create secrets outside Git

    ├── alert/

    │   ├── alerts.yaml         # PrometheusRule for messaging alerts

    │   ├── alert-config.yaml   # Alertmanager email routing

    │   └── email-test.yaml     # Temporary alert to verify email delivery

    └── monitoring/             # ServiceMonitors for Prometheus
Technology stack
Kubernetes, Minikube, KEDA, Horizontal Pod Autoscaler, RabbitMQ 4.1, PostgreSQL 16, Python 3.12 with pika and psycopg2, prometheus-client, postgres-exporter, Prometheus Operator and Alertmanager (kube-prometheus-stack), Grafana, Docker.
References
KEDA RabbitMQ queue scaler
RabbitMQ dead-letter exchanges
RabbitMQ publisher confirms
Kubernetes probes



Author: Sadha19
