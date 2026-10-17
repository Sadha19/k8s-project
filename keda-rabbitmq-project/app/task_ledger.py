
import os
from contextlib import contextmanager

import psycopg2


@contextmanager
def db_connection():
    conn = psycopg2.connect(
        host=os.getenv(
            "DB_HOST",
            "task-ledger-db.messaging.svc.cluster.local",
        ),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        connect_timeout=5,
    )

    try:
        with conn:
            yield conn
    finally:
        conn.close()


def initialize_schema():
    """Create the ledger and migrate existing installations."""
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS task_ledger (
                    task_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL DEFAULT 'received'
                        CHECK (
                            status IN (
                                'received',
                                'processed',
                                'failed'
                            )
                        ),
                    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    completed_at TIMESTAMPTZ,
                    error_message TEXT,
                    retry_count INTEGER NOT NULL DEFAULT 0
                        CHECK (retry_count >= 0),
                    last_attempt_at TIMESTAMPTZ,
                    attempt_count INTEGER NOT NULL DEFAULT 0
                        CHECK (attempt_count >= 0)
                )
            """)

            # Migrate an existing table without deleting its data.
            cur.execute("""
                ALTER TABLE task_ledger
                ADD COLUMN IF NOT EXISTS retry_count
                    INTEGER NOT NULL DEFAULT 0
            """)

            cur.execute("""
                ALTER TABLE task_ledger
                ADD COLUMN IF NOT EXISTS last_attempt_at
                    TIMESTAMPTZ
            """)

            cur.execute("""
                ALTER TABLE task_ledger
                ADD COLUMN IF NOT EXISTS attempt_count
                    INTEGER NOT NULL DEFAULT 0
            """)


def record_received(task_id):
    """Record a task once without resetting its existing state."""
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO task_ledger (task_id, status)
                VALUES (%s, 'received')
                ON CONFLICT (task_id) DO NOTHING
            """, (task_id,))


def is_processed(task_id):
    """Check whether a task has a durable successful outcome."""
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT status = 'processed'
                FROM task_ledger
                WHERE task_id = %s
            """, (task_id,))
            row = cur.fetchone()
            return bool(row and row[0])


def claim_task(task_id):
    """
    Atomically claim a task for processing.

    Returns:
        True  - this worker acquired the claim.
        False - task is already processed or claimed by another worker.

    The row lock is released when the transaction commits, so this
    simple claim is not a distributed processing lock by itself.
    """
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO task_ledger (
                    task_id,
                    status,
                    attempt_count,
                    last_attempt_at
                )
                VALUES (%s, 'received', 1, NOW())
                ON CONFLICT (task_id) DO NOTHING
                RETURNING task_id
            """, (task_id,))

            inserted = cur.fetchone()
            if inserted:
                return True

            cur.execute("""
                SELECT status
                FROM task_ledger
                WHERE task_id = %s
                FOR UPDATE
            """, (task_id,))
            row = cur.fetchone()

            if row is None:
                raise RuntimeError(
                    f"Task ledger row disappeared: {task_id}"
                )

            if row[0] in ("processed", "failed"):
                return False

            cur.execute("""
                UPDATE task_ledger
                SET attempt_count = attempt_count + 1,
                    last_attempt_at = NOW()
                WHERE task_id = %s
            """, (task_id,))

            return True


def record_retry(task_id, retry_count, error_message):
    """
    Record a retryable failure without marking the task terminally failed.
    """
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE task_ledger
                SET status = 'received',
                    retry_count = GREATEST(retry_count, %s),
                    error_message = %s,
                    completed_at = NULL
                WHERE task_id = %s
                  AND status <> 'processed'
            """, (
                retry_count,
                str(error_message)[:2000],
                task_id,
            ))

            if cur.rowcount != 1:
                raise RuntimeError(
                    f"Cannot record retry for task {task_id}"
                )


def record_processed(task_id):
    """Persist success without overwriting an existing success time."""
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE task_ledger
                SET status = 'processed',
                    completed_at = COALESCE(completed_at, NOW()),
                    error_message = NULL
                WHERE task_id = %s
                  AND status <> 'failed'
            """, (task_id,))

            if cur.rowcount != 1:
                raise RuntimeError(
                    f"Cannot mark task processed: {task_id}"
                )


def record_failed(task_id, error_message):
    """Record a terminal failure; never overwrite successful tasks."""
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE task_ledger
                SET status = 'failed',
                    completed_at = NOW(),
                    error_message = %s
                WHERE task_id = %s
                  AND status <> 'processed'
            """, (str(error_message)[:2000], task_id))

            if cur.rowcount != 1:
                raise RuntimeError(
                    f"Cannot mark task failed: {task_id}"
                )


def get_counts():
    """Return durable task totals from PostgreSQL."""
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*),
                    COUNT(*) FILTER (
                        WHERE status = 'processed'
                    ),
                    COUNT(*) FILTER (
                        WHERE status = 'failed'
                    ),
                    COUNT(*) FILTER (
                        WHERE status = 'received'
                    )
                FROM task_ledger
            """)
            received, processed, failed, pending = cur.fetchone()

    return {
        "received": received,
        "processed": processed,
        "failed": failed,
        "pending": pending,
    }
