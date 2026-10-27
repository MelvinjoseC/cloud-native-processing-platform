import threading
import os
import sys
import time
import json
import logging
import signal
import redis
import pika
from prometheus_client import Counter, Histogram, Gauge, start_http_server

METRICS_PORT = int(os.getenv("METRICS_PORT", "9102"))

# Prometheus metrics
WORKER_JOBS_PROCESSED_TOTAL = Counter(
    "worker_jobs_processed_total",
    "Total jobs processed by worker",
    ["status", "task_type"],
)
WORKER_JOB_DURATION_SECONDS = Histogram(
    "worker_job_duration_seconds",
    "Duration of worker task execution in seconds",
    ["task_type"],
)
WORKER_ACTIVE_JOBS = Gauge(
    "worker_active_jobs", "Current active jobs being processed by worker"
)


# Logging setup with thread-local correlation context
log_context = threading.local()


class ContextLogFilter(logging.Filter):
    def filter(self, record):
        record.correlation_id = getattr(log_context, "correlation_id", "none")
        record.request_id = getattr(log_context, "request_id", "none")
        return True


class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        if hasattr(record, "correlation_id") and record.correlation_id != "none":
            log_record["correlation_id"] = record.correlation_id
        if hasattr(record, "request_id") and record.request_id != "none":
            log_record["request_id"] = record.request_id
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record)


handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JSONFormatter())
handler.addFilter(ContextLogFilter())
logger = logging.getLogger("worker")
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# Env configurations
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "tasks")
RABBITMQ_DLQ = os.getenv("RABBITMQ_DLQ", "tasks_dlq")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

# S3 configurations for payload archival
S3_BUCKET = os.getenv("S3_BUCKET", None)
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

s3_client = None
if S3_BUCKET:
    try:
        import boto3

        s3_client = boto3.client("s3", region_name=AWS_REGION)
        logger.info(
            f"S3 client initialized for payload archival to bucket '{S3_BUCKET}'."
        )
    except Exception as s3_init_err:
        logger.warning(
            f"Could not initialize S3 client: {s3_init_err}. S3 archival disabled."
        )


def archive_payload_to_s3(job_id: str, task: dict, request_id: str = None):
    if not s3_client or not S3_BUCKET:
        return
    try:
        s3_key = f"results/{job_id}.json"
        put_kwargs = {
            "Bucket": S3_BUCKET,
            "Key": s3_key,
            "Body": json.dumps(task),
            "ContentType": "application/json",
        }
        if request_id:
            put_kwargs["Metadata"] = {"job_id": job_id, "request_id": str(request_id)}
        s3_client.put_object(**put_kwargs)
        logger.info(
            f"[{job_id}] Successfully archived payload to s3://{S3_BUCKET}/{s3_key}"
        )
    except Exception as upload_err:
        logger.error(f"[{job_id}] Failed to archive payload to S3: {upload_err}")


# Graceful shutdown state
shutdown_requested = False
current_channel = None
current_connection = None
processing_active = False


def sigterm_handler(signum, frame):
    global shutdown_requested, current_connection, current_channel
    logger.info("SIGTERM received. Starting graceful shutdown...")
    shutdown_requested = True

    # If we are not actively processing a task, we can close the connection and exit
    if not processing_active:
        logger.info("No active task. Exiting immediately.")
        cleanup_and_exit()


def cleanup_and_exit():
    global current_connection, current_channel
    try:
        if current_channel and current_channel.is_open:
            current_channel.close()
        if current_connection and current_connection.is_open:
            current_connection.close()
    except Exception as e:
        logger.error(f"Error closing connections: {e}")
    logger.info("Graceful shutdown complete. Exiting.")
    sys.exit(0)


# Register signals
signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigterm_handler)

# Initialize Redis client
redis_client = None
try:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        socket_timeout=3.0,
        decode_responses=True,
    )
    redis_client.ping()
    logger.info("Connected to Redis successfully.")
except Exception as e:
    logger.warning(
        f"Redis connection failed: {e}. Running without Redis status reporting."
    )


class ShutdownAbortedException(Exception):
    """Exception raised when worker shutdown is requested during task execution."""

    pass


def process_message(ch, method, properties, body):
    global processing_active, shutdown_requested
    processing_active = True
    WORKER_ACTIVE_JOBS.inc()

    # We will need the task dict inside the except block, so define it as empty first
    task = {}
    job_id = "unknown"
    task_type = "unknown"
    headers = (
        properties.headers
        if (
            properties
            and hasattr(properties, "headers")
            and isinstance(properties.headers, dict)
        )
        else {}
    )
    correlation_id = getattr(properties, "correlation_id", None) or "unknown"
    request_id = headers.get("x-request-id") or "unknown"

    try:
        task = json.loads(body.decode("utf-8"))
        job_id = task.get(
            "job_id", correlation_id if correlation_id != "unknown" else "unknown"
        )
        task_type = task.get("task_type", "unknown")
        duration = task.get("duration_seconds", 5)
        if request_id == "unknown":
            request_id = task.get("request_id", "unknown")

        log_context.correlation_id = job_id
        log_context.request_id = request_id

        logger.info(
            f"[{job_id}] Received task '{task_type}' (Simulated work: {duration}s)"
        )

        # Check if job was cancelled by client
        if redis_client and job_id != "unknown":
            try:
                cached_data = redis_client.get(f"job:{job_id}")
                if cached_data:
                    cached_obj = json.loads(cached_data)
                    if cached_obj.get("status") == "CANCELLED":
                        logger.info(
                            f"[{job_id}] Task was cancelled by client. Skipping execution."
                        )
                        WORKER_JOBS_PROCESSED_TOTAL.labels(
                            status="cancelled", task_type=task_type
                        ).inc()
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        return
            except Exception as check_err:
                logger.warning(
                    f"[{job_id}] Could not check cancellation status: {check_err}"
                )

        # 1. Update status to PROCESSING in Redis
        if redis_client:
            try:
                task["status"] = "PROCESSING"
                task["started_at"] = time.time()
                redis_client.set(f"job:{job_id}", json.dumps(task), ex=3600)
            except Exception as e:
                logger.error(f"Failed to update redis: {e}")

        # 2. Simulate CPU-heavy or network-bound processing
        for elapsed in range(duration):
            if shutdown_requested:
                raise ShutdownAbortedException(
                    "Graceful shutdown initiated during task execution."
                )
            time.sleep(1)

        logger.info(f"[{job_id}] Finished task successfully.")

        # 3. Update status to COMPLETED in Redis
        if redis_client:
            try:
                task["status"] = "COMPLETED"
                task["completed_at"] = time.time()
                redis_client.set(f"job:{job_id}", json.dumps(task), ex=3600)
            except Exception as e:
                logger.error(f"Failed to update redis: {e}")

        # 4. Archive payload to S3 if configured
        archive_payload_to_s3(job_id, task, request_id=request_id)

        # 5. Record completion metrics
        WORKER_JOBS_PROCESSED_TOTAL.labels(
            status="completed", task_type=task_type
        ).inc()
        WORKER_JOB_DURATION_SECONDS.labels(task_type=task_type).observe(duration)

        # 6. Acknowledge message processing in RabbitMQ
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except ShutdownAbortedException as e:
        logger.warning(f"[{job_id}] {e} Requeuing task.")
        # Reset status in Redis to PENDING so it can be retried cleanly
        if redis_client and job_id != "unknown":
            try:
                task["status"] = "PENDING"
                task.pop("started_at", None)
                redis_client.set(f"job:{job_id}", json.dumps(task), ex=3600)
            except Exception as re_err:
                logger.error(f"Failed to reset redis status: {re_err}")
        try:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        except Exception as nack_err:
            logger.error(f"Nack failed: {nack_err}")

    except Exception as e:
        logger.error(f"[{job_id}] Error processing message: {e}")

        headers = (
            properties.headers
            if (
                properties
                and hasattr(properties, "headers")
                and isinstance(properties.headers, dict)
            )
            else {}
        )
        retry_count = headers.get("x-retry-count", 0) + 1

        if retry_count > MAX_RETRIES:
            logger.error(
                f"[{job_id}] Exceeded maximum retries ({MAX_RETRIES}). Routing poison message to DLQ '{RABBITMQ_DLQ}'."
            )
            WORKER_JOBS_PROCESSED_TOTAL.labels(
                status="failed", task_type=task_type
            ).inc()
            if redis_client and job_id != "unknown":
                try:
                    task["status"] = "FAILED"
                    task["error"] = str(e)
                    task["failed_at"] = time.time()
                    task["retry_count"] = retry_count
                    redis_client.set(f"job:{job_id}", json.dumps(task), ex=3600)
                except Exception as re_err:
                    logger.error(f"Failed to update redis status to FAILED: {re_err}")

            try:
                dlq_headers = dict(headers)
                dlq_headers["x-death-reason"] = str(e)
                dlq_headers["x-retry-count"] = retry_count
                ch.basic_publish(
                    exchange="",
                    routing_key=RABBITMQ_DLQ,
                    body=body,
                    properties=pika.BasicProperties(
                        delivery_mode=2, headers=dlq_headers
                    ),
                )
                logger.info(
                    f"[{job_id}] Successfully forwarded poison message to DLQ '{RABBITMQ_DLQ}'."
                )
            except Exception as dlq_err:
                logger.error(f"Failed to forward message to DLQ: {dlq_err}")

            try:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as ack_err:
                logger.error(f"Ack failed after DLQ routing: {ack_err}")
        else:
            logger.warning(
                f"[{job_id}] Task processing failed (attempt {retry_count}/{MAX_RETRIES}): {e}"
            )
            try:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            except Exception as nack_err:
                logger.error(f"Nack failed: {nack_err}")

    finally:
        log_context.correlation_id = "none"
        log_context.request_id = "none"
        processing_active = False
        WORKER_ACTIVE_JOBS.dec()
        if shutdown_requested:
            logger.info(
                "Finished processing current message. Exiting due to shutdown request."
            )
            cleanup_and_exit()


def start_worker():
    global current_connection, current_channel
    import random

    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300,
    )

    base_delay = 2.0
    max_delay = 60.0
    retry_delay = base_delay

    # Start Prometheus metrics server
    try:
        start_http_server(METRICS_PORT)
        logger.info(f"Prometheus metrics server started on port {METRICS_PORT}.")
    except Exception as metrics_err:
        logger.warning(
            f"Could not start metrics server on port {METRICS_PORT}: {metrics_err}"
        )

    while not shutdown_requested:
        try:
            logger.info("Connecting to RabbitMQ...")
            current_connection = pika.BlockingConnection(parameters)
            current_channel = current_connection.channel()

            # Ensure queues exist
            current_channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
            current_channel.queue_declare(queue=RABBITMQ_DLQ, durable=True)

            # Prefetch count = 1 to distribute load evenly among workers
            current_channel.basic_qos(prefetch_count=1)

            current_channel.basic_consume(
                queue=RABBITMQ_QUEUE, on_message_callback=process_message
            )

            logger.info("Worker started. Waiting for tasks...")
            # Connection succeeded, reset retry_delay
            retry_delay = base_delay
            current_channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            if shutdown_requested:
                break
            jitter = random.uniform(0.0, 1.0)
            sleep_time = min(max_delay, retry_delay) + jitter
            logger.error(
                f"Connection lost. Reconnecting in {sleep_time:.2f} seconds... Detail: {e}"
            )
            time.sleep(sleep_time)
            retry_delay *= 2
        except Exception as e:
            if shutdown_requested:
                break
            jitter = random.uniform(0.0, 1.0)
            sleep_time = min(max_delay, retry_delay) + jitter
            logger.error(
                f"Unexpected error: {e}. Reconnecting in {sleep_time:.2f} seconds..."
            )
            time.sleep(sleep_time)
            retry_delay *= 2


if __name__ == "__main__":
    start_worker()
