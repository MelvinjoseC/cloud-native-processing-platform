import os
import threading
import uuid
import json
import logging
import time
from typing import Optional, List
from fastapi import FastAPI, HTTPException, status, Response, Query, Request
from pydantic import BaseModel, Field
import redis
import pika
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

import sys


# Logging setup
class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record)


handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JSONFormatter())
logger = logging.getLogger("producer")
logger.setLevel(logging.INFO)
logger.addHandler(handler)

app = FastAPI(title="Cloud-Native Job Producer", version="1.0.0")

# Env configurations
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "tasks")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

# Prometheus metrics
HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total", "Total HTTP Requests", ["method", "endpoint", "status"]
)
REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds", "HTTP Request Latency", ["method", "endpoint"]
)
JOBS_SUBMITTED = Counter("jobs_submitted_total", "Total jobs submitted to RabbitMQ")

REDIS_MAX_CONNECTIONS = int(os.getenv("REDIS_MAX_CONNECTIONS", "20"))
REDIS_SOCKET_TIMEOUT = float(os.getenv("REDIS_SOCKET_TIMEOUT", "3.0"))
REDIS_RETRY_ON_TIMEOUT = os.getenv("REDIS_RETRY_ON_TIMEOUT", "true").lower() == "true"

# Initialize Redis connection pool (degrades gracefully if down)
redis_pool = None
redis_client = None


def init_redis_client():
    global redis_pool
    try:
        redis_pool = redis.ConnectionPool(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            max_connections=REDIS_MAX_CONNECTIONS,
            socket_timeout=REDIS_SOCKET_TIMEOUT,
            retry_on_timeout=REDIS_RETRY_ON_TIMEOUT,
            health_check_interval=30,
            decode_responses=True,
        )
        client = redis.Redis(connection_pool=redis_pool)
        client.ping()
        logger.info(
            f"Connected to Redis connection pool (max={REDIS_MAX_CONNECTIONS}) successfully."
        )
        return client
    except Exception as e:
        logger.warning(
            f"Redis connection pool init failed: {e}. Degrading to database-less mode."
        )
        return None


redis_client = init_redis_client()


# Thread-safe RabbitMQ connection and channel pooling
class RabbitMQChannelPool:
    def __init__(
        self,
        host=RABBITMQ_HOST,
        user=RABBITMQ_USER,
        password=RABBITMQ_PASS,
        queue=RABBITMQ_QUEUE,
    ):
        self.host = host
        self.user = user
        self.password = password
        self.queue = queue
        self._lock = threading.Lock()
        self._connection = None
        self._channel = None

    def _connect(self):
        credentials = pika.PlainCredentials(self.user, self.password)
        parameters = pika.ConnectionParameters(
            host=self.host,
            credentials=credentials,
            socket_timeout=3.0,
            heartbeat=600,
            blocked_connection_timeout=300,
        )
        self._connection = pika.BlockingConnection(parameters)
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=self.queue, durable=True)

    def get_channel(self):
        with self._lock:
            try:
                if (
                    self._connection is None
                    or self._connection.is_closed
                    or self._channel is None
                    or self._channel.is_closed
                ):
                    self._connect()
                return self._connection, self._channel
            except Exception as e:
                logger.warning(
                    f"Failed to acquire pooled RabbitMQ channel: {e}, falling back to single-use connection."
                )
                credentials = pika.PlainCredentials(self.user, self.password)
                conn = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=self.host, credentials=credentials, socket_timeout=3.0
                    )
                )
                ch = conn.channel()
                ch.queue_declare(queue=self.queue, durable=True)
                return conn, ch


_rabbitmq_pool = RabbitMQChannelPool()


def get_rabbitmq_channel():
    return _rabbitmq_pool.get_channel()


class JobRequest(BaseModel):
    task_type: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Task identifier or workload type",
    )
    duration_seconds: int = Field(
        default=5,
        ge=1,
        le=3600,
        description="Execution duration in seconds (1 to 3600)",
    )
    payload: dict = Field(default_factory=dict, description="Task payload parameters")


class BatchJobRequest(BaseModel):
    jobs: List[JobRequest] = Field(
        ...,
        min_length=1,
        max_length=100,
        description="List of job requests to submit in batch (1-100)",
    )


@app.middleware("http")
async def add_metrics_and_tracing_middleware(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    request.state.request_id = request_id

    start_time = time.time()
    response = await call_next(request)
    duration = time.time() - start_time

    response.headers["X-Request-ID"] = request_id
    endpoint = request.url.path
    # Exclude metrics and health check from latency graphs to avoid noise
    if endpoint not in ["/metrics", "/healthz", "/readyz"]:
        HTTP_REQUESTS_TOTAL.labels(
            method=request.method, endpoint=endpoint, status=response.status_code
        ).inc()
        REQUEST_LATENCY.labels(method=request.method, endpoint=endpoint).observe(
            duration
        )

    return response


@app.post("/api/v1/jobs", status_code=status.HTTP_202_ACCEPTED)
def submit_job(job: JobRequest, request: Request = None):
    request_id = getattr(getattr(request, "state", None), "request_id", None) or (
        request.headers.get("X-Request-ID")
        if request and hasattr(request, "headers")
        else str(uuid.uuid4())
    )
    job_id = str(uuid.uuid4())
    job_data = {
        "job_id": job_id,
        "task_type": job.task_type,
        "duration_seconds": job.duration_seconds,
        "payload": job.payload,
        "status": "PENDING",
        "created_at": time.time(),
        "request_id": request_id,
    }

    # 1. Save state in Redis (graceful degradation)
    if redis_client:
        try:
            redis_client.set(
                f"job:{job_id}", json.dumps(job_data), ex=3600
            )  # TTL of 1 hour
        except Exception as e:
            logger.error(f"Failed to cache job status in Redis: {e}")

    # 2. Publish to RabbitMQ
    try:
        connection, channel = get_rabbitmq_channel()
        channel.basic_publish(
            exchange="",
            routing_key=RABBITMQ_QUEUE,
            body=json.dumps(job_data),
            properties=pika.BasicProperties(
                delivery_mode=2,
                correlation_id=job_id,
                headers={"x-request-id": request_id},
            ),
        )
        if connection != getattr(_rabbitmq_pool, "_connection", None):
            connection.close()
        logger.info(f"Successfully published job {job_id} to queue.")
        JOBS_SUBMITTED.inc()
    except Exception as e:
        logger.critical(f"Failed to publish job to RabbitMQ: {e}")
        # Mark as FAILED in redis if we could
        if redis_client:
            job_data["status"] = "FAILED"
            redis_client.set(f"job:{job_id}", json.dumps(job_data), ex=3600)
        raise HTTPException(
            status_code=503, detail="Task queue unavailable. Please try again later."
        )

    return {"job_id": job_id, "status": "PENDING"}


@app.post("/api/v1/jobs/batch", status_code=status.HTTP_202_ACCEPTED)
def submit_jobs_batch(batch: BatchJobRequest, request: Request = None):
    request_id = getattr(getattr(request, "state", None), "request_id", None) or (
        request.headers.get("X-Request-ID")
        if request and hasattr(request, "headers")
        else str(uuid.uuid4())
    )

    prepared_jobs = []
    job_responses = []

    for item in batch.jobs:
        job_id = str(uuid.uuid4())
        job_data = {
            "job_id": job_id,
            "task_type": item.task_type,
            "duration_seconds": item.duration_seconds,
            "payload": item.payload,
            "status": "PENDING",
            "created_at": time.time(),
            "request_id": request_id,
        }
        prepared_jobs.append(job_data)
        job_responses.append({"job_id": job_id, "status": "PENDING"})

    # 1. Pipeline caching in Redis
    if redis_client:
        try:
            pipe = redis_client.pipeline()
            for job_data in prepared_jobs:
                pipe.set(f"job:{job_data['job_id']}", json.dumps(job_data), ex=3600)
            pipe.execute()
        except Exception as e:
            logger.error(f"Failed to cache batch jobs in Redis pipeline: {e}")

    # 2. Publish batch to RabbitMQ
    try:
        connection, channel = get_rabbitmq_channel()
        for job_data in prepared_jobs:
            channel.basic_publish(
                exchange="",
                routing_key=RABBITMQ_QUEUE,
                body=json.dumps(job_data),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    correlation_id=job_data["job_id"],
                    headers={"x-request-id": request_id},
                ),
            )
        if connection != getattr(_rabbitmq_pool, "_connection", None):
            connection.close()
        logger.info(
            f"Successfully published batch of {len(prepared_jobs)} jobs to queue."
        )
        JOBS_SUBMITTED.inc(len(prepared_jobs))
    except Exception as e:
        logger.critical(f"Failed to publish batch to RabbitMQ: {e}")
        if redis_client:
            try:
                pipe = redis_client.pipeline()
                for job_data in prepared_jobs:
                    job_data["status"] = "FAILED"
                    pipe.set(f"job:{job_data['job_id']}", json.dumps(job_data), ex=3600)
                pipe.execute()
            except Exception as pipe_err:
                logger.error(
                    f"Failed to update redis batch status to FAILED: {pipe_err}"
                )
        raise HTTPException(
            status_code=503,
            detail="Task queue unavailable. Failed to submit batch.",
        )

    return {"submitted": len(job_responses), "jobs": job_responses}


@app.get("/api/v1/jobs")
def list_jobs(
    status_filter: Optional[str] = Query(
        None,
        alias="status",
        description="Filter by job status (PENDING, PROCESSING, COMPLETED, FAILED, CANCELLED)",
    ),
    limit: int = Query(50, ge=1, le=100, description="Max jobs to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
):
    if not redis_client:
        raise HTTPException(
            status_code=501, detail="Redis caching unavailable. Cannot list jobs."
        )

    try:
        keys = redis_client.keys("job:*")
    except Exception as e:
        logger.error(f"Error retrieving job keys from Redis: {e}")
        raise HTTPException(status_code=500, detail="Error fetching jobs list")

    jobs = []
    for key in keys:
        try:
            val = redis_client.get(key)
            if val:
                job_obj = json.loads(val)
                if (
                    status_filter is None
                    or job_obj.get("status", "").upper() == status_filter.upper()
                ):
                    jobs.append(job_obj)
        except Exception:
            continue

    # Sort newest first based on created_at timestamp
    jobs.sort(key=lambda j: j.get("created_at", 0), reverse=True)

    total = len(jobs)
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "jobs": jobs[offset : offset + limit],
    }


@app.get("/api/v1/jobs/{job_id}")
def get_job_status(job_id: str):
    if not redis_client:
        raise HTTPException(
            status_code=501,
            detail="Redis caching unavailable. Cannot retrieve job status.",
        )

    try:
        data = redis_client.get(f"job:{job_id}")
    except Exception as e:
        logger.error(f"Redis retrieval error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching job status")

    if not data:
        raise HTTPException(status_code=404, detail="Job not found")
    return json.loads(data)


@app.delete("/api/v1/jobs/{job_id}")
def cancel_job(job_id: str):
    if not redis_client:
        raise HTTPException(
            status_code=501, detail="Redis caching unavailable. Cannot cancel job."
        )

    try:
        data = redis_client.get(f"job:{job_id}")
    except Exception as e:
        logger.error(f"Redis retrieval error during cancellation: {e}")
        raise HTTPException(
            status_code=500, detail="Error retrieving job for cancellation"
        )

    if not data:
        raise HTTPException(status_code=404, detail="Job not found")

    job_data = json.loads(data)
    current_status = job_data.get("status")

    if current_status in ["PENDING", "PROCESSING"]:
        job_data["status"] = "CANCELLED"
        job_data["cancelled_at"] = time.time()
        redis_client.set(f"job:{job_id}", json.dumps(job_data), ex=3600)
        logger.info(f"Job {job_id} marked as CANCELLED (was {current_status}).")
        return {
            "job_id": job_id,
            "status": "CANCELLED",
            "message": f"Job was {current_status} and is now cancelled",
        }
    else:
        redis_client.delete(f"job:{job_id}")
        logger.info(f"Job {job_id} ({current_status}) removed from cache.")
        return {
            "job_id": job_id,
            "status": "DELETED",
            "message": f"Job in '{current_status}' state removed from cache",
        }


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/healthz")
def healthz():
    return {"status": "healthy"}


@app.get("/readyz")
def readyz():
    global redis_client
    # Verify connectivity to RabbitMQ
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=RABBITMQ_HOST, credentials=credentials, socket_timeout=1.0
            )
        )
        if connection != getattr(_rabbitmq_pool, "_connection", None):
            connection.close()
    except Exception as e:
        logger.error(f"Readyz check failed (RabbitMQ offline): {e}")
        raise HTTPException(status_code=503, detail="RabbitMQ not reachable")

    # Verify or initialize connectivity to Redis
    if redis_client is None:
        redis_client = init_redis_client()
        if redis_client is None:
            raise HTTPException(status_code=503, detail="Redis cache not reachable")
    else:
        try:
            redis_client.ping()
        except Exception as e:
            logger.error(f"Readyz check failed (Redis connection lost): {e}")
            raise HTTPException(status_code=503, detail="Redis cache not reachable")

    return {"status": "ready"}
