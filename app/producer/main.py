import os
import uuid
import json
import logging
import time
from typing import Optional, List
from fastapi import FastAPI, HTTPException, status, Response, Query
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
            "logger": record.name
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
HTTP_REQUESTS_TOTAL = Counter("http_requests_total", "Total HTTP Requests", ["method", "endpoint", "status"])
REQUEST_LATENCY = Histogram("http_request_duration_seconds", "HTTP Request Latency", ["method", "endpoint"])
JOBS_SUBMITTED = Counter("jobs_submitted_total", "Total jobs submitted to RabbitMQ")

# Initialize Redis client (degrades gracefully if down)
redis_client = None
try:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        socket_timeout=3.0,
        decode_responses=True
    )
    redis_client.ping()
    logger.info("Connected to Redis successfully.")
except Exception as e:
    logger.warning(f"Redis connection failed: {e}. Degrading to database-less mode.")


# Pika connection helper
def get_rabbitmq_channel():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials, socket_timeout=3.0))
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
    return connection, channel

class JobRequest(BaseModel):
    task_type: str = Field(..., min_length=1, max_length=100, description="Task identifier or workload type")
    duration_seconds: int = Field(default=5, ge=1, le=3600, description="Execution duration in seconds (1 to 3600)")
    payload: dict = Field(default_factory=dict, description="Task payload parameters")

@app.middleware("http")
async def add_metrics_middleware(request, call_next):
    start_time = time.time()
    response = await call_next(request)
    duration = time.time() - start_time
    
    endpoint = request.url.path
    # Exclude metrics and health check from latency graphs to avoid noise
    if endpoint not in ["/metrics", "/healthz", "/readyz"]:
        HTTP_REQUESTS_TOTAL.labels(method=request.method, endpoint=endpoint, status=response.status_code).inc()
        REQUEST_LATENCY.labels(method=request.method, endpoint=endpoint).observe(duration)
        
    return response

@app.post("/api/v1/jobs", status_code=status.HTTP_202_ACCEPTED)
def submit_job(job: JobRequest):
    job_id = str(uuid.uuid4())
    job_data = {
        "job_id": job_id,
        "task_type": job.task_type,
        "duration_seconds": job.duration_seconds,
        "payload": job.payload,
        "status": "PENDING",
        "created_at": time.time()
    }
    
    # 1. Save state in Redis (graceful degradation)
    if redis_client:
        try:
            redis_client.set(f"job:{job_id}", json.dumps(job_data), ex=3600)  # TTL of 1 hour
        except Exception as e:
            logger.error(f"Failed to cache job status in Redis: {e}")

    # 2. Publish to RabbitMQ
    try:
        connection, channel = get_rabbitmq_channel()
        channel.basic_publish(
            exchange="",
            routing_key=RABBITMQ_QUEUE,
            body=json.dumps(job_data),
            properties=pika.BasicProperties(delivery_mode=2)  # Make message persistent
        )
        connection.close()
        logger.info(f"Successfully published job {job_id} to queue.")
        JOBS_SUBMITTED.inc()
    except Exception as e:
        logger.critical(f"Failed to publish job to RabbitMQ: {e}")
        # Mark as FAILED in redis if we could
        if redis_client:
            job_data["status"] = "FAILED"
            redis_client.set(f"job:{job_id}", json.dumps(job_data), ex=3600)
        raise HTTPException(status_code=503, detail="Task queue unavailable. Please try again later.")
        
    return {"job_id": job_id, "status": "PENDING"}

@app.get("/api/v1/jobs")
def list_jobs(
    status_filter: Optional[str] = Query(None, alias="status", description="Filter by job status (PENDING, PROCESSING, COMPLETED, FAILED, CANCELLED)"),
    limit: int = Query(50, ge=1, le=100, description="Max jobs to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination")
):
    if not redis_client:
        raise HTTPException(status_code=501, detail="Redis caching unavailable. Cannot list jobs.")
    
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
                if status_filter is None or job_obj.get("status", "").upper() == status_filter.upper():
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
        "jobs": jobs[offset:offset + limit]
    }

@app.get("/api/v1/jobs/{job_id}")
def get_job_status(job_id: str):
    if not redis_client:
        raise HTTPException(status_code=501, detail="Redis caching unavailable. Cannot retrieve job status.")
    
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
        raise HTTPException(status_code=501, detail="Redis caching unavailable. Cannot cancel job.")
    
    try:
        data = redis_client.get(f"job:{job_id}")
    except Exception as e:
        logger.error(f"Redis retrieval error during cancellation: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving job for cancellation")
        
    if not data:
        raise HTTPException(status_code=404, detail="Job not found")
        
    job_data = json.loads(data)
    current_status = job_data.get("status")
    
    if current_status in ["PENDING", "PROCESSING"]:
        job_data["status"] = "CANCELLED"
        job_data["cancelled_at"] = time.time()
        redis_client.set(f"job:{job_id}", json.dumps(job_data), ex=3600)
        logger.info(f"Job {job_id} marked as CANCELLED (was {current_status}).")
        return {"job_id": job_id, "status": "CANCELLED", "message": f"Job was {current_status} and is now cancelled"}
    else:
        redis_client.delete(f"job:{job_id}")
        logger.info(f"Job {job_id} ({current_status}) removed from cache.")
        return {"job_id": job_id, "status": "DELETED", "message": f"Job in '{current_status}' state removed from cache"}

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
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials, socket_timeout=1.0))
        connection.close()
    except Exception as e:
        logger.error(f"Readyz check failed (RabbitMQ offline): {e}")
        raise HTTPException(status_code=503, detail="RabbitMQ not reachable")
    
    # Verify or initialize connectivity to Redis
    if redis_client is None:
        try:
            redis_client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                password=REDIS_PASSWORD,
                socket_timeout=1.0,
                decode_responses=True
            )
            redis_client.ping()
            logger.info("Re-initialized and connected to Redis successfully during readyz check.")
        except Exception as e:
            logger.warning(f"Readyz check: Redis is still unreachable: {e}")
            raise HTTPException(status_code=503, detail="Redis cache not reachable")
    else:
        try:
            redis_client.ping()
        except Exception as e:
            logger.error(f"Readyz check failed (Redis connection lost): {e}")
            raise HTTPException(status_code=503, detail="Redis cache not reachable")
        
    return {"status": "ready"}

