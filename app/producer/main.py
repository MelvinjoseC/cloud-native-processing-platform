import os
import uuid
import json
import logging
import time
from fastapi import FastAPI, HTTPException, status, Response
from pydantic import BaseModel
import redis
import pika
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("producer")

app = FastAPI(title="Cloud-Native Job Producer", version="1.0.0")

# Env configurations
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "tasks")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

# Prometheus metrics
HTTP_REQUESTS_TOTAL = Counter("http_requests_total", "Total HTTP Requests", ["method", "endpoint", "status"])
REQUEST_LATENCY = Histogram("http_request_duration_seconds", "HTTP Request Latency", ["method", "endpoint"])
JOBS_SUBMITTED = Counter("jobs_submitted_total", "Total jobs submitted to RabbitMQ")

# Initialize Redis client (degrades gracefully if down)
redis_client = None
try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, socket_timeout=3.0, decode_responses=True)
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
    task_type: str
    duration_seconds: int = 5
    payload: dict = {}

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

@app.post("/jobs", status_code=status.HTTP_202_ACCEPTED)
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

@app.get("/jobs/{job_id}")
def get_job_status(job_id: str):
    if not redis_client:
        raise HTTPException(status_code=501, detail="Redis caching unavailable. Cannot retrieve job status.")
    
    try:
        data = redis_client.get(f"job:{job_id}")
        if not data:
            raise HTTPException(status_code=404, detail="Job not found")
        return json.loads(data)
    except Exception as e:
        logger.error(f"Redis retrieval error: {e}")
        raise HTTPException(status_code=500, detail="Error fetching job status")

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/healthz")
def healthz():
    return {"status": "healthy"}

@app.get("/readyz")
def readyz():
    # Verify connectivity to RabbitMQ
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials, socket_timeout=1.0))
        connection.close()
    except Exception as e:
        logger.error(f"Readyz check failed (RabbitMQ offline): {e}")
        raise HTTPException(status_code=503, detail="RabbitMQ not reachable")
    
    # Verify connectivity to Redis
    if redis_client:
        try:
            redis_client.ping()
        except Exception as e:
            logger.error(f"Readyz check failed (Redis offline): {e}")
            raise HTTPException(status_code=503, detail="Redis cache not reachable")
    else:
        logger.error("Readyz check failed: Redis client is not initialized")
        raise HTTPException(status_code=503, detail="Redis client not initialized")
        
    return {"status": "ready"}

