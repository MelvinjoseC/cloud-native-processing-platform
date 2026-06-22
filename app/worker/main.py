import os
import sys
import time
import json
import logging
import signal
import redis
import pika

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("worker")

# Env configurations
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "tasks")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

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
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, socket_timeout=3.0, decode_responses=True)
    redis_client.ping()
    logger.info("Connected to Redis successfully.")
except Exception as e:
    logger.warning(f"Redis connection failed: {e}. Running without Redis status reporting.")

def process_message(ch, method, properties, body):
    global processing_active, shutdown_requested
    processing_active = True
    
    try:
        task = json.loads(body.decode("utf-8"))
        job_id = task.get("job_id")
        task_type = task.get("task_type")
        duration = task.get("duration_seconds", 5)
        
        logger.info(f"[{job_id}] Received task '{task_type}' (Simulated work: {duration}s)")
        
        # 1. Update status to PROCESSING in Redis
        if redis_client:
            try:
                task["status"] = "PROCESSING"
                task["started_at"] = time.time()
                redis_client.set(f"job:{job_id}", json.dumps(task), ex=3600)
            except Exception as e:
                logger.error(f"Failed to update redis: {e}")
        
        # 2. Simulate CPU-heavy or network-bound processing
        # In a real app, this could be image processing, data processing, etc.
        for elapsed in range(duration):
            if shutdown_requested:
                logger.warning(f"[{job_id}] Shutdown requested mid-processing!")
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
                
        # 4. Acknowledge message processing in RabbitMQ
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        # Reject message and requeue it so another worker can process it
        try:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        except Exception as nack_err:
            logger.error(f"Nack failed: {nack_err}")
            
    finally:
        processing_active = False
        if shutdown_requested:
            logger.info("Finished processing current message. Exiting due to shutdown request.")
            cleanup_and_exit()

def start_worker():
    global current_connection, current_channel
    
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300
    )
    
    while not shutdown_requested:
        try:
            logger.info("Connecting to RabbitMQ...")
            current_connection = pika.BlockingConnection(parameters)
            current_channel = current_connection.channel()
            
            # Ensure queue exists
            current_channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
            
            # Prefetch count = 1 to distribute load evenly among workers
            current_channel.basic_qos(prefetch_count=1)
            
            current_channel.basic_consume(
                queue=RABBITMQ_QUEUE,
                on_message_callback=process_message
            )
            
            logger.info("Worker started. Waiting for tasks...")
            current_channel.start_consuming()
            
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Connection lost. Reconnecting in 5 seconds... Detail: {e}")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Unexpected error: {e}. Reconnecting in 5 seconds...")
            time.sleep(5)

if __name__ == "__main__":
    start_worker()
