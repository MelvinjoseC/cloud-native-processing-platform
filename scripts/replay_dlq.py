#!/usr/bin/env python3
"""Dead Letter Queue (DLQ) Management and Replay Utility.

Allows inspection, counting, purges, and safe replay of dead-lettered
poison messages back to the primary processing queue once underlying
issues have been remediated.
"""

import argparse
import json
import logging
import os
import sys
import pika

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("dlq_tool")


def get_connection(host, port, user, password):
    credentials = pika.PlainCredentials(user, password)
    params = pika.ConnectionParameters(
        host=host,
        port=port,
        credentials=credentials,
        socket_timeout=5.0,
    )
    return pika.BlockingConnection(params)


def count_queue(channel, queue_name):
    res = channel.queue_declare(queue=queue_name, durable=True, passive=True)
    return res.method.message_count


def peek_dlq(channel, queue_name, limit=5):
    count = count_queue(channel, queue_name)
    logger.info("Queue '%s' currently holds %d messages.", queue_name, count)
    peeked = 0
    while peeked < min(limit, count):
        method_frame, header_frame, body = channel.basic_get(
            queue=queue_name, auto_ack=False
        )
        if not method_frame:
            break
        peeked += 1
        headers = header_frame.headers or {}
        reason = headers.get("x-death-reason", "Unknown")
        retries = headers.get("x-retry-count", 0)
        try:
            payload = json.loads(body.decode("utf-8"))
            job_id = payload.get("job_id", "unknown")
            task_type = payload.get("task_type", "unknown")
        except Exception:
            job_id = "unknown"
            task_type = "unknown"

        logger.info(
            "[%d/%d] Job ID: %s | Type: %s | Retries: %s | Death Reason: %s",
            peeked,
            count,
            job_id,
            task_type,
            retries,
            reason,
        )
        channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)
        break  # Nack requeues at head; only peek top item safely


def replay_dlq(channel, src_queue, dst_queue, max_messages=100, reset_retries=True):
    count = count_queue(channel, src_queue)
    if count == 0:
        logger.info("DLQ '%s' is empty. Nothing to replay.", src_queue)
        return 0

    replayed = 0
    channel.queue_declare(queue=dst_queue, durable=True)

    while replayed < min(max_messages, count):
        method_frame, header_frame, body = channel.basic_get(
            queue=src_queue, auto_ack=False
        )
        if not method_frame:
            break

        headers = dict(header_frame.headers or {})
        if reset_retries:
            headers["x-retry-count"] = 0
            headers.pop("x-death-reason", None)

        props = pika.BasicProperties(
            delivery_mode=2,
            headers=headers,
            correlation_id=header_frame.correlation_id,
        )

        channel.basic_publish(
            exchange="",
            routing_key=dst_queue,
            body=body,
            properties=props,
        )
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        replayed += 1

    logger.info(
        "Successfully replayed %d message(s) from '%s' to '%s'.",
        replayed,
        src_queue,
        dst_queue,
    )
    return replayed


def purge_queue(channel, queue_name):
    purged = channel.queue_purge(queue=queue_name)
    logger.warning(
        "Purged %d message(s) from '%s'.", purged.method.message_count, queue_name
    )
    return purged.method.message_count


def main():
    parser = argparse.ArgumentParser(description="DLQ Management & Replay Utility")
    parser.add_argument(
        "--action",
        choices=["count", "peek", "replay", "purge"],
        default="count",
        help="Action to perform",
    )
    parser.add_argument(
        "--host", default=os.getenv("RABBITMQ_HOST", "localhost"), help="RabbitMQ Host"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("RABBITMQ_PORT", 5672)),
        help="RabbitMQ Port",
    )
    parser.add_argument(
        "--user", default=os.getenv("RABBITMQ_USER", "guest"), help="RabbitMQ User"
    )
    parser.add_argument(
        "--password",
        default=os.getenv("RABBITMQ_PASS", "guest"),
        help="RabbitMQ Password",
    )
    parser.add_argument(
        "--src-queue",
        default=os.getenv("RABBITMQ_DLQ", "tasks_dlq"),
        help="Source Dead Letter Queue",
    )
    parser.add_argument(
        "--dst-queue",
        default=os.getenv("RABBITMQ_QUEUE", "tasks"),
        help="Destination Target Queue for replay",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=100,
        help="Maximum messages to replay in a single run",
    )
    parser.add_argument(
        "--reset-retries",
        action="store_true",
        default=True,
        help="Reset retry counter headers on replay",
    )

    args = parser.parse_args()

    try:
        connection = get_connection(args.host, args.port, args.user, args.password)
        channel = connection.channel()

        if args.action == "count":
            cnt = count_queue(channel, args.src_queue)
            logger.info("DLQ '%s' count: %d", args.src_queue, cnt)
        elif args.action == "peek":
            peek_dlq(channel, args.src_queue)
        elif args.action == "replay":
            replay_dlq(
                channel,
                args.src_queue,
                args.dst_queue,
                args.max_messages,
                args.reset_retries,
            )
        elif args.action == "purge":
            purge_queue(channel, args.src_queue)

        connection.close()
    except Exception as e:
        logger.error("DLQ operation failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
