import sys
import os
import json
import importlib.util
from unittest.mock import MagicMock, patch

# Load app/worker/main.py dynamically as worker_main to avoid collision
spec = importlib.util.spec_from_file_location(
    "worker_main",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../app/worker/main.py")),
)
worker_app = importlib.util.module_from_spec(spec)
sys.modules["worker_main"] = worker_app

mock_redis_client = MagicMock()
mock_pika_channel = MagicMock()
mock_pika_connection = MagicMock()

mock_pika_connection.channel.return_value = mock_pika_channel

# Set up patching before executing the module code
with patch("redis.Redis", return_value=mock_redis_client), patch(
    "pika.BlockingConnection", return_value=mock_pika_connection
):
    spec.loader.exec_module(worker_app)


def test_process_message_success():
    # Reset mocks
    mock_redis_client.reset_mock()
    mock_pika_channel.reset_mock()

    # Define test message body
    task_payload = {
        "job_id": "test-job-456",
        "task_type": "compress-video",
        "duration_seconds": 1,
    }
    body = json.dumps(task_payload).encode("utf-8")

    # Mock parameters
    ch = MagicMock()
    method = MagicMock()
    method.delivery_tag = 42
    properties = MagicMock()

    with patch("worker_main.redis_client", mock_redis_client):
        # We also mock time.sleep so the test runs instantly
        with patch("time.sleep", return_value=None):
            worker_app.process_message(ch, method, properties, body)

    # Verify redis calls
    assert mock_redis_client.set.call_count == 2
    # Verify ack call
    ch.basic_ack.assert_called_once_with(delivery_tag=42)


def test_process_message_shutdown_aborted():
    # Reset mocks
    mock_redis_client.reset_mock()
    mock_pika_channel.reset_mock()

    task_payload = {
        "job_id": "test-job-shutdown",
        "task_type": "compress-video",
        "duration_seconds": 3,
    }
    body = json.dumps(task_payload).encode("utf-8")

    ch = MagicMock()
    method = MagicMock()
    method.delivery_tag = 100
    properties = MagicMock()

    # Set shutdown requested to True and mock cleanup_and_exit
    with patch("worker_main.redis_client", mock_redis_client), patch(
        "worker_main.shutdown_requested", True
    ), patch("worker_main.cleanup_and_exit") as mock_exit:
        worker_app.process_message(ch, method, properties, body)

        # Verify nack call with requeue=True
        ch.basic_nack.assert_called_once_with(delivery_tag=100, requeue=True)
        # Verify exit was triggered in finally block because shutdown_requested is True
        mock_exit.assert_called_once()

        # Verify status in Redis was set back to PENDING
        assert mock_redis_client.set.call_count == 2
        args, kwargs = mock_redis_client.set.call_args
        assert "PENDING" in args[1]


def test_process_message_general_failure():
    # Reset mocks
    mock_redis_client.reset_mock()
    mock_pika_channel.reset_mock()

    # Pass invalid JSON body to cause an exception
    body = b"invalid json"

    ch = MagicMock()
    method = MagicMock()
    method.delivery_tag = 50
    properties = MagicMock()

    with patch("worker_main.redis_client", mock_redis_client):
        worker_app.process_message(ch, method, properties, body)

    # Verify nack call with requeue=True
    ch.basic_nack.assert_called_once_with(delivery_tag=50, requeue=True)


def test_sigterm_handler_active():
    # If processing is active, sigterm_handler sets shutdown_requested=True but does not exit
    with patch("worker_main.cleanup_and_exit") as mock_exit, patch(
        "worker_main.processing_active", True
    ):
        # Reset state
        worker_app.shutdown_requested = False

        worker_app.sigterm_handler(None, None)

        assert worker_app.shutdown_requested is True
        mock_exit.assert_not_called()


def test_sigterm_handler_idle():
    # If processing is NOT active, sigterm_handler exits immediately
    with patch("worker_main.cleanup_and_exit") as mock_exit, patch(
        "worker_main.processing_active", False
    ):
        worker_app.shutdown_requested = False

        worker_app.sigterm_handler(None, None)

        assert worker_app.shutdown_requested is True
        mock_exit.assert_called_once()
        # Reset state for following tests
        worker_app.shutdown_requested = False


def test_process_message_cancelled_job():
    mock_redis_client.reset_mock()
    mock_pika_channel.reset_mock()

    task_payload = {
        "job_id": "job-cancelled-123",
        "task_type": "compress-video",
        "duration_seconds": 10,
    }
    body = json.dumps(task_payload).encode("utf-8")

    ch = MagicMock()
    method = MagicMock()
    method.delivery_tag = 77
    properties = MagicMock()
    properties.headers = None

    cancelled_job = {"job_id": "job-cancelled-123", "status": "CANCELLED"}
    mock_redis_client.get.return_value = json.dumps(cancelled_job)

    with patch("worker_main.redis_client", mock_redis_client):
        worker_app.process_message(ch, method, properties, body)

    ch.basic_ack.assert_called_once_with(delivery_tag=77)


def test_process_message_dlq_routing():
    mock_redis_client.reset_mock()
    mock_pika_channel.reset_mock()

    body = b"poison-pill-data"

    ch = MagicMock()
    method = MagicMock()
    method.delivery_tag = 99

    properties = MagicMock()
    properties.headers = {"x-retry-count": 3}

    with patch("worker_main.redis_client", mock_redis_client):
        worker_app.process_message(ch, method, properties, body)

    ch.basic_publish.assert_called_once()
    assert ch.basic_publish.call_args[1]["routing_key"] == "tasks_dlq"
    ch.basic_ack.assert_called_once_with(delivery_tag=99)


def test_archive_payload_to_s3():
    mock_s3 = MagicMock()
    task = {"job_id": "s3-job-1", "status": "COMPLETED"}

    with patch("worker_main.s3_client", mock_s3), patch(
        "worker_main.S3_BUCKET", "my-test-bucket"
    ):
        worker_app.archive_payload_to_s3("s3-job-1", task)
        mock_s3.put_object.assert_called_once_with(
            Bucket="my-test-bucket",
            Key="results/s3-job-1.json",
            Body=json.dumps(task),
            ContentType="application/json",
        )


def test_archive_payload_to_s3_disabled():
    mock_s3 = MagicMock()
    task = {"job_id": "s3-job-2", "status": "COMPLETED"}

    with patch("worker_main.s3_client", None), patch("worker_main.S3_BUCKET", None):
        worker_app.archive_payload_to_s3("s3-job-2", task)
        mock_s3.put_object.assert_not_called()


def test_worker_task_dispatcher_dispatch():
    mock_redis_client.reset_mock()
    mock_redis_client.get.return_value = None
    mock_redis_client.get.side_effect = None
    ch = MagicMock()
    method = MagicMock()
    method.delivery_tag = 10
    properties = MagicMock()
    properties.correlation_id = "corr-10"
    properties.headers = {"x-request-id": "req-10"}

    task_payload = {
        "job_id": "dispatch-job-1",
        "task_type": "image_resize",
        "duration_seconds": 1,
        "payload": {"width": 1920, "height": 1080},
    }
    body = json.dumps(task_payload).encode("utf-8")

    with patch("worker_main.redis_client", mock_redis_client), patch("time.sleep"):
        worker_app.process_message(ch, method, properties, body)

    ch.basic_ack.assert_called_once_with(delivery_tag=10)
    args, kwargs = mock_redis_client.set.call_args
    completed_task = json.loads(args[1])
    assert completed_task["status"] == "COMPLETED"
    assert completed_task["result"]["target_resolution"] == "1920x1080"


def test_worker_task_timeout():
    mock_redis_client.reset_mock()
    mock_redis_client.get.return_value = None
    mock_redis_client.get.side_effect = None
    ch = MagicMock()
    method = MagicMock()
    method.delivery_tag = 20
    properties = MagicMock()
    properties.correlation_id = "corr-20"
    properties.headers = {}

    task_payload = {
        "job_id": "timeout-job-1",
        "task_type": "heavy_job",
        "duration_seconds": 500,
        "timeout_seconds": 10,
    }
    body = json.dumps(task_payload).encode("utf-8")

    with patch("worker_main.redis_client", mock_redis_client), patch(
        "worker_main.TASK_MAX_TIMEOUT_SECONDS", 10
    ):
        worker_app.process_message(ch, method, properties, body)

    ch.basic_nack.assert_called_once_with(delivery_tag=20, requeue=True)
