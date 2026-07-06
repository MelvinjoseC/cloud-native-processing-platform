import sys
import os
import json
import importlib.util
from unittest.mock import MagicMock, patch

# Load app/worker/main.py dynamically as worker_main to avoid collision
spec = importlib.util.spec_from_file_location(
    "worker_main",
    os.path.abspath(os.path.join(os.path.dirname(__file__), '../app/worker/main.py'))
)
worker_app = importlib.util.module_from_spec(spec)
sys.modules["worker_main"] = worker_app

mock_redis_client = MagicMock()
mock_pika_channel = MagicMock()
mock_pika_connection = MagicMock()

mock_pika_connection.channel.return_value = mock_pika_channel

# Set up patching before executing the module code
with patch('redis.Redis', return_value=mock_redis_client), \
     patch('pika.BlockingConnection', return_value=mock_pika_connection):
    spec.loader.exec_module(worker_app)

def test_process_message_success():
    # Reset mocks
    mock_redis_client.reset_mock()
    mock_pika_channel.reset_mock()
    
    # Define test message body
    task_payload = {
        "job_id": "test-job-456",
        "task_type": "compress-video",
        "duration_seconds": 1
    }
    body = json.dumps(task_payload).encode('utf-8')
    
    # Mock parameters
    ch = MagicMock()
    method = MagicMock()
    method.delivery_tag = 42
    properties = MagicMock()
    
    with patch('worker_main.redis_client', mock_redis_client):
        # We also mock time.sleep so the test runs instantly
        with patch('time.sleep', return_value=None):
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
        "duration_seconds": 3
    }
    body = json.dumps(task_payload).encode('utf-8')
    
    ch = MagicMock()
    method = MagicMock()
    method.delivery_tag = 100
    properties = MagicMock()
    
    # Set shutdown requested to True and mock cleanup_and_exit
    with patch('worker_main.redis_client', mock_redis_client), \
         patch('worker_main.shutdown_requested', True), \
         patch('worker_main.cleanup_and_exit') as mock_exit:
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
    
    with patch('worker_main.redis_client', mock_redis_client):
        worker_app.process_message(ch, method, properties, body)
        
    # Verify nack call with requeue=True
    ch.basic_nack.assert_called_once_with(delivery_tag=50, requeue=True)

def test_sigterm_handler_active():
    # If processing is active, sigterm_handler sets shutdown_requested=True but does not exit
    with patch('worker_main.cleanup_and_exit') as mock_exit, \
         patch('worker_main.processing_active', True):
        # Reset state
        worker_app.shutdown_requested = False
        
        worker_app.sigterm_handler(None, None)
        
        assert worker_app.shutdown_requested is True
        mock_exit.assert_not_called()

def test_sigterm_handler_idle():
    # If processing is NOT active, sigterm_handler exits immediately
    with patch('worker_main.cleanup_and_exit') as mock_exit, \
         patch('worker_main.processing_active', False):
        worker_app.shutdown_requested = False
        
        worker_app.sigterm_handler(None, None)
        
        assert worker_app.shutdown_requested is True
        mock_exit.assert_called_once()
