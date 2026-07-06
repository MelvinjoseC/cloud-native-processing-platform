import sys
import os
import json
import importlib.util
from unittest.mock import MagicMock, patch

# Load app/producer/main.py dynamically as producer_main to avoid collision
spec = importlib.util.spec_from_file_location(
    "producer_main",
    os.path.abspath(os.path.join(os.path.dirname(__file__), '../app/producer/main.py'))
)
producer_app = importlib.util.module_from_spec(spec)
sys.modules["producer_main"] = producer_app

# Create mock classes to intercept calls at import time
mock_redis_client = MagicMock()
mock_pika_channel = MagicMock()
mock_pika_connection = MagicMock()

mock_pika_connection.channel.return_value = mock_pika_channel

# Set up patching before executing the module code
with patch('redis.Redis', return_value=mock_redis_client), \
     patch('pika.BlockingConnection', return_value=mock_pika_connection):
    spec.loader.exec_module(producer_app)
    from fastapi.testclient import TestClient
    client = TestClient(producer_app.app)

def test_healthz():
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}

def test_readyz_success():
    # Reset mock state
    mock_pika_connection.reset_mock()
    mock_redis_client.ping.reset_mock()
    mock_redis_client.ping.return_value = True
    mock_redis_client.ping.side_effect = None
    
    with patch('producer_main.redis_client', mock_redis_client), \
         patch('pika.BlockingConnection', return_value=mock_pika_connection):
        response = client.get("/readyz")
        assert response.status_code == 200
        assert response.json() == {"status": "ready"}

def test_readyz_rabbitmq_failure():
    # Mock pika to fail
    with patch('pika.BlockingConnection', side_effect=Exception("RabbitMQ connection down")):
        response = client.get("/readyz")
        assert response.status_code == 503
        assert "RabbitMQ not reachable" in response.json()["detail"]

def test_readyz_redis_failure():
    mock_redis_client.ping.reset_mock()
    mock_redis_client.ping.side_effect = Exception("Redis down")
    
    with patch('producer_main.redis_client', mock_redis_client), \
         patch('pika.BlockingConnection', return_value=mock_pika_connection):
        response = client.get("/readyz")
        assert response.status_code == 503
        assert "Redis cache not reachable" in response.json()["detail"]

def test_submit_job_success():
    mock_redis_client.reset_mock()
    mock_pika_channel.reset_mock()
    
    job_payload = {
        "task_type": "resize-image",
        "duration_seconds": 10,
        "payload": {"image_url": "http://example.com/img.png"}
    }
    
    with patch('producer_main.redis_client', mock_redis_client), \
         patch('producer_main.get_rabbitmq_channel', return_value=(mock_pika_connection, mock_pika_channel)):
        response = client.post("/jobs", json=job_payload)
        assert response.status_code == 202
        data = response.json()
        assert "job_id" in data
        assert data["status"] == "PENDING"
        
        # Verify redis and rabbitmq were called
        mock_redis_client.set.assert_called_once()
        mock_pika_channel.basic_publish.assert_called_once()

def test_submit_job_rabbitmq_failure():
    mock_redis_client.reset_mock()
    mock_pika_channel.reset_mock()
    
    # Make rabbitmq publish fail
    mock_pika_channel.basic_publish.side_effect = Exception("Queue error")
    
    job_payload = {
        "task_type": "resize-image",
        "duration_seconds": 10,
        "payload": {}
    }
    
    with patch('producer_main.redis_client', mock_redis_client), \
         patch('producer_main.get_rabbitmq_channel', return_value=(mock_pika_connection, mock_pika_channel)):
        response = client.post("/jobs", json=job_payload)
        assert response.status_code == 503
        assert "Task queue unavailable" in response.json()["detail"]
        
        # Verify job was set to FAILED in redis
        assert mock_redis_client.set.call_count == 2
        args, kwargs = mock_redis_client.set.call_args
        assert "FAILED" in args[1]

def test_get_job_status_success():
    mock_redis_client.reset_mock()
    
    job_id = "test-job-123"
    job_data = {
        "job_id": job_id,
        "task_type": "resize-image",
        "status": "COMPLETED",
        "created_at": 1000.0,
        "completed_at": 1005.0
    }
    mock_redis_client.get.return_value = json.dumps(job_data)
    
    with patch('producer_main.redis_client', mock_redis_client):
        response = client.get(f"/jobs/{job_id}")
        assert response.status_code == 200
        assert response.json() == job_data
        mock_redis_client.get.assert_called_once_with(f"job:{job_id}")

def test_get_job_status_not_found():
    mock_redis_client.reset_mock()
    mock_redis_client.get.return_value = None
    
    with patch('producer_main.redis_client', mock_redis_client):
        response = client.get("/jobs/non-existent")
        assert response.status_code == 404
        assert "Job not found" in response.json()["detail"]
