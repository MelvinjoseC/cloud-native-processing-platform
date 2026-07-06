import sys
import os
import json
from unittest.mock import MagicMock, patch

# Add app/producer to path so we can import it
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../app/producer')))

# Create mock classes to intercept calls at import time
mock_redis_client = MagicMock()
mock_pika_channel = MagicMock()
mock_pika_connection = MagicMock()

mock_pika_connection.channel.return_value = mock_pika_channel

# Set up patching before importing the app
with patch('redis.Redis', return_value=mock_redis_client), \
     patch('pika.BlockingConnection', return_value=mock_pika_connection):
    import main as producer_app
    from fastapi.testclient import TestClient
    client = TestClient(producer_app.app)

def test_healthz():
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}

def test_readyz_success():
    # Reset mocks
    mock_pika_connection.reset_mock()
    mock_redis_client.reset_mock()
    
    # Configure mock behavior
    mock_redis_client.ping.return_value = True
    
    # Run readyz
    with patch('main.redis_client', mock_redis_client):
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
    mock_redis_client.ping.side_effect = Exception("Redis down")
    
    with patch('main.redis_client', mock_redis_client):
        response = client.get("/readyz")
        assert response.status_code == 503
        assert "Redis cache not reachable" in response.json()["detail"]

def test_submit_job_success():
    # Mock redis set and rabbitmq basic_publish
    mock_redis_client.reset_mock()
    mock_pika_channel.reset_mock()
    
    job_payload = {
        "task_type": "resize-image",
        "duration_seconds": 10,
        "payload": {"image_url": "http://example.com/img.png"}
    }
    
    with patch('main.redis_client', mock_redis_client):
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
    
    with patch('main.redis_client', mock_redis_client):
        response = client.post("/jobs", json=job_payload)
        assert response.status_code == 503
        assert "Task queue unavailable" in response.json()["detail"]
        
        # Verify job was set to FAILED in redis
        # First call to set() is to save PENDING state, second call to set FAILED state
        assert mock_redis_client.set.call_count == 2
        args, kwargs = mock_redis_client.set.call_args
        assert "FAILED" in args[0][1]

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
    
    with patch('main.redis_client', mock_redis_client):
        response = client.get(f"/jobs/{job_id}")
        assert response.status_code == 200
        assert response.json() == job_data
        mock_redis_client.get.assert_called_once_with(f"job:{job_id}")

def test_get_job_status_not_found():
    mock_redis_client.reset_mock()
    mock_redis_client.get.return_value = None
    
    with patch('main.redis_client', mock_redis_client):
        response = client.get("/jobs/non-existent")
        assert response.status_code == 404
        assert "Job not found" in response.json()["detail"]
