# Operational Runbook: Cloud-Native Processing Platform

This runbook outlines daily operations, observability, troubleshooting, and recovery procedures for the Cloud-Native Processing Platform.

---

## 1. System Architecture Summary

The platform consists of four primary operational layers:
1. **API Ingress Layer**: FastAPI Producer microservice receiving HTTP task requests, validating inputs via Pydantic, and buffering state in Redis.
2. **Message Broker Layer**: RabbitMQ 3.12 StatefulSet with persistent storage and a dedicated Dead Letter Queue (`tasks_dlq`) for poison pill quarantine.
3. **Consumer Worker Layer**: Auto-scaling Python workers running on AWS EC2 Spot instances managed by KEDA (0 to 10/20 replicas) with SIGTERM graceful interception and S3 payload archiving.
4. **Telemetry & Observability Stack**: Prometheus metrics scraping (ports 8000 & 9102), Promtail JSON log shipping, Loki, and Grafana dashboard visualization.

---

## 2. Standard Operating Procedures

### 2.1 Verifying Platform Health
To check readiness of all cluster services:
```bash
# Check pod lifecycle status
kubectl get pods -n processing-platform

# Verify Producer readiness and RabbitMQ/Redis connectivity
curl -i http://<PRODUCER_ENDPOINT>/readyz
```

### 2.2 Checking Autoscaling & Queue Depth
Inspect KEDA ScaledObject and queue length:
```bash
# View active KEDA ScaledObject status
kubectl get scaledobject -n processing-platform

# Check current worker deployment replica count
kubectl get deployment worker -n processing-platform
```

### 2.3 Inspecting the Dead Letter Queue (DLQ)
When tasks fail beyond `MAX_RETRIES` (default: 3), workers automatically forward them to `tasks_dlq`:
```bash
# Port-forward to RabbitMQ Management UI
kubectl port-forward svc/rabbitmq 15672:15672 -n processing-platform
# Open http://localhost:15672 (User: guest, Password: from platform-secrets)
# Check the 'tasks_dlq' queue under the Queues tab
```

---

## 3. Troubleshooting & Incident Response

### 3.1 Worker Crash Loops
**Symptom**: `WorkerCrashLooping` alert fires or pod restarts increase.
**Diagnosis**:
```bash
kubectl logs -n processing-platform -l app=worker --tail=100
```
**Remediation**:
- Verify Redis credentials: ensure `REDIS_PASSWORD` matches `platform-secrets`.
- Check RabbitMQ reachability: test AMQP port 5672 connectivity.

### 3.2 High Queue Backlog with Workers at Zero Replicas
**Symptom**: `QueueBacklogHigh` alert fires, but worker replicas remain 0.
**Diagnosis**:
```bash
kubectl describe scaledobject rabbitmq-worker-scaler -n processing-platform
kubectl logs -n keda -l app=keda-operator --tail=50
```
**Remediation**:
- Check AMQP connection string in ScaledObject triggers.
- Ensure KEDA operator has network access to RabbitMQ in `processing-platform` namespace.

### 3.3 Poison Pill Recovery
**Symptom**: `DeadLetterQueueHasMessages` alert fires.
**Diagnosis**:
- Query Redis for job status `FAILED`:
  ```bash
  curl -s http://<PRODUCER_ENDPOINT>/api/v1/jobs?status=FAILED
  ```
- Inspect error details and stack trace in Redis job data or Loki logs filtered by `{app="worker"} |= "Routing poison message"`.

---

## 4. Multi-Environment Deployment

### Development Overlay
```bash
kubectl apply -k k8s/overlays/dev
```

### Production Overlay (HA Anti-Affinity & Spot IRSA)
```bash
kubectl apply -k k8s/overlays/prod
```

### Helm Deployment
```bash
helm upgrade --install platform ./helm/cost-optimizer-app -n processing-platform --create-namespace
```
