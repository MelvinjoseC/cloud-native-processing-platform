# Disaster Recovery & Business Continuity Guide

## 1. Recovery Objectives

- **Recovery Point Objective (RPO)**: < 5 minutes (via RabbitMQ durable message persistence and S3 versioning).
- **Recovery Time Objective (RTO)**: < 15 minutes (via automated Terraform IaC and Helm/Kustomize deployment).

---

## 2. Failure Scenarios & Standard Operating Procedures (SOPs)

### Scenario A: Worker Pod Crash-Loop or Poison Pill Flood
- **Symptoms**: `DeadLetterQueueHasMessages` or `WorkerHighTaskFailureRate` alert fires in Prometheus.
- **Root Cause**: Corrupted input payload or unhandled exception in task handler.
- **SOP**:
  1. Inspect dead letter queue count and error details:
     ```bash
     python scripts/replay_dlq.py --action peek --src-queue tasks_dlq
     ```
  2. Identify and fix bug in task handler or producer validation schema.
  3. Deploy updated worker image.
  4. Replay dead-lettered messages back to primary queue:
     ```bash
     python scripts/replay_dlq.py --action replay --src-queue tasks_dlq --dst-queue tasks --max-messages 500
     ```

### Scenario B: EC2 Spot Instance Interruption Notice
- **Event**: AWS issues 2-minute Spot termination notification.
- **Automated Protection**:
  1. Kubelet receives SIGTERM and forwards it to worker pods.
  2. Worker process intercepts SIGTERM in `sigterm_handler`.
  3. In-flight tasks with duration < remaining grace period finish cleanly.
  4. Active tasks unable to complete in time are safely rejected via `basic_nack(requeue=True)` and reset to `PENDING` in Redis.
  5. The AMQP broker immediately redistributes the task to surviving or newly spawned workers without data loss.

### Scenario C: Complete Availability Zone Outage
- **Automated Protection**:
  - `TopologySpreadConstraints` spread producer pods across multiple availability zones (`topology.kubernetes.io/zone`).
  - EKS cluster subnets span 3 availability zones in VPC configuration.
  - Multi-instance type diversification (`t3.medium`, `t3a.medium`, `c5.large`) ensures that if one instance type is exhausted in an AZ, alternative spot pools are used.
