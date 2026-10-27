# Platform Architecture & Distributed System Design

## 1. Overview

The **Cloud-Native Cost-Optimized Processing Platform** is an enterprise-grade, event-driven distributed task execution platform engineered to minimize cloud infrastructure costs while providing high availability, fault tolerance, and sub-second scaling responsiveness.

```mermaid
flowchart TD
    Client["Client / API Gateway"] -->|HTTP / REST (X-Request-ID)| Ingress["NGINX Ingress Controller"]
    Ingress --> Producer["FastAPI Producer Cluster"]
    
    subgraph DataPlane ["Persistence & Messaging Layer"]
        Producer -->|Connection Pool| Redis[("Redis (StatefulSet)")]
        Producer -->|AMQP Channel Pool| RMQ[("RabbitMQ (StatefulSet)")]
        RMQ -->|Dead Letter Exchange| DLQ[("RabbitMQ DLQ (tasks_dlq)")]
    end
    
    subgraph Autoscaling ["KEDA Autoscaler"]
        KEDAOpr["KEDA Controller"] -->|Polls Queue Length| RMQ
        KEDAOpr -->|Scales 0 <-> N| WorkerDeploy["Worker Deployment"]
    end
    
    subgraph ComputePool ["Kubernetes Compute Nodes"]
        WorkerDeploy --> WorkerPods["Scale-to-Zero Worker Pods (Spot EC2)"]
    end
    
    WorkerPods -->|Prefetch QoS| RMQ
    WorkerPods -->|State & Execution Results| Redis
    WorkerPods -->|Payload Archival (Glacier Lifecycle)| S3[("AWS S3 Bucket")]
    WorkerPods -->|Watchdog / Telemetry| OTel["OTel Collector & Prometheus"]
```

## 2. Core Architectural Pillars

### A. Producer (Ingestion Layer)
- **FastAPI Async Engine**: Handles job submission (`/api/v1/jobs`), batching (`/api/v1/jobs/batch`), and cancellation with strict Pydantic v2 validation.
- **Connection Pooling**:
  - Redis connection pool with automatic retry, health check intervals, and socket timeout controls.
  - Thread-safe AMQP connection and channel pool eliminating connection churn and TCP/AMQP handshake overhead.
- **Distributed Tracing**: Automatically injects or propagates `X-Request-ID` across HTTP headers, message brokers, and structured worker logs.
- **Rate Limiting**: Sliding window rate limiter backed by Redis sorted sets with in-memory thread-safe fallback.

### B. Broker & State Storage
- **RabbitMQ**: Deployed as a Kubernetes StatefulSet with persistent storage (PVC) and configured with an `x-max-priority` priority queue. Poison messages exceeding maximum retries are routed to `tasks_dlq`.
- **Redis**: StatefulSet with persistent volume for sub-millisecond job state tracking (`PENDING`, `PROCESSING`, `COMPLETED`, `FAILED`, `CANCELLED`).

### C. Worker & Compute Scaling
- **Scale-to-Zero Workers**: When the queue is empty, worker replicas scale to `0`, freeing 100% of consumer compute resources.
- **Task Dispatcher Registry**: Extensible dispatch pattern executing specialized workloads (`data_processing`, `image_resize`, `report_generation`).
- **Resilience Watchdogs**: Execution timeout watchdog terminates hung tasks and routes poison pills to DLQ.
- **Spot Interruption Safe**: Configured with `terminationGracePeriodSeconds: 120` to complete tasks or cleanly nack and requeue tasks upon AWS EC2 Spot 2-minute interruption notices.
- **Telemetry Server**: Dedicated multi-endpoint HTTP server on port 9102 serving `/metrics`, `/healthz`, and `/readyz`.

### D. Observability & FinOps
- **OpenTelemetry & Prometheus**: End-to-end metrics scraping, Loki JSON structured logging, and Grafana platform & deep-dive dashboards.
- **AWS Budgets & Lifecycle Policies**: Automated FinOps spend guardrails and tiered S3 Glacier payload archival.
