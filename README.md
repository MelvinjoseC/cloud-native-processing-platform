# Cloud-Native Cost-Optimized Processing Platform with Scale-to-Zero Workers

![CI/CD Pipeline](https://github.com/MelvinjoseC/cloud-native-processing-platform/actions/workflows/ci.yml/badge.svg)
![Terraform](https://img.shields.io/badge/IaC-Terraform-blueviolet)
![AWS EKS](https://img.shields.io/badge/Kubernetes-EKS-orange)
![ArgoCD](https://img.shields.io/badge/GitOps-ArgoCD-blue)
![KEDA](https://img.shields.io/badge/Scaling-KEDA-green)
![FinOps](https://img.shields.io/badge/FinOps-Scale--to--Zero-success)

A production-grade, enterprise-scale distributed processing platform demonstrating dynamic load handling, scale-to-zero FinOps cost optimization, GitOps, and robust resilience architectures. Built using AWS EKS, KEDA (Kubernetes Event-driven Autoscaling), RabbitMQ, Redis, ArgoCD, and automated Chaos testing to deploy a high-throughput, self-healing processing queue.

---

## 🏗️ System Architecture

```
                                    ┌─────────────────────┐
                                    │    ArgoCD / GitOps  │
                                    └──────────┬──────────┘
                                               │ Syncs Manifests
                                               ▼
  ┌────────────────────────────────────────────EKS Cluster (VPC)─────────────────────────────────────────────┐
  │                                                                                                          │
  │     Client / Gateway      ┌────────────────────────┐                                                     │
  │    ────────────────►      │   Producer (FastAPI)   ├────────┐                                             │
  │    (X-Request-ID)         └───────────┬────────────┘        │                                             │
  │                                       │ (AMQP Pool)         │ Writes State (Redis Pool)                   │
  │                                       ▼                     ▼                                             │
  │                           ┌────────────────────────┐   ┌─────────────┐                                    │
  │                           │   RabbitMQ Broker      │   │ Redis Cache │                                    │
  │                           └───────────┬────────────┘   └─────────────┘                                    │
  │                                       │                     ▲                                             │
  │                                       │                     │ Reads/Writes                                │
  │                                       ▼                     │                                             │
  │                           ┌────────────────────────┐        │                                             │
  │                           │   Worker Pods          ├────────┘                                             │
  │                           │ (Scale: 0 to 10)       ├────────────────────────────────┐                     │
  │                           └───────────▲────────────┘                                │ Archival            │
  │                                       │                                             ▼                     │
  │                                       │ Scales via KEDA ScaledObject      ┌───────────────────┐           │
  │                                       │                                   │ AWS S3 (Glacier)  │           │
  │                           ┌───────────┴────────────┐                      └───────────────────┘           │
  │                           │    KEDA Controller     ├──────────────────────────┐                           │
  │                           └────────────────────────┘                          │ Scrapes Metrics           │
  │                                                                               ▼                           │
  │     Observability Stack:                                            ┌───────────────────┐                 │
  │     [ Grafana Dashboards ] ◄────────────────────────────────────────┤  Prometheus / OTel│                 │
  │     [ Grafana Loki (Logs) ]                                         └───────────────────┘                 │
  │                                                                                                          │
  └──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Technology Stack & Core Design

| Component | Technology | Production Role |
|---|---|---|
| **Infrastructure** | Terraform, AWS VPC, EKS, IRSA, AWS Budgets | Secure multi-AZ networking, least-privilege IAM, and automated monthly spend guardrails. |
| **Compute Pools** | AWS Spot Instances, On-Demand System Nodes, KEDA | Scales consumers completely to `0` when idle; isolates critical state on reliable On-Demand nodes. |
| **Ingestion API** | Python 3.12, FastAPI, Pydantic v2, Uvicorn | High-throughput asynchronous REST API with connection pools, rate limiting, and batch endpoints. |
| **Broker & State** | RabbitMQ StatefulSet, Redis StatefulSet | Durable message queuing with DLQ, priority queuing (`x-max-priority`), and low-latency state caching. |
| **GitOps CD** | ArgoCD | Enforces continuous reconciliation directly from version control. |
| **Observability** | Prometheus, OpenTelemetry Collector, Grafana, Loki | Distributed request tracing, worker deep-dive dashboards, and proactive alerting rules. |
| **Reliability** | Pod Disruption Budgets (PDB), NetworkPolicies | Guarantees high availability during disruptions and enforces zero-trust container network isolation. |
| **Testing & Chaos** | Pytest, Async Load Tester, Bash Chaos Script | Comprehensive unit tests, high-concurrency benchmarks, and automated runtime chaos injection. |

---

## 🌐 API Endpoints & Contract Reference

All endpoints include distributed tracing support (`X-Request-ID` header propagation) and request validation.

| Method | Endpoint | Description | Status Code |
|---|---|---|---|
| `POST` | `/api/v1/jobs` | Submit a single job with task type, duration, and priority (1-10) | `202 Accepted` |
| `POST` | `/api/v1/jobs/batch` | Atomically submit up to 100 jobs in a single request (Redis pipeline) | `202 Accepted` |
| `GET` | `/api/v1/jobs` | List cached jobs with status filtering (`status=COMPLETED`) and pagination | `200 OK` |
| `GET` | `/api/v1/jobs/{job_id}` | Retrieve comprehensive job status, execution timeline, and results | `200 OK` |
| `DELETE` | `/api/v1/jobs/{job_id}` | Cancel pending/processing job or remove completed job from cache | `200 OK` |
| `POST` | `/api/v1/jobs/{job_id}/retry` | Re-queue a failed or cancelled job back into RabbitMQ with reset headers | `202 Accepted` |
| `GET` | `/healthz` | Kubernetes liveness probe endpoint | `200 OK` |
| `GET` | `/readyz` | Kubernetes readiness probe (validates RabbitMQ & Redis connectivity) | `200 OK` |
| `GET` | `/metrics` | Prometheus metrics endpoint | `200 OK` |

Interactive Swagger OpenAPI documentation is available locally at `http://localhost:8080/docs`.

---

## 💡 Key Architectural Highlights

### 1. Spot Instance Node Group & Scale-to-Zero Workers
The background `worker` deployment starts at `replicas: 0`.
* **Scale-to-Zero**: When the queue is empty, worker replicas scale to `0`, freeing 100% of consumer compute resources.
* **On-Demand Scaling**: As soon as tasks arrive, KEDA detects queue depth and provisions workers up to 10 replicas.
* **Spot Diversification**: Worker node pools run on discounted EC2 Spot instances (`t3.medium`, `t3a.medium`, `c5.large`), saving up to 90% compared to on-demand compute.

### 2. High-Performance Connection Pooling
* **Redis Connection Pool**: Configured with automatic reconnection, keep-alives, socket timeouts, and health-check intervals.
* **Thread-Safe AMQP Channel Pool**: Reuses long-lived AMQP channels under a thread lock, eliminating per-request TCP/AMQP handshake latency and ephemeral port exhaustion.

### 3. Distributed Tracing & Sliding Window Rate Limiting
* **End-to-End Correlation**: Automatically extracts or generates `X-Request-ID`, propagating it through HTTP responses, AMQP headers, worker execution context, and S3 metadata.
* **Rate Limiter**: Redis-backed sliding window rate limiter protects the producer from traffic spikes and DDoS, with thread-safe in-memory fallback.

### 4. Task Dispatcher Pattern & Execution Timeout Watchdog
* **Task Registry**: Dynamically routes workloads (`data_processing`, `image_resize`, `report_generation`) to specialized handlers.
* **Timeout Watchdog**: Terminates tasks that exceed maximum execution deadlines, routing poison pills to the Dead Letter Queue (`tasks_dlq`).

### 5. Dedicated Worker Telemetry & Health Server
* A lightweight HTTP server on port 9102 serves `/metrics`, `/healthz`, and `/readyz`, enabling native Kubernetes HTTP liveness and readiness probes on workers.

### 6. Operational Tooling & DLQ Replay Utility
* **DLQ Management CLI** (`scripts/replay_dlq.py`): Allows operators to count, inspect, purge, or safely replay dead-lettered messages back to the primary queue.
* **Async Load Tester** (`tests/load_test.py`): Generates concurrent batch traffic spikes to benchmark API latency and observe KEDA scale-out events.

### 7. FinOps Cost Guardrails & S3 Glacier Lifecycle
* **AWS Budgets**: Terraform provisions hard monthly cost limits with automated alerts at 80% and 100% thresholds.
* **S3 Glacier Archival**: Processed job payloads automatically transition to S3 Glacier after 90 days and permanently expire after 365 days.

---

## 📚 In-Depth Technical Guides

- [System Architecture & Event Flow](docs/ARCHITECTURE.md)
- [Zero-Trust Security Posture & IRSA](docs/SECURITY.md)
- [FinOps Cost Optimization Model](docs/COST_OPTIMIZATION.md)
- [Disaster Recovery & SOPs](docs/DISASTER_RECOVERY.md)
- [Incident Response Runbook](docs/RUNBOOK.md)

---

## 🚀 Getting Started

### Local Orchestration (Using Docker Compose)
```bash
docker compose up --build
```
* **Producer API**: [http://localhost:8080/docs](http://localhost:8080/docs)
* **RabbitMQ Console**: [http://localhost:15672](http://localhost:15672) (`guest` / `guest`)
* **Worker Telemetry**: [http://localhost:9102/metrics](http://localhost:9102/metrics)

### Running Automated Test Suite & Linter
```bash
# Run flake8 and black
flake8 app/ tests/ scripts/
black --check app/ tests/ scripts/

# Run unit tests with pytest
pytest tests/
```

### Simulating Traffic Spikes
```bash
# Run concurrent load test with 50 jobs
python tests/load_test.py --url http://localhost:8080 --total 50 --concurrency 10

# Run batch submission load test
python tests/load_test.py --url http://localhost:8080 --total 100 --batch
```

### Managing the Dead Letter Queue
```bash
# Check DLQ message count
python scripts/replay_dlq.py --action count

# Peek at the top dead-lettered message
python scripts/replay_dlq.py --action peek

# Replay dead-lettered messages back to the primary queue
python scripts/replay_dlq.py --action replay --max-messages 50
```

---

## 💥 Chaos & Resilience Validation

To verify the platform's self-healing capabilities, execute the automated chaos testing script:

```bash
chmod +x chaos/test-resilience.sh
./chaos/test-resilience.sh
```

---

## 📋 Suggested Resume Bullet Points

* **Architected and deployed** a *Cloud-Native Processing Platform* on AWS EKS with KEDA, scaling worker pools from `0` to `10` replicas based on RabbitMQ queue depths.
* **Reduced compute infrastructure costs by ~85%** by leveraging AWS Spot Instances, scale-to-zero autoscaling, and S3 Glacier lifecycle tiering.
* **Implemented resilient event-driven architectures** with connection pooling, Dead Letter Queue (DLQ) retry policies, and `SIGTERM` task drainage (`terminationGracePeriodSeconds: 120`).
* **Secured Kubernetes microservices** using IAM Roles for Service Accounts (IRSA), PodDisruptionBudgets, and NetworkPolicies with zero-trust isolation.
* **Engineered operational SRE tooling**, including an async load tester, DLQ replay CLI, and an OpenTelemetry Collector telemetry pipeline with Grafana dashboards.
