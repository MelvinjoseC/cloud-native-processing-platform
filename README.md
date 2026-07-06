# Cloud-Native Cost-Optimized Processing Platform with Scale-to-Zero Workers

![CI/CD Pipeline](https://github.com/MelvinjoseC/fastapi-k8s-helm-cicd/actions/workflows/ci.yml/badge.svg)
![Terraform](https://img.shields.io/badge/IaC-Terraform-blueviolet)
![AWS EKS](https://img.shields.io/badge/Kubernetes-EKS-orange)
![ArgoCD](https://img.shields.io/badge/GitOps-ArgoCD-blue)
![KEDA](https://img.shields.io/badge/Scaling-KEDA-green)

A production-grade, enterprise-scale microservice platform demonstrating dynamic load handling, cost optimization, GitOps, and robust resilience architectures. This project showcases how to combine AWS EKS, KEDA (Kubernetes Event-driven Autoscaling), ArgoCD, and automated Chaos testing to deploy a self-healing, cost-efficient processing queue.

---

## 🏗️ Architecture

```
                                    ┌─────────────────────┐
                                    │    ArgoCD / GitOps  │
                                    └──────────┬──────────┘
                                               │ Syncs Manifests
                                               ▼
  ┌────────────────────────────────────────────EKS Cluster (VPC)─────────────────────────────────────────────┐
  │                                                                                                          │
  │     Client/HTTP           ┌────────────────────────┐                                                     │
  │    ───────────────►       │   Producer (FastAPI)   ├────────┐                                             │
  │                           └───────────┬────────────┘        │                                             │
  │                                       │ (Pub/Sub)           │ Writes State                                │
  │                                       ▼                     ▼                                             │
  │                           ┌────────────────────────┐   ┌─────────────┐                                    │
  │                           │   RabbitMQ Broker      │   │ Redis Cache │                                    │
  │                           └───────────┬────────────┘   └─────────────┘                                    │
  │                                       │                     ▲                                             │
  │                                       │                     │ Reads/Writes                                │
  │                                       ▼                     │                                             │
  │                           ┌────────────────────────┐        │                                             │
  │                           │   Worker Pods          ├────────┘                                             │
  │                           │ (Scale: 0 to 10)       │                                                      │
  │                           └───────────▲────────────┘                                                      │
  │                                       │                                                                   │
  │                                       │ Scales via KEDA ScaledObject                                      │
  │                                       │                                                                   │
  │                           ┌───────────┴────────────┐                                                      │
  │                           │    KEDA Controller     ├──────────────────────────┐                           │
  │                           └────────────────────────┘                          │                           │
  │                                                                               │ Scrapes Metrics           │
  │                                                                               ▼                           │
  │     Observability Stack:                                            ┌───────────────────┐                 │
  │     [ Grafana Dashboards ] ◄────────────────────────────────────────┤    Prometheus     │                 │
  │     [ Grafana Loki (Logs) ]                                         └───────────────────┘                 │
  │                                                                                                          │
  └──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Technology Stack & Core Design

| Component | Technology | Practical Purpose |
|---|---|---|
| **Infrastructure** | Terraform, AWS VPC, EKS, IAM Roles for Service Accounts (IRSA) | Automates secure, isolated AWS networking and least-privilege service configurations. |
| **Cost Optimization**| AWS EKS Spot Instances, KEDA | Runs pods on discounted EC2 Spot nodes, and scales consumers completely to `0` when idle. |
| **Application** | Python 3.12, FastAPI, RabbitMQ, Redis | High-performance asynchronous REST API, persistent task broker, and low-latency status caching. |
| **GitOps CD** | ArgoCD | Enforces declarative state reconciliation directly from Git. |
| **Observability** | Prometheus, Grafana, Loki, Alertmanager | Full-stack telemetry monitoring task latency, queue backlogs, pod scale history, and unified logs. |
| **Reliability** | Pod Disruption Budgets (PDB), NetworkPolicies | Ensures high availability during maintenance and restricts pod networking traffic (least-privilege). |
| **Chaos Testing** | Bash Automation, Kubernetes API | Emulates runtime disasters (broker offline, pod termination) to validate self-healing and zero task loss. |

---

## 💡 Key Standout Features

### 1. Spot Instance Node Group & Scale-to-Zero Workers
The background `worker` deployment is configured with `replicas: 0`. 
*   **Scale-to-Zero**: When there are no tasks in RabbitMQ, no worker containers consume CPU or Memory.
*   **Scale-up**: As soon as tasks are pushed, KEDA detects the backlog and immediately provisions worker pods.
*   **Spot Billing**: Nodes are configured in Terraform to use AWS Spot Instances (`capacity_type = "SPOT"`), cutting EKS compute costs by up to 90%.

### 2. IAM Roles for Service Accounts (IRSA)
Rather than giving the EKS nodes wide AWS permissions, we configure an **OIDC provider** in [eks.tf](terraform/eks.tf). In [irsa.tf](terraform/irsa.tf), we create a specific IAM role with S3/SQS access policies that can only be assumed by the specific K8s ServiceAccount (`worker-sa`) in our namespace.

### 3. Graceful Shutdown & Zero Task Loss
When EKS scales down or EC2 reclaims a Spot instance, pods receive a `SIGTERM` signal. The worker code intercepts this signal:
*   **Early Abort & Requeue**: If the worker is in the middle of processing a job, it aborts execution immediately, resets the job state in Redis to `PENDING`, rejects the task (`nack` with `requeue=True`) so another worker picks it up immediately, and exits. This ensures that long-running jobs are not lost or stuck in processing when Kubernetes terminates the container.
*   **Automatic Requeue**: If the worker crashes abruptly, RabbitMQ automatically requeues the task so another worker picks it up—achieving **zero message loss**.

### 4. Exponential Backoff & Jitter
Worker connections to RabbitMQ implement an exponential backoff retry mechanism with random jitter. This prevents a thundering herd issue during startup spikes or service recovery, gracefully scaling reconnection attempts up to a maximum of 60 seconds.

---

## 🚀 Getting Started

### Local Orchestration (Using Docker Compose)
If you prefer not to spin up a full Kubernetes cluster locally, you can run the entire stack via Docker Compose:

1. **Start the stack**:
   ```bash
   docker compose up --build
   ```
   This automatically provisions RabbitMQ (with management console), Redis, the FastAPI job producer, and the worker consumer.

2. **Access the services**:
   - **Producer Swagger API**: [http://localhost:8080/docs](http://localhost:8080/docs)
   - **RabbitMQ Management Dashboard**: [http://localhost:15672](http://localhost:15672) (User: `guest`, Pass: `guest`)

3. **Teardown**:
   ```bash
   docker compose down
   ```

### Running Unit Tests
To run the automated Python unit test suite:

1. **Install dependencies**:
   ```bash
   pip install -r app/producer/requirements.txt -r app/worker/requirements.txt -r tests/requirements.txt
   ```

2. **Run tests**:
   ```bash
   pytest
   ```

### Local Kubernetes Emulation (Using `kind`)
To test KEDA autoscaling and resilience:

1.  **Start a local Kubernetes cluster**:
    ```bash
    kind create cluster --name keda-dev
    ```

2.  **Install KEDA**:
    ```bash
    helm repo add kedacore https://kedacore.github.io/charts
    helm repo update
    helm install keda kedacore/keda --namespace keda --create-namespace
    ```

3.  **Build and load local images**:
    ```bash
    docker build -t producer:dev ./app/producer
    docker build -t worker:dev ./app/worker
    kind load docker-image producer:dev --name keda-dev
    kind load docker-image worker:dev --name keda-dev
    ```

4.  **Deploy via Kustomize**:
    ```bash
    cd k8s/overlays/dev
    kustomize edit set image ghcr.io/melvinjosec/cloud-native-processing-platform-producer=producer:dev
    kustomize edit set image ghcr.io/melvinjosec/cloud-native-processing-platform-worker=worker:dev
    kubectl apply -k .
    ```

5.  **Access the API**:
    ```bash
    kubectl port-forward svc/producer 8080:80 -n processing-platform
    ```
    Access Swagger API docs at `http://localhost:8080/docs`.

---

## 💥 Chaos & Resilience Validation

To verify the platform's self-healing capabilities, execute the automated chaos testing script:

```bash
chmod +x chaos/test-resilience.sh
./chaos/test-resilience.sh
```

**What the Chaos script does:**
1.  Floods the broker with 50 jobs.
2.  Asserts KEDA provisions worker pods up from `0`.
3.  Terminates a running worker pod in the middle of processing.
4.  Deletes the active RabbitMQ broker container.
5.  Waits for node recovery and audits task completion state in Redis.
6.  Asserts **100% processing success** (zero dropped messages) and verifies workers scale back down to `0`.

---

## 📋 Suggested Resume Bullet Points

If you deploy or build upon this project, you can include the following bullet points on your resume:

*   **Designed and implemented** a *Cloud-Native Processing Platform* on AWS EKS using KEDA, scaling backend worker pools from `0` to `10` replicas based on RabbitMQ queue depths.
*   **Reduced cloud compute overhead** by leveraging EKS Spot Instances and scale-to-zero pod configurations, minimizing idle resource waste.
*   **Secured EKS pod identity** using IAM Roles for Service Accounts (IRSA) via Terraform, enforcing least-privilege access to AWS S3/SQS.
*   **Architected resilient caching** and message handling using Redis and RabbitMQ, ensuring zero message loss through worker signal intercepting (`SIGTERM` graceful shutdown).
*   **Integrated GitOps continuous delivery** workflows using ArgoCD to automate EKS cluster synchronization directly from version control.
*   **Validated system self-healing** by developing automated chaos scripts that simulated node/broker crashes during load spikes, verifying 100% service recovery.
