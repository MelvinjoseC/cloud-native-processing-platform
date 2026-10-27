# FinOps & Cloud Cost Optimization Model

## 1. Executive Summary

This architecture achieves up to **80% - 90% compute and storage cost reduction** compared to traditional 24/7 provisioned processing infrastructure by combining:
1. Event-driven scale-to-zero autoscaling (KEDA)
2. EC2 Spot instance capacity diversification
3. Strict tier separation between control plane and data processing nodes
4. Automated S3 Glacier storage tiering and object expiration
5. Real-time AWS Budget cost guardrails

---

## 2. Architectural Cost Breakdown

### A. Compute: Scale-to-Zero Worker Pool
- **Standard Baseline Architecture**: 3 to 10 on-demand EC2 instances running 24/7/365 to handle variable peak traffic, resulting in 70%+ idle resource waste during off-peak hours.
- **Our Cost-Optimized Strategy**:
  - KEDA ScaledObject monitors RabbitMQ queue depth. When backlog = 0, worker deployment scales to **0 replicas**.
  - Worker node group (`spot_nodes`) has `min_size = 0`. With zero pods scheduled, the Cluster Autoscaler terminates idle EC2 instances.
  - Workers run on **AWS Spot instances** (`t3.medium`, `t3a.medium`, `c5.large`), saving up to 90% per instance hour compared to On-Demand rates.

### B. Node Tier Separation
- **On-Demand System Group (`system_nodes`)**: Houses critical cluster daemons (CoreDNS, CNI), stateful RabbitMQ, Redis, and API Producer pods to guarantee zero service interruption.
- **Spot Worker Group (`spot_nodes`)**: Isolated via Kubernetes node labels and tolerations (`lifecycle=spot`). Spot termination notices are handled gracefully via `terminationGracePeriodSeconds: 120`.

### C. Storage: Tiered Lifecycle & S3 Glacier
- In-memory state in Redis expires via TTL (1 hour default).
- Archived job payloads written to S3 automatically transition to **Amazon S3 Glacier Flexible Retrieval** after 90 days (lowering storage cost from \$0.023/GB to \$0.0036/GB).
- Payloads permanently expire and delete after 365 days.

### D. FinOps Guardrails & Alerting
- Integrated Terraform configuration (`terraform/budgets.tf`) sets hard monthly budget limits with automatic threshold alerts at 80% (actual spend) and 100% (forecasted spend).
