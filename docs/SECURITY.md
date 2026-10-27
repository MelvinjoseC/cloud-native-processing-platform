# Security Posture & Threat Modeling

## 1. Zero-Trust Kubernetes Architecture

The platform follows Defense-in-Depth and Least Privilege principles across container, network, identity, and infrastructure layers.

### A. Container Sandboxing
- **Non-Root Execution**: Both `producer` and `worker` Docker containers run with non-root security contexts (`UID 1000`, `GID 1000`, `runAsNonRoot: true`).
- **Read-Only Root Filesystems**: Temporary files are isolated in volatile `emptyDir` mounts; persistent data is routed strictly to managed PVCs.
- **Minimal Base Images**: Multi-stage Docker builds based on `python:3.12-slim` with stripped build tools and package caches to minimize CVE surface area.

### B. Network Segmentation
- **Kubernetes NetworkPolicies**:
  - Restricts ingress/egress so that only authorized pods (e.g. producer and worker) can communicate with RabbitMQ (port 5672) and Redis (port 6379).
  - External internet egress from workers is restricted strictly to AWS S3 and STS endpoints.

### C. Identity & Cloud IAM (IRSA)
- **IAM Roles for Service Accounts (IRSA)**:
  - Eliminates long-lived static AWS access keys. Pods authenticate dynamically via OIDC token federation.
  - S3 payload archival permissions are scoped strictly to `s3:PutObject` on `results/*` prefixes within the dedicated platform bucket.

### D. Envelope Encryption & Secret Management
- **AWS KMS Envelope Encryption**: EKS control plane uses dedicated customer managed KMS keys (`aws_kms_key.eks_secrets`) with automatic yearly key rotation.
- **K8s Secrets**: Passwords for RabbitMQ and Redis are stored as Kubernetes Secrets, injected exclusively via environment variables using `valueFrom.secretKeyRef`.

### E. Static Analysis & Vulnerability Scanning
- **Trivy IaC & Container Scanners**: Continuous GitHub Actions CI scans all Terraform configurations and built container images for HIGH and CRITICAL vulnerabilities.
- **Hadolint**: Enforces Dockerfile best practices and lint checks.
