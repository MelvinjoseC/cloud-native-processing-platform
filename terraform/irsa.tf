# IAM Roles for Service Accounts (IRSA) Configuration
# Provides fine-grained AWS credentials to EKS pods without using node instance roles

# 1. Helper locals for OIDC parsing
locals {
  oidc_arn = aws_iam_openid_connect_provider.eks.arn
  oidc_url = replace(aws_eks_cluster.main.identity[0].oidc[0].issuer, "https://", "")
}

# 2. IAM Role for KEDA Operator (e.g. to scale based on AWS CloudWatch/SQS metrics if needed)
resource "aws_iam_role" "keda_operator" {
  name = "${var.cluster_name}-keda-operator-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRoleWithWebIdentity"
      Effect = "Allow"
      Principal = {
        Federated = local.oidc_arn
      }
      Condition = {
        StringEquals = {
          "${local.oidc_url}:sub" = "system:serviceaccount:keda:keda-operator"
        }
      }
    }]
  })
}

# Example IAM policy for KEDA to access cloud services (like SQS for scaling)
resource "aws_iam_policy" "keda_sqs_access" {
  name        = "${var.cluster_name}-keda-sqs-policy"
  description = "Allows KEDA to query SQS queue length for scaling"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "sqs:GetQueueAttributes"
        ]
        Effect   = "Allow"
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "keda_attach" {
  policy_arn = aws_iam_policy.keda_sqs_access.arn
  role       = aws_iam_role.keda_operator.name
}

# 3. IAM Role for Worker Microservice (e.g. if the background worker needs to write to S3 bucket)
resource "aws_iam_role" "worker_role" {
  name = "${var.cluster_name}-worker-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRoleWithWebIdentity"
      Effect = "Allow"
      Principal = {
        Federated = local.oidc_arn
      }
      Condition = {
        StringEquals = {
          # Targets the 'worker' service account in the 'processing-platform' namespace
          "${local.oidc_url}:sub" = "system:serviceaccount:processing-platform:worker-sa"
        }
      }
    }]
  })
}

resource "aws_iam_policy" "worker_s3_access" {
  name        = "${var.cluster_name}-worker-s3-policy"
  description = "Allows EKS workers to store processed payloads in S3"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Effect   = "Allow"
        Resource = [
          "arn:aws:s3:::${var.cluster_name}-processed-payloads",
          "arn:aws:s3:::${var.cluster_name}-processed-payloads/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "worker_attach" {
  policy_arn = aws_iam_policy.worker_s3_access.arn
  role       = aws_iam_role.worker_role.name
}
