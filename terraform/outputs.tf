output "vpc_id" {
  description = "The ID of the VPC"
  value       = aws_vpc.eks_vpc.id
}

output "eks_cluster_name" {
  description = "The EKS cluster name"
  value       = aws_eks_cluster.main.name
}

output "eks_cluster_endpoint" {
  description = "The endpoint for the EKS cluster API"
  value       = aws_eks_cluster.main.endpoint
}

output "s3_bucket_name" {
  description = "The name of the worker payloads S3 bucket"
  value       = aws_s3_bucket.payloads.id
}

output "worker_role_arn" {
  description = "The ARN of the IAM role for EKS worker pods"
  value       = aws_iam_role.worker_role.arn
}

output "keda_role_arn" {
  description = "The ARN of the IAM role for KEDA operator"
  value       = aws_iam_role.keda_operator.arn
}
