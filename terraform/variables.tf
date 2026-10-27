variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

variable "cluster_name" {
  description = "Name of the EKS cluster"
  type        = string
  default     = "cost-optimized-eks"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "monthly_budget_limit" {
  description = "Monthly budget limit in USD for platform cost guardrails"
  type        = string
  default     = "50"
}

variable "alert_email" {
  description = "Email recipient for AWS Budget cost alert notifications"
  type        = string
  default     = "platform-alerts@example.com"
}
