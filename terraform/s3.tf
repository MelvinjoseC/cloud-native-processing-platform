# S3 Bucket for storing processed worker payloads

resource "aws_s3_bucket" "payloads" {
  bucket        = "${var.cluster_name}-processed-payloads"
  force_destroy = true

  tags = merge(local.common_tags, {
    Name = "${var.cluster_name}-processed-payloads"
  })
}

# Block all public access (Security Best Practice)
resource "aws_s3_bucket_public_access_block" "payloads" {
  bucket = aws_s3_bucket.payloads.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Enable server-side encryption with AWS managed key
resource "aws_s3_bucket_server_side_encryption_configuration" "payloads" {
  bucket = aws_s3_bucket.payloads.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Enable versioning for payloads to track modifications/history
resource "aws_s3_bucket_versioning" "payloads" {
  bucket = aws_s3_bucket.payloads.id
  versioning_configuration {
    status = "Enabled"
  }
}
