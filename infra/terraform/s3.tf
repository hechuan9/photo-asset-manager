resource "aws_s3_bucket" "derivative" {
  bucket = local.derivative_bucket_name
}

resource "aws_s3_bucket_public_access_block" "derivative" {
  bucket = aws_s3_bucket.derivative.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "derivative" {
  bucket = aws_s3_bucket.derivative.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "derivative" {
  bucket = aws_s3_bucket.derivative.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_cors_configuration" "derivative" {
  bucket = aws_s3_bucket.derivative.id

  cors_rule {
    allowed_headers = ["Authorization", "Content-Type", "Content-MD5", "Range", "x-amz-*"]
    allowed_methods = ["GET", "HEAD", "PUT", "POST"]
    allowed_origins = var.derivative_bucket_cors_allowed_origins
    expose_headers  = ["ETag", "Content-Length", "x-amz-request-id", "x-amz-id-2"]
    max_age_seconds = 300
  }
}
