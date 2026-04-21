variable "aws_region" {
  description = "AWS region for the control plane and derivative storage."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name prefix for AWS resources."
  type        = string
  default     = "photo-asset-manager"
}

variable "environment" {
  description = "Deployment environment name."
  type        = string
  default     = "dev"
}

variable "terraform_state_bucket_name" {
  description = "Optional override for the Terraform remote state bucket name."
  type        = string
  default     = null
}

variable "terraform_state_lock_table_name" {
  description = "Optional override for the Terraform remote state lock table name."
  type        = string
  default     = null
}

variable "derivative_bucket_name" {
  description = "Optional override for the S3 derivative bucket name."
  type        = string
  default     = null
}

variable "aurora_cluster_identifier" {
  description = "Optional override for the Aurora cluster identifier."
  type        = string
  default     = null
}

variable "aurora_connection_secret_name" {
  description = "Optional override for the Secrets Manager secret that stores Aurora connection materials."
  type        = string
  default     = null
}

variable "runtime_function_name" {
  description = "Optional override for the control-plane Lambda function name."
  type        = string
  default     = null
}

variable "api_name" {
  description = "Optional override for the HTTP API name."
  type        = string
  default     = null
}

variable "lambda_log_group_name" {
  description = "Optional override for the Lambda CloudWatch log group."
  type        = string
  default     = null
}

variable "api_access_log_group_name" {
  description = "Optional override for the API Gateway access log group."
  type        = string
  default     = null
}

variable "database_name" {
  description = "Aurora PostgreSQL database name."
  type        = string
  default     = "photo_asset_manager"
}

variable "db_master_username" {
  description = "Aurora master username."
  type        = string
  default     = "eventstore_admin"
}

variable "aurora_min_capacity" {
  description = "Minimum Aurora Serverless v2 capacity in ACUs."
  type        = number
  default     = 0.5

  validation {
    condition     = var.aurora_min_capacity >= 0.5
    error_message = "aurora_min_capacity must be at least 0.5 ACU."
  }
}

variable "aurora_max_capacity" {
  description = "Maximum Aurora Serverless v2 capacity in ACUs."
  type        = number
  default     = 2

  validation {
    condition     = var.aurora_max_capacity >= 0.5 && var.aurora_max_capacity <= 128 && var.aurora_min_capacity <= var.aurora_max_capacity
    error_message = "aurora_max_capacity must be between 0.5 and 128 ACUs and no smaller than aurora_min_capacity."
  }
}

variable "runtime_image_uri" {
  description = "Container image URI containing the control-plane Lambda runtime. No default is provided to avoid deploying an empty base image."
  type        = string
}

variable "runtime_handler" {
  description = "Lambda image command that starts the control-plane runtime."
  type        = string
  default     = "control_plane.lambda_handler.handler"
}

variable "derivative_bucket_cors_allowed_origins" {
  description = "Allowed browser origins for derivative bucket CORS."
  type        = list(string)
  default     = ["https://app.example.com"]
}

variable "lambda_subnet_ids" {
  description = "Optional Lambda VPC subnet IDs. Leave empty to fall back to default VPC subnets during bootstrap."
  type        = list(string)
  default     = []

  validation {
    condition     = length(var.lambda_subnet_ids) == 0 || length(var.lambda_subnet_ids) >= 2
    error_message = "lambda_subnet_ids must be empty or contain at least two subnet IDs."
  }
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days."
  type        = number
  default     = 30
}
