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
