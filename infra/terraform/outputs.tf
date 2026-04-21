output "project_name" {
  description = "Project name used for AWS resource naming."
  value       = var.project_name
}

output "environment" {
  description = "Deployment environment name."
  value       = var.environment
}

output "terraform_state_bucket_name" {
  description = "S3 bucket for Terraform remote state bootstrap."
  value       = aws_s3_bucket.terraform_state.bucket
}

output "terraform_state_lock_table_name" {
  description = "DynamoDB lock table for Terraform remote state bootstrap."
  value       = aws_dynamodb_table.terraform_lock.name
}

output "aurora_cluster_identifier" {
  description = "Aurora PostgreSQL cluster identifier."
  value       = aws_rds_cluster.aurora.cluster_identifier
}

output "aurora_endpoint" {
  description = "Aurora writer endpoint."
  value       = aws_rds_cluster.aurora.endpoint
}

output "aurora_reader_endpoint" {
  description = "Aurora reader endpoint."
  value       = aws_rds_cluster.aurora.reader_endpoint
}

output "aurora_database_name" {
  description = "Aurora PostgreSQL database name."
  value       = var.database_name
}

output "aurora_connection_secret_arn" {
  description = "Secrets Manager ARN storing the Aurora connection materials."
  value       = aws_secretsmanager_secret.aurora_connection.arn
}

output "derivative_bucket_name" {
  description = "S3 bucket used for thumbnails and previews."
  value       = aws_s3_bucket.derivative.bucket
}

output "runtime_api_url" {
  description = "Invoke URL for the control-plane HTTP API."
  value       = aws_apigatewayv2_api.control_plane.api_endpoint
}

output "runtime_lambda_name" {
  description = "Lambda function name for the control-plane runtime skeleton."
  value       = aws_lambda_function.control_plane.function_name
}
