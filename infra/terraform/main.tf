provider "aws" {
  region = var.aws_region

  default_tags {
    tags = local.common_tags
  }
}

locals {
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }

  naming_prefix = "${var.project_name}-${var.environment}"

  account_scoped_suffix = data.aws_caller_identity.current.account_id

  terraform_state_bucket_name = coalesce(
    var.terraform_state_bucket_name,
    "${local.naming_prefix}-${local.account_scoped_suffix}-tfstate"
  )

  terraform_state_lock_table_name = coalesce(
    var.terraform_state_lock_table_name,
    "${local.naming_prefix}-${local.account_scoped_suffix}-tfstate-lock"
  )

  derivative_bucket_name = coalesce(
    var.derivative_bucket_name,
    "${local.naming_prefix}-${local.account_scoped_suffix}-derivatives"
  )

  aurora_cluster_identifier = coalesce(
    var.aurora_cluster_identifier,
    "${local.naming_prefix}-${local.account_scoped_suffix}-aurora"
  )

  aurora_connection_secret_name = coalesce(
    var.aurora_connection_secret_name,
    "${local.naming_prefix}/aurora-connection"
  )

  runtime_function_name = coalesce(
    var.runtime_function_name,
    "${local.naming_prefix}-${local.account_scoped_suffix}-control-plane"
  )

  api_name = coalesce(
    var.api_name,
    "${local.naming_prefix}-${local.account_scoped_suffix}-api"
  )

  lambda_log_group_name = coalesce(
    var.lambda_log_group_name,
    "/aws/lambda/${local.runtime_function_name}"
  )

  api_access_log_group_name = coalesce(
    var.api_access_log_group_name,
    "/aws/apigateway/${local.api_name}"
  )

  resolved_vpc_id   = var.vpc_id != null ? var.vpc_id : data.aws_vpc.default[0].id
  lambda_subnet_ids = length(var.lambda_subnet_ids) > 0 ? var.lambda_subnet_ids : data.aws_subnets.default[0].ids
  db_subnet_ids     = length(var.db_subnet_ids) > 0 ? var.db_subnet_ids : data.aws_subnets.default[0].ids
  route_table_ids   = length(var.route_table_ids) > 0 ? var.route_table_ids : data.aws_route_tables.default[0].ids
}

data "aws_caller_identity" "current" {}
