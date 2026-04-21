data "aws_route_tables" "default" {
  count = var.vpc_id == null && length(var.route_table_ids) == 0 ? 1 : 0

  filter {
    name   = "vpc-id"
    values = [local.resolved_vpc_id]
  }
}

resource "aws_security_group" "vpc_endpoints" {
  name_prefix = "${local.naming_prefix}-endpoints-"
  description = "Interface endpoint security group for the control plane runtime."
  vpc_id      = local.resolved_vpc_id

  ingress {
    description     = "Allow the Lambda runtime security group to reach interface endpoints on HTTPS."
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    security_groups = [aws_security_group.control_plane_runtime.id]
  }

  egress {
    description = "Allow interface endpoints to return traffic."
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_vpc_endpoint" "secretsmanager" {
  vpc_id              = local.resolved_vpc_id
  service_name        = "com.amazonaws.${var.aws_region}.secretsmanager"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = local.lambda_subnet_ids
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = local.resolved_vpc_id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = local.route_table_ids
}
