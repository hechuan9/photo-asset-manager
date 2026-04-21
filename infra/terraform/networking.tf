data "aws_vpc" "default" {
  count   = var.vpc_id == null ? 1 : 0
  default = true
}

data "aws_subnets" "default" {
  count = var.vpc_id == null && (length(var.lambda_subnet_ids) == 0 || length(var.db_subnet_ids) == 0) ? 1 : 0

  filter {
    name   = "vpc-id"
    values = [local.resolved_vpc_id]
  }

  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

resource "aws_security_group" "control_plane_runtime" {
  name_prefix = "${local.naming_prefix}-runtime-"
  description = "Runtime security group for the control plane and future VPC-bound services."
  vpc_id      = local.resolved_vpc_id

  egress {
    description = "Allow outbound access for database connectivity and AWS APIs."
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "aurora" {
  name_prefix = "${local.naming_prefix}-aurora-"
  description = "Aurora PostgreSQL security group."
  vpc_id      = local.resolved_vpc_id

  ingress {
    description     = "Allow the control-plane runtime to reach Aurora PostgreSQL."
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.control_plane_runtime.id]
  }

  egress {
    description = "Allow Aurora to complete required outbound traffic."
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_subnet_group" "aurora" {
  name_prefix = "${local.naming_prefix}-aurora-"
  description = "Bootstrap subnets for the first Aurora PostgreSQL cluster."
  subnet_ids  = local.db_subnet_ids
}
