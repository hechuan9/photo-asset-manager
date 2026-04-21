data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }

  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

resource "aws_security_group" "control_plane_runtime" {
  name_prefix = "${local.naming_prefix}-runtime-"
  description = "Runtime security group for the control plane and future VPC-bound services."
  vpc_id      = data.aws_vpc.default.id

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
  vpc_id      = data.aws_vpc.default.id

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
  description = "Default VPC subnets for the first Aurora PostgreSQL cluster."
  subnet_ids  = data.aws_subnets.default.ids
}
