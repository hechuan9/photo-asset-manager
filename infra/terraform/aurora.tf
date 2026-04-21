resource "random_password" "aurora_master_password" {
  length           = 32
  special          = true
  override_special = "_%@"
}

resource "aws_rds_cluster" "aurora" {
  cluster_identifier              = local.aurora_cluster_identifier
  engine                          = "aurora-postgresql"
  database_name                   = var.database_name
  master_username                 = var.db_master_username
  master_password                 = random_password.aurora_master_password.result
  db_subnet_group_name            = aws_db_subnet_group.aurora.name
  vpc_security_group_ids          = [aws_security_group.aurora.id]
  storage_encrypted               = true
  deletion_protection             = true
  backup_retention_period         = 7
  preferred_backup_window         = "07:00-09:00"
  preferred_maintenance_window    = "sun:09:00-sun:10:00"
  final_snapshot_identifier       = "${local.aurora_cluster_identifier}-final"
  skip_final_snapshot             = false
  copy_tags_to_snapshot           = true
  enabled_cloudwatch_logs_exports = ["postgresql"]

  serverlessv2_scaling_configuration {
    min_capacity = var.aurora_min_capacity
    max_capacity = var.aurora_max_capacity
  }
}

resource "aws_rds_cluster_instance" "aurora_writer" {
  identifier_prefix   = "${local.aurora_cluster_identifier}-writer-"
  cluster_identifier  = aws_rds_cluster.aurora.id
  instance_class      = "db.serverless"
  engine              = aws_rds_cluster.aurora.engine
  publicly_accessible = false
}

resource "aws_secretsmanager_secret" "aurora_connection" {
  name                    = local.aurora_connection_secret_name
  recovery_window_in_days = 7
  description             = "Aurora PostgreSQL connection materials for the control-plane runtime."
}

resource "aws_secretsmanager_secret_version" "aurora_connection" {
  secret_id = aws_secretsmanager_secret.aurora_connection.id

  secret_string = jsonencode({
    username           = var.db_master_username
    password           = random_password.aurora_master_password.result
    engine             = "postgresql"
    host               = aws_rds_cluster.aurora.endpoint
    reader_host        = aws_rds_cluster.aurora.reader_endpoint
    port               = 5432
    database           = var.database_name
    cluster_arn        = aws_rds_cluster.aurora.arn
    cluster_identifier = aws_rds_cluster.aurora.cluster_identifier
  })
}
