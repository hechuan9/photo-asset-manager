resource "aws_cloudwatch_log_group" "lambda" {
  name              = local.lambda_log_group_name
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "api_access" {
  name              = local.api_access_log_group_name
  retention_in_days = var.log_retention_days
}

resource "aws_iam_role" "runtime" {
  name_prefix = "${local.naming_prefix}-runtime-"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "runtime_basic_execution" {
  role       = aws_iam_role.runtime.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "runtime_vpc_access" {
  role       = aws_iam_role.runtime.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy" "runtime_storage_access" {
  name = "${local.naming_prefix}-runtime-storage-access"
  role = aws_iam_role.runtime.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadAuroraConnectionSecret"
        Effect = "Allow"
        Action = [
          "secretsmanager:DescribeSecret",
          "secretsmanager:GetSecretValue"
        ]
        Resource = aws_secretsmanager_secret.aurora_connection.arn
      },
      {
        Sid    = "UseDerivativeBucket"
        Effect = "Allow"
        Action = [
          "s3:AbortMultipartUpload",
          "s3:GetObject",
          "s3:GetObjectTagging",
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads",
          "s3:PutObject",
          "s3:PutObjectTagging"
        ]
        Resource = [
          aws_s3_bucket.derivative.arn,
          "${aws_s3_bucket.derivative.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_lambda_function" "control_plane" {
  function_name = local.runtime_function_name
  role          = aws_iam_role.runtime.arn
  package_type  = "Image"
  image_uri     = var.runtime_image_uri
  architectures = ["arm64"]
  timeout       = 30
  memory_size   = 512

  image_config {
    command = [var.runtime_handler]
  }

  environment {
    variables = {
      CONTROL_PLANE_AUTO_CREATE_SCHEMA = "1"
      DATABASE_CONNECTION_SECRET_ARN   = aws_secretsmanager_secret.aurora_connection.arn
      DATABASE_NAME                    = var.database_name
      DERIVATIVE_BUCKET_NAME           = aws_s3_bucket.derivative.bucket
      LOG_LEVEL                        = "INFO"
      PROJECT_NAME                     = var.project_name
      SERVICE_ENVIRONMENT              = var.environment
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.runtime_basic_execution,
    aws_iam_role_policy_attachment.runtime_vpc_access,
    aws_iam_role_policy.runtime_storage_access,
    aws_cloudwatch_log_group.lambda
  ]

  vpc_config {
    security_group_ids = [aws_security_group.control_plane_runtime.id]
    subnet_ids         = local.lambda_subnet_ids
  }
}

resource "aws_apigatewayv2_api" "control_plane" {
  name          = local.api_name
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "control_plane" {
  api_id                 = aws_apigatewayv2_api.control_plane.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.control_plane.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "default" {
  api_id             = aws_apigatewayv2_api.control_plane.id
  route_key          = "$default"
  target             = "integrations/${aws_apigatewayv2_integration.control_plane.id}"
  authorization_type = "AWS_IAM"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.control_plane.id
  name        = "$default"
  auto_deploy = true

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api_access.arn
    format = jsonencode({
      requestId      = "$context.requestId"
      routeKey       = "$context.routeKey"
      status         = "$context.status"
      responseLength = "$context.responseLength"
      errorMessage   = "$context.error.message"
    })
  }
}

resource "aws_lambda_permission" "allow_apigw" {
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.control_plane.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.control_plane.execution_arn}/*/*"
}
