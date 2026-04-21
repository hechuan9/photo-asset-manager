terraform {
  backend "s3" {
    bucket         = "photo-asset-manager-dev-665303623943-tfstate"
    key            = "infra/terraform/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "photo-asset-manager-dev-665303623943-tfstate-lock"
    encrypt        = true
  }
}
