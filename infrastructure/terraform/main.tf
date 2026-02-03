terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  access_key                  = var.minio_access_key
  secret_key                  = var.minio_secret_key
  region                      = "us-east-1"

  endpoints {
    s3 = var.minio_endpoint
  }

  # Requerido para MinIO
  s3_use_path_style           = true
  skip_credentials_validation = true
  skip_requesting_account_id  = true
  skip_metadata_api_check     = true
}

resource "aws_s3_bucket" "datalake" {
  bucket = var.bucket_name
}

resource "aws_s3_object" "raw" {
  bucket = aws_s3_bucket.datalake.id
  key    = "raw/"
}

resource "aws_s3_object" "processed" {
  bucket = aws_s3_bucket.datalake.id
  key    = "processed/"
}

resource "aws_s3_object" "output" {
  bucket = aws_s3_bucket.datalake.id
  key    = "output/"
}
