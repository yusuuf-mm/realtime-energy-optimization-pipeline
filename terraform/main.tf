terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
  profile = "default"
}

# ── S3 Data Lake Bucket ──────────────────────────────────
resource "aws_s3_bucket" "energy_data_lake" {
  bucket = var.bucket_name

  tags = {
    Project     = "realtime-energy-pipeline"
    Environment = "dev"
  }
}

resource "aws_s3_bucket_versioning" "energy_data_lake" {
  bucket = aws_s3_bucket.energy_data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "energy_data_lake" {
  bucket = aws_s3_bucket.energy_data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
