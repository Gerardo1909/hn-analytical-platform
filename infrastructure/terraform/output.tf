output "bucket_name" {
  description = "Nombre del bucket del data lake"
  value       = aws_s3_bucket.datalake.bucket
}

output "bucket_arn" {
  description = "ARN del bucket"
  value       = aws_s3_bucket.datalake.arn
}
