variable "minio_endpoint" {
  description = "Endpoint de MinIO"
  type        = string
}

variable "minio_access_key" {
  description = "Access key de MinIO"
  type        = string
  sensitive   = true
}

variable "minio_secret_key" {
  description = "Secret key de MinIO"
  type        = string
  sensitive   = true
}

variable "bucket_name" {
  description = "Nombre del bucket del data lake"
  type        = string
}

variable "environment" {
  description = "Ambiente de deployment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "enable_versioning" {
  description = "Habilitar versionado de objetos en S3"
  type        = bool
  default     = false
}
