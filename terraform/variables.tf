variable "DL_BUCKET" {
  description = "GCS Bucket name"
  type = string
}

variable "PROJECT" {
  description = "NOAA Climate"
}

variable "GCP_REGION" {
  description = "GCP Region"
  type = string
}

variable "STORAGE_CLASS" {
  description = "Storage class type"
  default = "STANDARD"
}

variable "BIGQUERY_DATASET" {
  description = "BigQuery Dataset for GCS raw data"
  type = string
}

variable "BIGQUERY_DATASET_DBT_DEV" {
  description = "BigQuery Dataset for dbt development"
  type = string
}

variable "BIGQUERY_DATASET_DBT_PROD" {
  description = "BigQuery Dataset for dbt production"
  type = string
  default = "production"
}