terraform {
  required_version = ">= 1.0"
  backend "local" {}  # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}



provider "google" {
  project = var.PROJECT
  region = var.GCP_REGION
}


resource "google_storage_bucket" "data-lake-bucket" {
  name          = var.DL_BUCKET # Concatenating DL bucket & Project name for unique naming
  location      = var.GCP_REGION

  storage_class = var.STORAGE_CLASS
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  force_destroy = true
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.BIGQUERY_DATASET
  project    = var.PROJECT
  location   = var.GCP_REGION
}

resource "google_bigquery_dataset" "dataset_dbt" {
  dataset_id = var.BIGQUERY_DATASET_DBT_DEV
  project    = var.PROJECT
  location   = var.GCP_REGION
}

resource "google_bigquery_dataset" "dataset_dbt_prod" {
  dataset_id = var.BIGQUERY_DATASET_DBT_PROD
  project    = var.PROJECT
  location   = var.GCP_REGION
}

