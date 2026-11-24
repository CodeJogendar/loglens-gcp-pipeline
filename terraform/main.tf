terraform {
  required_version = ">= 1.3"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

data "google_project" "this" {
  project_id = var.project_id
}

locals {
  bucket_name = "${var.project_id}-loglens-lake"
  compute_sa  = "serviceAccount:${data.google_project.this.number}-compute@developer.gserviceaccount.com"
  pubsub_sa   = "serviceAccount:service-${data.google_project.this.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# ---------------- Storage ----------------
resource "google_storage_bucket" "lake" {
  name                        = local.bucket_name
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true
}

# ---------------- BigQuery ----------------
resource "google_bigquery_dataset" "loglens" {
  dataset_id                 = "loglens"
  location                   = var.region
  delete_contents_on_destroy = true
}

# Raw landing table written directly by the Pub/Sub BigQuery subscription
resource "google_bigquery_table" "logs_stream_raw" {
  dataset_id          = google_bigquery_dataset.loglens.dataset_id
  table_id            = "logs_stream_raw"
  deletion_protection = false
  schema = jsonencode([
    { name = "log_id", type = "STRING", mode = "NULLABLE" },
    { name = "event_ts", type = "STRING", mode = "NULLABLE" },
    { name = "service", type = "STRING", mode = "NULLABLE" },
    { name = "region", type = "STRING", mode = "NULLABLE" },
    { name = "endpoint", type = "STRING", mode = "NULLABLE" },
    { name = "http_status", type = "INTEGER", mode = "NULLABLE" },
    { name = "latency_ms", type = "INTEGER", mode = "NULLABLE" },
    { name = "severity", type = "STRING", mode = "NULLABLE" },
    { name = "message", type = "STRING", mode = "NULLABLE" }
  ])
}

# ---------------- IAM (least privilege for what each service needs) ----------------
resource "google_project_iam_member" "compute_roles" {
  for_each = toset([
    "roles/dataproc.worker",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectAdmin",
  ])
  project = var.project_id
  role    = each.value
  member  = local.compute_sa
}

# Pub/Sub service agent must be able to write into BigQuery
resource "google_project_iam_member" "pubsub_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = local.pubsub_sa
}

resource "google_project_iam_member" "pubsub_bq_viewer" {
  project = var.project_id
  role    = "roles/bigquery.metadataViewer"
  member  = local.pubsub_sa
}

# ---------------- Pub/Sub: schema-enforced topic -> BigQuery subscription ----------------
resource "google_pubsub_schema" "log_event" {
  name = "loglens-log-event"
  type = "AVRO"
  definition = jsonencode({
    type = "record"
    name = "LogEvent"
    fields = [
      { name = "log_id", type = "string" },
      { name = "event_ts", type = "string" },
      { name = "service", type = "string" },
      { name = "region", type = "string" },
      { name = "endpoint", type = "string" },
      { name = "http_status", type = "int" },
      { name = "latency_ms", type = "int" },
      { name = "severity", type = "string" },
      { name = "message", type = "string" }
    ]
  })
}

resource "google_pubsub_topic" "logs" {
  name = "loglens-topic"
  schema_settings {
    schema   = google_pubsub_schema.log_event.id
    encoding = "JSON"
  }
}

resource "google_pubsub_subscription" "to_bigquery" {
  name  = "loglens-bq-sub"
  topic = google_pubsub_topic.logs.id

  bigquery_config {
    table            = "${var.project_id}.${google_bigquery_dataset.loglens.dataset_id}.${google_bigquery_table.logs_stream_raw.table_id}"
    use_topic_schema = true
  }

  depends_on = [
    google_project_iam_member.pubsub_bq_editor,
    google_project_iam_member.pubsub_bq_viewer,
  ]
}

# ---------------- Monitoring: log-based metric + alert ----------------
resource "google_logging_metric" "errors_5xx" {
  name   = "loglens_5xx_count"
  filter = "logName=\"projects/${var.project_id}/logs/loglens-app\" AND jsonPayload.http_status>=500"

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
    labels {
      key         = "service"
      value_type  = "STRING"
      description = "Service that produced the 5xx"
    }
  }

  label_extractors = {
    "service" = "EXTRACT(jsonPayload.service)"
  }
}

resource "google_monitoring_notification_channel" "email" {
  display_name = "LogLens on-call email"
  type         = "email"
  labels = {
    email_address = var.alert_email
  }
}

resource "google_monitoring_alert_policy" "high_5xx" {
  display_name = "LogLens: 5xx errors above 5 per minute"
  combiner     = "OR"

  conditions {
    display_name = "5xx count > 5 in 1 minute"
    condition_threshold {
      filter          = "metric.type=\"logging.googleapis.com/user/${google_logging_metric.errors_5xx.name}\" AND resource.type=\"global\""
      comparison      = "COMPARISON_GT"
      threshold_value = 5
      duration        = "0s"
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_SUM"
      }
      trigger {
        count = 1
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.email.id]

  documentation {
    content   = "5xx spike detected. See docs/RUNBOOK.md, incident 2 (5xx burst), and run the per-minute 5xx query in sql/analytics.sql."
    mime_type = "text/markdown"
  }

  alert_strategy {
    auto_close = "1800s"
  }
}
