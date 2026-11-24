output "bucket" {
  value = google_storage_bucket.lake.name
}

output "topic" {
  value = google_pubsub_topic.logs.name
}

output "subscription" {
  value = google_pubsub_subscription.to_bigquery.name
}

output "stream_table" {
  value = "${var.project_id}.${google_bigquery_dataset.loglens.dataset_id}.${google_bigquery_table.logs_stream_raw.table_id}"
}
