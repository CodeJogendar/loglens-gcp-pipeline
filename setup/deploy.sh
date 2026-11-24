#!/usr/bin/env bash
# One-time deploy. Usage (from project root):  ALERT_EMAIL=you@gmail.com bash setup/deploy.sh
set -euo pipefail
PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
ALERT_EMAIL=${ALERT_EMAIL:?Usage: ALERT_EMAIL=you@example.com bash setup/deploy.sh}
REGION=asia-south1
BUCKET=${PROJECT_ID}-loglens-lake
echo "Project: $PROJECT_ID | Alert email: $ALERT_EMAIL"

gcloud services enable cloudresourcemanager.googleapis.com serviceusage.googleapis.com \
  compute.googleapis.com storage.googleapis.com bigquery.googleapis.com dataproc.googleapis.com \
  pubsub.googleapis.com logging.googleapis.com monitoring.googleapis.com

# Make sure the Pub/Sub service agent exists (Terraform grants it BigQuery access)
gcloud beta services identity create --service=pubsub.googleapis.com --project="$PROJECT_ID" || true

( cd terraform
  terraform init -input=false
  terraform apply -auto-approve -input=false \
    -var="project_id=$PROJECT_ID" -var="region=$REGION" -var="alert_email=$ALERT_EMAIL" )

gcloud storage cp data/logs_historical.csv gs://$BUCKET/raw/
gcloud storage cp dataproc/analyze_logs.py gs://$BUCKET/jobs/
echo "Deploy complete."
