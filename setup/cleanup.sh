#!/usr/bin/env bash
# Destroys everything Terraform created, plus any leftover cluster.  Run from project root.
PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
gcloud dataproc clusters delete loglens-etl --region=asia-south1 --quiet 2>/dev/null || true
( cd terraform && terraform destroy -auto-approve -input=false \
    -var="project_id=$PROJECT_ID" -var="region=asia-south1" -var="alert_email=${ALERT_EMAIL:-unused@example.com}" )
echo "Cleanup done."
