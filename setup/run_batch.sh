#!/usr/bin/env bash
# Batch: GCS -> Dataproc (PySpark) -> Parquet -> BigQuery.  Run from project root.
set -euo pipefail
PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
REGION=asia-south1
BUCKET=${PROJECT_ID}-loglens-lake
CLUSTER=loglens-etl

gcloud compute networks subnets update default --region=$REGION --enable-private-ip-google-access
trap 'echo "Deleting cluster..."; gcloud dataproc clusters delete $CLUSTER --region=$REGION --quiet || true' EXIT

gcloud dataproc clusters create $CLUSTER --region=$REGION --single-node \
  --master-machine-type=n2-standard-4 --master-boot-disk-size=50GB --image-version=2.2-debian12

gcloud dataproc jobs submit pyspark gs://$BUCKET/jobs/analyze_logs.py \
  --cluster=$CLUSTER --region=$REGION -- $BUCKET

for T in logs_clean:logs_historical service_health:service_health \
         service_hourly:service_hourly endpoint_failures:endpoint_failures; do
  SRC=${T%%:*}; DST=${T##*:}
  bq load --location=$REGION --replace --source_format=PARQUET \
    loglens.$DST "gs://$BUCKET/processed/$SRC/*.parquet"
done
echo "Batch complete."
