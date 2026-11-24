# LogLens: Cloud Log Analytics, Incident Detection and Alerting on GCP

**Services:** Cloud Storage · Dataproc · Pub/Sub (BigQuery subscription) · BigQuery · Cloud Logging · Cloud Monitoring · Terraform
Dataflow and Composer are intentionally not used (see "Design choices").

## Architecture
```
BATCH
 logs_historical.csv ─► GCS /raw ─► Dataproc (PySpark: clean, service health, hourly incident detection)
                                      └─► GCS /processed (Parquet) ─► bq load ─► BigQuery:
                                          logs_historical, service_health, service_hourly, endpoint_failures

STREAMING
 publisher.py ─┬─► Pub/Sub topic (Avro schema enforced) ─► BigQuery subscription ─► logs_stream_raw
               └─► Cloud Logging (log: loglens-app) ─► log-based metric loglens_5xx_count
                                                        └─► Alert policy ─► email

BigQuery view v_all_logs = historical UNION ALL stream ─► sql/analytics.sql
Terraform provisions: bucket, dataset, stream table, IAM, Pub/Sub schema/topic/subscription, metric, alert
```

## Dataset: 500 rows
| File | Rows | Notes |
|---|---|---|
| data/logs_historical.csv | 400 | 5 seeded incidents (60 rows), 5 duplicates, 4 missing regions, 3 negative latencies |
| data/logs_stream.csv | 100 | 20 consecutive 503s on payment-service (triggers the alert) and 5 permission-denied 403s |

Columns: log_id, event_ts, service, region, endpoint, http_status, latency_ms, severity, message

## Run it (Cloud Shell, from the project root)
1. `ALERT_EMAIL=you@gmail.com bash setup/deploy.sh`  (Terraform apply, about 3 min)
2. `bash setup/run_batch.sh`  (Dataproc, about 6 min)
3. `pip install -r requirements.txt`, then `python pubsub/publisher.py --project $(gcloud config get-value project)`
4. Check Monitoring → Alerting for the 5xx alert (2 to 3 min after the publisher finishes)
5. Run sql/analytics.sql
6. `bash setup/cleanup.sh`

## Expected results
| Check | Expected |
|---|---|
| Dataproc log: RAW / CLEAN ROWS | 400 / 392 |
| Dataproc log: INCIDENT BUCKETS | 5 (PERMISSION_DENIED, RATE_LIMITING, UPSTREAM_TIMEOUT, 5XX_BURST, LATENCY_SPIKE, 1 each) |
| loglens.logs_historical | 392 rows (25 with 5xx) |
| loglens.service_health | 6 rows |
| loglens.service_hourly | 277 rows, 5 not NORMAL |
| loglens.logs_stream_raw | 100 rows (20 with 5xx) |
| Alert | fires once, auto-closes after 30 min |

## Design choices
- **Pub/Sub BigQuery subscription instead of Dataflow:** for straight ingestion with schema enforcement, it is serverless and has no worker VMs. Dataflow is the right tool when you need transformations, windowing or enrichment.
- **Raw table + typed view:** the stream lands as-is (timestamp as string), and the view converts it. This is a common ELT pattern.
- **No Composer:** one three-step batch job does not justify an always-on orchestrator.
- **Terraform:** the whole platform is reproducible with one command, and `terraform destroy` removes it.

## Cost
Dataproc bills only while the cluster exists (the script deletes it). Pub/Sub, BigQuery and Logging usage is near zero at this size. Run `bash setup/cleanup.sh` when finished.

## Resume bullets
- Built a log analytics and incident detection platform on GCP using Terraform, with Dataproc (PySpark) batch analysis, a schema-enforced Pub/Sub to BigQuery streaming path, and Cloud Monitoring alerting.
- Wrote a Spark job that classifies service incidents (latency spikes, 5xx bursts, permission errors, timeouts, rate limiting) from hourly metrics, and loaded results into partition-friendly BigQuery tables.
- Created a log-based metric and alert policy that detects 5xx spikes within minutes, and authored a troubleshooting runbook covering 9 incident and deployment scenarios.
