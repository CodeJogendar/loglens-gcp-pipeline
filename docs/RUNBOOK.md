# LogLens Troubleshooting Runbook

Format for every entry: **Symptom → Diagnosis (query/command) → Root cause → Fix → Prevention.**
Replace PROJECT_ID in queries. Incidents 1 to 5 are seeded in the dataset; entries 6 to 9 are platform issues you may hit while deploying.

## Incident 1: Latency spike on payment-service (2026-11-12, 14:00 to 15:00 UTC)
- **Symptom:** p95 latency jumps from about 400 ms to over 2,500 ms with no errors; users report slow checkout.
- **Diagnosis:**
  `SELECT * FROM loglens.service_hourly WHERE incident_type='LATENCY_SPIKE'`, then
  `SELECT message, COUNT(*) FROM loglens.v_all_logs WHERE service='payment-service' AND latency_ms>=2000 GROUP BY 1`
- **Root cause (simulated):** slow response from the card gateway (the message field says so).
- **Fix:** fail over to the secondary gateway or lower client timeouts and retry.
- **Prevention:** alert on p95 latency, not only on errors.

## Incident 2: 5xx burst on auth-service (2026-11-13, 02:10 UTC, us-central1)
- **Symptom:** 500/503 responses; logins failing.
- **Diagnosis:** incident_type='5XX_BURST'. Then group by region: all errors are in `us-central1`.
- **Root cause (simulated):** database connection pool exhausted in one region.
- **Fix:** raise pool size or restart the pool; shift traffic to a healthy region.
- **Prevention:** the log-based metric `loglens_5xx_count` plus the alert policy pages on-call within minutes.

## Incident 3: PERMISSION_DENIED on storage-service (2026-11-14, 09:00 UTC)
- **Symptom:** a burst of 403s, with the message "caller lacks storage.objects.get".
- **Diagnosis:** incident_type='PERMISSION_DENIED'; check the message query; in Cloud Shell, `gcloud projects get-iam-policy PROJECT_ID` and look for the service account's roles.
- **Root cause (simulated):** an IAM role was removed or never granted to the service account.
- **Fix:** grant the least-privilege role (for example `roles/storage.objectViewer`) to that service account only.
- **Prevention:** manage IAM in Terraform so changes are reviewed; avoid manual console edits.

## Incident 4: Upstream timeouts on order-service (2026-11-15, 18:00 UTC)
- **Symptom:** 504 responses with latency near 30 s.
- **Diagnosis:** incident_type='UPSTREAM_TIMEOUT'; the message names `inventory-db`.
- **Root cause (simulated):** a downstream database did not answer within the timeout.
- **Fix:** restore the dependency; add a circuit breaker and a shorter client timeout.
- **Prevention:** monitor dependency health separately; set timeouts lower than the caller's.

## Incident 5: Rate limiting on search-service (2026-11-11, 20:00 UTC)
- **Symptom:** 429 responses, very low latency (the limiter rejects instantly).
- **Diagnosis:** incident_type='RATE_LIMITING'; find the offending client in a real system with a per-client breakdown.
- **Root cause (simulated):** one client exceeded its quota.
- **Fix:** contact the client, raise the quota if legitimate, or block abuse.
- **Prevention:** per-client quotas and a 429 dashboard.

## Platform issues (deployment)
### 6. Terraform: "Service account service-NNN@gcp-sa-pubsub... does not exist"
- **Cause:** the Pub/Sub service agent has not been created yet.
- **Fix:** `gcloud beta services identity create --service=pubsub.googleapis.com`, then re-run `bash setup/deploy.sh`.

### 7. Terraform: subscription creation fails with a BigQuery permission error
- **Cause:** IAM binding propagation delay (the roles were granted seconds earlier).
- **Fix:** wait 1 to 2 minutes and re-run `bash setup/deploy.sh` (Terraform is idempotent).

### 8. Dataproc: cluster creation fails (quota, or "startup failed")
- **Diagnosis:** `gcloud dataproc clusters list --region=asia-south1` for leftovers; check CPU quota with `gcloud compute regions describe asia-south1`.
- **Fix:** delete leftovers; make sure private Google access is on (run_batch.sh does this); retry.

### 9. Alert did not fire / no email
- **Diagnosis:** Monitoring → Alerting shows the incident even if email is delayed; Logs Explorer with `logName:"loglens-app" AND jsonPayload.http_status>=500` confirms logs arrived; Metrics Explorer, search `loglens_5xx_count`, confirms the metric has data.
- **Cause candidates:** publisher not run; metric needs 2 to 3 minutes; email channel not verified (check the Spam folder).

## Your own incidents
Add every real error you hit (Dataflow worker startup failure, Composer reserved variable, Broken DAG KeyError, wrong folder path) as entries in the same format. Real incidents are the strongest interview material.
