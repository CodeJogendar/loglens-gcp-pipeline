-- Replace PROJECT_ID first. Run after batch AND streaming have finished.

-- 1. Unified view: batch + streaming logs
CREATE OR REPLACE VIEW `PROJECT_ID.loglens.v_all_logs` AS
SELECT log_id, event_ts, service, region, endpoint, http_status, latency_ms, severity, message,
       'batch' AS source FROM `PROJECT_ID.loglens.logs_historical`
UNION ALL
SELECT log_id, TIMESTAMP(event_ts), service, region, endpoint, http_status, latency_ms, severity, message,
       'stream' AS source FROM `PROJECT_ID.loglens.logs_stream_raw`;

-- 2. Service health league table (from Dataproc)
SELECT service, requests, errors_5xx, error_rate_5xx, avg_latency_ms, p95_latency_ms, max_latency_ms
FROM `PROJECT_ID.loglens.service_health` ORDER BY error_rate_5xx DESC;

-- 3. Detected incidents (expect 5, one per type)
SELECT hour_start, service, incident_type, requests, errors_5xx, forbidden_403,
       rate_limited_429, timeouts_504, slow_requests, p95_latency_ms
FROM `PROJECT_ID.loglens.service_hourly` WHERE incident_type != 'NORMAL' ORDER BY hour_start;

-- 4. Top failing endpoints
SELECT service, endpoint, http_status, failures
FROM `PROJECT_ID.loglens.endpoint_failures` ORDER BY failures DESC LIMIT 10;

-- 5. Live 5xx per minute per service (the burst that triggers the alert)
SELECT TIMESTAMP_TRUNC(event_ts, MINUTE) AS minute, service, COUNTIF(http_status >= 500) AS errors_5xx
FROM `PROJECT_ID.loglens.v_all_logs` WHERE source = 'stream'
GROUP BY minute, service HAVING errors_5xx > 0 ORDER BY minute;

-- 6. Availability vs a 99.5% SLO
SELECT service, COUNT(*) AS requests,
       ROUND(100 * (1 - COUNTIF(http_status >= 500) / COUNT(*)), 2) AS availability_pct,
       IF(100 * (1 - COUNTIF(http_status >= 500) / COUNT(*)) >= 99.5, 'MEETS SLO', 'BREACHES SLO') AS slo_99_5
FROM `PROJECT_ID.loglens.v_all_logs` GROUP BY service ORDER BY availability_pct;

-- 7. Latency percentiles by region
SELECT region, APPROX_QUANTILES(latency_ms, 100)[OFFSET(50)] AS p50_ms,
       APPROX_QUANTILES(latency_ms, 100)[OFFSET(95)] AS p95_ms
FROM `PROJECT_ID.loglens.v_all_logs` GROUP BY region ORDER BY p95_ms DESC;

-- 8. Error messages ranked (find root-cause strings fast)
SELECT message, service, COUNT(*) AS occurrences
FROM `PROJECT_ID.loglens.v_all_logs` WHERE http_status >= 400
GROUP BY message, service ORDER BY occurrences DESC LIMIT 10;
