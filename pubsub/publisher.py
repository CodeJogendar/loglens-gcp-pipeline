"""Replays logs_stream.csv as live traffic.
Each event goes to (1) Pub/Sub -> BigQuery subscription and (2) Cloud Logging (drives the 5xx alert).
Usage: python pubsub/publisher.py --project PROJECT_ID --delay 0.3"""
import argparse, csv, json, time
from datetime import datetime, timezone
from google.cloud import logging as cloud_logging
from google.cloud import pubsub_v1
from google.cloud.logging_v2.resource import Resource

p = argparse.ArgumentParser()
p.add_argument("--project", required=True)
p.add_argument("--topic", default="loglens-topic")
p.add_argument("--file", default="data/logs_stream.csv")
p.add_argument("--delay", type=float, default=0.3)
a = p.parse_args()

pub = pubsub_v1.PublisherClient()
topic = pub.topic_path(a.project, a.topic)
logger = cloud_logging.Client(project=a.project).logger("loglens-app")
resource = Resource(type="global", labels={"project_id": a.project})

with open(a.file) as f:
    for i, row in enumerate(csv.DictReader(f), 1):
        msg = {
            "log_id": row["log_id"],
            "event_ts": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),   # live event time
            "service": row["service"], "region": row["region"], "endpoint": row["endpoint"],
            "http_status": int(row["http_status"]), "latency_ms": int(row["latency_ms"]),
            "severity": row["severity"], "message": row["message"],
        }
        mid = pub.publish(topic, json.dumps(msg).encode("utf-8")).result()
        logger.log_struct(msg, severity=msg["severity"], resource=resource)
        print(f"[{i}/100] {msg['log_id']} {msg['service']} {msg['http_status']} -> {mid}")
        time.sleep(a.delay)
print("Done. Metric + alert can take 2-3 minutes to react.")
