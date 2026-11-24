"""LogLens dataset: 400 historical API logs (5 seeded incidents + dirty rows) + 100 streaming logs = 500 rows."""
import csv, random
from collections import Counter, defaultdict
from datetime import datetime, timedelta

random.seed(11)
SERVICES = {
    "auth-service": ["/login", "/token/refresh", "/logout"],
    "payment-service": ["/pay", "/refund", "/payment/status"],
    "order-service": ["/orders", "/orders/{id}", "/orders/cancel"],
    "search-service": ["/search", "/suggest"],
    "storage-service": ["/files/upload", "/files/download", "/files/list"],
    "notification-service": ["/notify/email", "/notify/sms"],
}
REGIONS = ["asia-south1", "us-central1", "europe-west1"]
MSG = {200: "request completed", 201: "resource created", 400: "invalid request payload",
       404: "resource not found", 500: "internal server error"}
HEADER = ["log_id", "event_ts", "service", "region", "endpoint", "http_status", "latency_ms", "severity", "message"]
FMT = "%Y-%m-%d %H:%M:%S"

def sev(s): return "INFO" if s < 400 else ("WARNING" if s < 500 else "ERROR")

def mk(ts, service, status, latency, message=None, region=None, incident=""):
    return {"event_ts": ts.strftime(FMT), "service": service, "region": region or random.choice(REGIONS),
            "endpoint": random.choice(SERVICES[service]), "http_status": status, "latency_ms": latency,
            "severity": sev(status), "message": message or MSG.get(status, "error"), "incident": incident}

def normal(ts, statuses, weights):
    st = random.choices(statuses, weights)[0]
    lat = random.randint(200, 1200) if st >= 500 else random.randint(40, 450)
    return mk(ts, random.choice(list(SERVICES)), st, lat)

def burst(start, minutes, n, service, statuses, lat, msg, tag, region=None):
    return [mk(start + timedelta(seconds=random.randint(0, minutes * 60)), service,
               random.choice(statuses), random.randint(*lat), msg, region, tag) for _ in range(n)]

# ---------- historical: 335 normal + 60 incident rows (+5 duplicates = 400) ----------
t0 = datetime(2026, 11, 10)
rows = [normal(t0 + timedelta(seconds=random.randint(0, 6 * 86400)),
               [200, 201, 400, 404, 500], [88, 5, 3, 3, 1]) for _ in range(335)]
rows += burst(datetime(2026, 11, 12, 14, 0), 55, 15, "payment-service", [200, 201], (2500, 6000),
              "slow response from card gateway", "LATENCY_SPIKE")
rows += burst(datetime(2026, 11, 13, 2, 10), 15, 15, "auth-service", [500, 503], (300, 1500),
              "database connection pool exhausted", "5XX_BURST", "us-central1")
rows += burst(datetime(2026, 11, 14, 9, 0), 40, 10, "storage-service", [403], (20, 120),
              "PERMISSION_DENIED: caller lacks storage.objects.get", "PERMISSION_DENIED")
rows += burst(datetime(2026, 11, 15, 18, 0), 20, 10, "order-service", [504], (29000, 31000),
              "upstream timeout calling inventory-db", "UPSTREAM_TIMEOUT")
rows += burst(datetime(2026, 11, 11, 20, 0), 30, 10, "search-service", [429], (5, 40),
              "rate limit exceeded for client", "RATE_LIMITING")
rows.sort(key=lambda r: r["event_ts"])
for i, r in enumerate(rows, 1):
    r["log_id"] = f"L{i:05d}"

normal_idx = [i for i, r in enumerate(rows) if not r["incident"]]
pick = random.sample(normal_idx, 12)
dup_idx, null_idx, bad_idx = pick[:5], pick[5:9], pick[9:12]
hist = [dict(r) for r in rows]
for i in null_idx: hist[i]["region"] = ""            # 4 missing regions
for i in bad_idx:  hist[i]["latency_ms"] = -1        # 3 invalid latencies
for i in dup_idx:  hist.append(dict(hist[i]))        # 5 exact duplicates
assert len(hist) == 400

# ---------- streaming: 100 rows (live burst of 20 x 503 on payment-service) ----------
stream = []
for i in range(100):
    if 60 <= i < 80:
        r = mk(datetime(2026, 11, 23), "payment-service", 503, random.randint(800, 2500),
               "upstream connect error or disconnect/reset before headers", "us-central1", "5XX_BURST")
    elif 30 <= i < 35:
        r = mk(datetime(2026, 11, 23), "storage-service", 403, random.randint(20, 120),
               "PERMISSION_DENIED: caller lacks storage.objects.get", None, "PERMISSION_DENIED")
    else:
        r = normal(datetime(2026, 11, 23), [200, 201, 400, 404], [90, 5, 3, 2])
    r["log_id"] = f"S{i+1:05d}"
    stream.append(r)

for name, data in (("logs_historical.csv", hist), ("logs_stream.csv", stream)):
    with open(f"data/{name}", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=HEADER, extrasaction="ignore")
        w.writeheader(); w.writerows(data)

# ---------- expected results (mirrors the Spark job rules exactly) ----------
seen, clean = set(), []
for r in hist:
    if r["log_id"] in seen or r["latency_ms"] < 0: continue
    seen.add(r["log_id"]); clean.append(r)
buckets = defaultdict(lambda: Counter())
for r in clean:
    k = (r["service"], r["event_ts"][:13]); b = buckets[k]; s = r["http_status"]
    b["n"] += 1; b["e5"] += s >= 500; b["403"] += s == 403; b["429"] += s == 429
    b["504"] += s == 504; b["slow"] += r["latency_ms"] >= 2000
def classify(b):
    if b["403"] >= 3: return "PERMISSION_DENIED"
    if b["429"] >= 3: return "RATE_LIMITING"
    if b["504"] >= 3: return "UPSTREAM_TIMEOUT"
    if b["e5"] >= 3: return "5XX_BURST"
    if b["slow"] >= 3: return "LATENCY_SPIKE"
    return "NORMAL"
types = Counter(classify(b) for b in buckets.values())
print("historical rows:", len(hist), "| duplicates:", len(dup_idx), "| null region:", len(null_idx), "| latency<0:", len(bad_idx))
print("expected clean rows:", len(clean), "| 5xx in clean:", sum(r["http_status"] >= 500 for r in clean))
print("expected service_hourly buckets:", len(buckets), "| incident buckets:", {k: v for k, v in types.items() if k != "NORMAL"})
print("stream rows:", len(stream), "| 5xx in stream:", sum(r["http_status"] >= 500 for r in stream),
      "| 403 in stream:", sum(r["http_status"] == 403 for r in stream))
print("TOTAL rows:", len(hist) + len(stream))
