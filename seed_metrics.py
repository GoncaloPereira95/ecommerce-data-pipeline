"""
Seeder for the TechMart pipeline metrics dashboard.

Pushes hours of per-minute metrics for 3 components (ingestion, processing, serving)
plus a synthetic data_quality score. No incident injection — keeps the demo clean.

Usage:
    docker compose exec jupyter python /home/jovyan/work/observability/seed_techmart_metrics.py --hours 24
    python observability/seed_techmart_metrics.py --hours 24 --url http://localhost:8086
"""
from __future__ import annotations
import argparse
import math
import random
import sys
from datetime import datetime, timedelta, timezone

try:
    from influxdb_client import InfluxDBClient, Point, WritePrecision
    from influxdb_client.client.write_api import SYNCHRONOUS
except ImportError:
    print("influxdb-client missing. Install with:  pip install influxdb-client")
    sys.exit(1)

random.seed(42)

BUCKET = "pipeline_metrics"
ORG = "bdarch"
TOKEN = "my-super-secret-admin-token"
DEFAULT_URL = "http://localhost:8086"
TEAM = "techmart"
SCENARIO = "techmart"

COMPONENTS = ["ingestion", "processing", "serving", "data_quality"]
BASE_THROUGHPUT = {"ingestion": 1500, "processing": 1400, "serving": 1300, "data_quality": 5}
BASE_LATENCY    = {"ingestion": 25,   "processing": 110,  "serving": 70,   "data_quality": 600}


def diurnal(t: datetime) -> float:
    h = t.hour + t.minute / 60
    return 0.5 + 0.7 * math.exp(-0.5 * ((h - 14.5) / 4.0) ** 2)


def generate_points(hours: int):
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start = now - timedelta(hours=hours)
    points = []
    for minute in range(hours * 60):
        ts = start + timedelta(minutes=minute)
        f = diurnal(ts)
        for c in COMPONENTS:
            jitter = random.uniform(0.9, 1.1)
            throughput = BASE_THROUGHPUT[c] * f * jitter
            latency    = BASE_LATENCY[c] * (1 + random.uniform(-0.15, 0.30))
            error_count = random.randint(1, 3) if random.random() < 0.01 else 0
            freshness   = random.uniform(5, 60)
            quality     = random.uniform(96.0, 99.5) if c == "data_quality" else random.uniform(95.0, 99.9)
            points.append(
                Point("pipeline_metrics")
                .tag("component", c)
                .tag("scenario", SCENARIO)
                .tag("team", TEAM)
                .field("throughput", round(throughput, 2))
                .field("latency_ms", round(latency, 2))
                .field("error_count", int(error_count))
                .field("data_freshness_sec", round(freshness, 2))
                .field("quality_score", round(quality, 2))
                .time(ts, WritePrecision.S)
            )
    return points


def main():
    ap = argparse.ArgumentParser(description="Seed TechMart pipeline metrics into InfluxDB")
    ap.add_argument("--hours", type=int, default=24)
    ap.add_argument("--url", default=DEFAULT_URL)
    args = ap.parse_args()

    print(f"Connecting to InfluxDB at {args.url}")
    client = InfluxDBClient(url=args.url, token=TOKEN, org=ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    points = generate_points(args.hours)
    print(f"Writing {len(points)} points...")
    for i in range(0, len(points), 1000):
        write_api.write(bucket=BUCKET, org=ORG, record=points[i:i + 1000])
    print(f"Done. Open Grafana → TechMart Pipeline dashboard.")
    client.close()


if __name__ == "__main__":
    main()
