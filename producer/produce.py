"""
Synthetic e-commerce event producer for the Kafka → Spark → Hudi pipeline.

We emit `order_event` records with a stable `order_id` so that Hudi upserts/merges
can be exercised end-to-end. Roughly 30% of events are status updates for previously
seen orders (UPDATE), the rest are new orders (INSERT). A small fraction are
soft-deletes (status=CANCELLED).

Schema (JSON):
{
  "order_id":     "ORD-0000123",            # primary key (recordKey)
  "user_id":      "U-0042",
  "country":      "IN",                     # partition column
  "category":     "electronics",
  "amount":       249.99,
  "currency":     "USD",
  "status":       "PLACED|PAID|SHIPPED|DELIVERED|CANCELLED",
  "payment_method": "card|upi|wallet|cod",
  "items":        3,
  "event_ts_ms":  1700000000000,             # precombine field (latest wins)
  "ingest_ts_ms": 1700000000050,
  "_op":          "u|i|d"                    # change type hint (advisory)
}
"""
from __future__ import annotations

import os
import random
import signal
import sys
import time
from typing import Dict, List

import orjson
from confluent_kafka import Producer
from faker import Faker

BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC       = os.getenv("KAFKA_TOPIC", "events")
EPS         = int(os.getenv("EVENTS_PER_SECOND", "200"))
TOTAL       = int(os.getenv("TOTAL_EVENTS", "20000"))
SEED        = int(os.getenv("SEED", "42"))

random.seed(SEED)
Faker.seed(SEED)
fake = Faker()

COUNTRIES = ["IN", "US", "GB", "DE", "FR", "JP", "BR", "AU"]
CATEGORIES = ["electronics", "fashion", "grocery", "books", "home", "toys", "beauty", "sports"]
STATUSES = ["PLACED", "PAID", "SHIPPED", "DELIVERED"]
CURRENCIES = ["USD", "EUR", "INR", "GBP", "JPY"]
PAYMENTS = ["card", "upi", "wallet", "cod"]


def make_new_order(order_id: str) -> Dict:
    now_ms = int(time.time() * 1000)
    return {
        "order_id": order_id,
        "user_id": f"U-{random.randint(1, 5000):05d}",
        "country": random.choice(COUNTRIES),
        "category": random.choice(CATEGORIES),
        "amount": round(random.uniform(5, 1500), 2),
        "currency": random.choice(CURRENCIES),
        "status": "PLACED",
        "payment_method": random.choice(PAYMENTS),
        "items": random.randint(1, 8),
        "event_ts_ms": now_ms,
        "ingest_ts_ms": now_ms,
        "_op": "i",
    }


def update_existing(order: Dict) -> Dict:
    now_ms = int(time.time() * 1000)
    # advance the lifecycle most of the time, occasionally cancel
    if random.random() < 0.05:
        new_status = "CANCELLED"
        op = "d"
    else:
        idx = STATUSES.index(order.get("status", "PLACED")) if order.get("status") in STATUSES else 0
        new_status = STATUSES[min(idx + 1, len(STATUSES) - 1)]
        op = "u"
    return {**order, "status": new_status, "event_ts_ms": now_ms, "ingest_ts_ms": now_ms, "_op": op}


def delivery_callback(err, msg):
    if err is not None:
        print(f"[producer] delivery failed: {err}", file=sys.stderr)


def main() -> int:
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "synthetic-orders",
        "linger.ms": 10,
        "batch.size": 65536,
        "compression.type": "lz4",
        "acks": "all",
        "enable.idempotence": True,
    })

    print(f"[producer] target={BOOTSTRAP} topic={TOPIC} eps={EPS} total={TOTAL}")

    # graceful shutdown
    stop = {"flag": False}
    def _stop(signum, frame):
        stop["flag"] = True
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    sent = 0
    period = 1.0 / max(EPS, 1)
    seen: List[Dict] = []
    next_id = 1

    start = time.time()
    last_log = start

    while sent < TOTAL and not stop["flag"]:
        loop_start = time.time()
        batch = max(1, EPS // 50)  # send in small bursts, keeps rate accurate
        for _ in range(batch):
            if sent >= TOTAL:
                break
            do_update = seen and random.random() < 0.30
            if do_update:
                victim = random.choice(seen)
                event = update_existing(victim)
                # update in-memory copy so subsequent updates progress the lifecycle
                victim.update(event)
            else:
                oid = f"ORD-{next_id:07d}"
                next_id += 1
                event = make_new_order(oid)
                seen.append(event)
                # cap memory of "active" orders to keep update rate bounded
                if len(seen) > 5000:
                    seen.pop(0)

            payload = orjson.dumps(event)
            # key = order_id so all updates for the same order land on the same partition
            producer.produce(
                TOPIC,
                key=event["order_id"].encode(),
                value=payload,
                callback=delivery_callback,
            )
            sent += 1

        producer.poll(0)

        # pace
        elapsed = time.time() - loop_start
        sleep_for = period * batch - elapsed
        if sleep_for > 0:
            time.sleep(sleep_for)

        now = time.time()
        if now - last_log >= 2.0:
            rate = sent / (now - start)
            print(f"[producer] sent={sent}/{TOTAL} rate={rate:0.0f}/s tracked_orders={len(seen)}")
            last_log = now

    producer.flush(15)
    elapsed = time.time() - start
    print(f"[producer] done sent={sent} elapsed={elapsed:0.1f}s avg_rate={sent / max(elapsed, 0.001):0.0f}/s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
