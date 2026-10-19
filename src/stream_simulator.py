"""Replay data/events.jsonl as a live stream, in invoice-time order.

Each event keeps its original invoice time (event_time) and gets an ingest_time
stamp at the moment it is sent, so Spark can measure end-to-end freshness.

  python src/stream_simulator.py                 # uses config.yaml
  python src/stream_simulator.py --rate 2000 --limit 50000
"""
import argparse
import json
import os
import time
from datetime import datetime, timezone

from common import DATA, load_config, path


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


def batches(lines, size):
    for i in range(0, len(lines), size):
        yield lines[i:i + size]


def file_sink(conf):
    landing = path(conf["stream"]["landing_dir"])
    landing.mkdir(parents=True, exist_ok=True)
    counter = {"n": 0}

    def send(events):
        counter["n"] += 1
        name = f"events-{counter['n']:07d}.json"
        tmp = landing / f".{name}.tmp"           # Spark ignores dot-files
        with tmp.open("w", encoding="utf-8") as f:
            for ev in events:
                f.write(json.dumps(ev, ensure_ascii=False) + "\n")
        os.replace(tmp, landing / name)          # atomic: Spark never sees half a file
    return send, lambda: None


def kafka_sink(conf):
    from kafka import KafkaProducer

    k = conf["kafka"]
    producer = KafkaProducer(
        bootstrap_servers=k["bootstrap_servers"],
        acks=k["acks"],
        linger_ms=k["linger_ms"],
        batch_size=k["batch_size"],
        key_serializer=lambda s: s.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    def send(events):
        for ev in events:
            producer.send(k["topic"], key=ev["country"], value=ev)
    return send, producer.flush


def main() -> None:
    conf = load_config()
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=conf["stream"]["events_per_second"])
    ap.add_argument("--limit", type=int, default=0, help="stop after N events (0 = all)")
    args = ap.parse_args()

    events_path = DATA / "events.jsonl"
    assert events_path.exists(), "Run src/prepare_data.py first"
    lines = events_path.read_text(encoding="utf-8").splitlines()
    if args.limit:
        lines = lines[:args.limit]

    mode = conf["source"]
    send, flush = file_sink(conf) if mode == "files" else kafka_sink(conf)
    size = conf["stream"]["events_per_file"] if mode == "files" else 500

    print(f"Replaying {len(lines):,} events at {args.rate:,}/s via {mode} ...")
    start = time.perf_counter()
    sent = 0
    for chunk in batches(lines, size):
        stamp = now_iso()
        events = []
        for line in chunk:
            ev = json.loads(line)
            ev["ingest_time"] = stamp
            events.append(ev)
        send(events)
        sent += len(events)
        # Pace to the target rate.
        ahead = sent / args.rate - (time.perf_counter() - start)
        if ahead > 0:
            time.sleep(ahead)
    flush()
    elapsed = time.perf_counter() - start
    print(f"Sent {sent:,} events in {elapsed:,.1f}s ({sent / elapsed:,.0f}/s)")


if __name__ == "__main__":
    main()
