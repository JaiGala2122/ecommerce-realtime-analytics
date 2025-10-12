
import json, random, time, yaml
from pathlib import Path
from kafka import KafkaProducer

CONF = yaml.safe_load((Path(__file__).resolve().parents[0] / "config.yaml").read_text())

producer = KafkaProducer(
    bootstrap_servers=CONF["kafka"]["bootstrap_servers"],
    acks=CONF["kafka"]["acks"],
    compression_type=CONF["kafka"]["compression_type"],
    linger_ms=CONF["kafka"]["linger_ms"],
    batch_size=CONF["kafka"]["batch_size"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

topic = CONF["kafka"]["topic"]
events_path = Path(__file__).resolve().parents[1] / "data" / "events.jsonl"
assert events_path.exists(), "Run prepare_offline_sample.py first to generate events.jsonl"

# Load events into memory (for a simple simulator)
events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
random.seed(CONF["stream"]["random_seed"])

eps = CONF["stream"]["events_per_second"]
sleep_max = CONF["stream"]["max_sleep_ms"] / 1000.0

print(f"Streaming ~{eps} events/sec to topic '{topic}' ... Ctrl+C to stop.")
i = 0
while True:
    ev = events[i % len(events)].copy()
    # Jitter event_time slightly to emulate 'now'
    ev["event_time"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    producer.send(topic, ev)
    i += 1
    # Pace
    time.sleep(max(0.0, random.random() * sleep_max))
