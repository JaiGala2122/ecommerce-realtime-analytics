# Real-Time E‑Commerce Analytics — Technical

Minimal, **technical-only** repo. Narrative/conclusions live on the project website (`docs/index.html`).

## Components
- `data/Online_Retail.xlsx` — real UCI dataset.
- `src/` — scripts:
  - `prepare_offline_sample.py` → clean Excel to CSV + JSONL.
  - `stream_simulator.py` → publish JSONL to Kafka.
  - `spark_job.py` → Spark Structured Streaming → aggregates (Parquet by default).
  - `config.yaml`, `utils.py`.
- `sql/schema.sql` — Postgres DDL.
- `docs/index.html` — website (conclusions & write‑up).
- `docker-compose.yml`, `requirements.txt`, `.gitignore`, `LICENSE`, CI workflow.

## Quickstart
```bash
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Optional infra
docker compose up -d

# Prepare real data (creates CSV + events.jsonl)
python src/prepare_offline_sample.py

# Stream to Kafka
python src/stream_simulator.py

# Run streaming job
python src/spark_job.py
```
