"""Shared paths, config loading and time zone."""
import os
import time
from pathlib import Path

import yaml

# PySpark hands timestamps to Python in the process's local time zone.
# Pin it to UTC so window times and freshness are correct on any laptop.
os.environ["TZ"] = "UTC"
time.tzset()

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"


def load_config() -> dict:
    with open(ROOT / "src" / "config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def path(rel: str) -> Path:
    """Resolve a config path relative to the repo root, not the shell's cwd."""
    return ROOT / rel
