"""Command line entry point.

Examples:
    python -m ingestion.run                          # yesterday + today -> ./data/landing
    python -m ingestion.run --start 2026-10-01 --end 2026-10-31   # backfill
    python -m ingestion.run --sink databricks        # uses DATABRICKS_HOST / DATABRICKS_TOKEN
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from .fetch import FetchError, fetch_ree, fetch_weather
from .sinks import DatabricksVolumeSink, LocalSink, Sink
from .sources import REE_DATASETS, WEATHER_DATASET

MADRID = ZoneInfo("Europe/Madrid")
CHUNK_DAYS = 7  # keeps each REE request comfortably small

log = logging.getLogger("gridpulse.ingestion")


def date_chunks(start: date, end: date, size: int = CHUNK_DAYS):
    """Split [start, end] (inclusive) into consecutive windows of at most `size` days."""
    if end < start:
        raise ValueError("end date is before start date")
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=size - 1), end)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def build_sink(kind: str, local_root: str) -> Sink:
    if kind == "local":
        return LocalSink(local_root)
    missing = [v for v in ("DATABRICKS_HOST", "DATABRICKS_TOKEN") if not os.environ.get(v)]
    if missing:
        raise SystemExit(f"missing environment variables: {', '.join(missing)}")
    return DatabricksVolumeSink(
        host=os.environ["DATABRICKS_HOST"],
        token=os.environ["DATABRICKS_TOKEN"],
        volume_root=os.environ.get("GRIDPULSE_VOLUME", "/Volumes/gridpulse/raw/landing"),
    )


def run(start: date, end: date, sink: Sink) -> tuple[int, int]:
    """Collect every dataset for the window. Returns (written, failed)."""
    jobs = [(f"ree/{d.name}", lambda s, e, d=d: fetch_ree(d, s, e)) for d in REE_DATASETS]
    jobs.append((f"open_meteo/{WEATHER_DATASET.name}", lambda s, e: fetch_weather(WEATHER_DATASET, s, e)))

    written = failed = 0
    for chunk_start, chunk_end in date_chunks(start, end):
        for label, job in jobs:
            try:
                path = sink.write(job(chunk_start, chunk_end))
                written += 1
                log.info("ok   %-26s %s..%s -> %s", label, chunk_start, chunk_end, path)
            except (FetchError, RuntimeError) as error:
                failed += 1
                log.warning("fail %-26s %s..%s: %s", label, chunk_start, chunk_end, error)
    return written, failed


def main(argv: list[str] | None = None) -> int:
    today = datetime.now(MADRID).date()
    parser = argparse.ArgumentParser(description="Collect GridPulse raw data")
    parser.add_argument("--start", type=date.fromisoformat, default=today - timedelta(days=1))
    parser.add_argument("--end", type=date.fromisoformat, default=today)
    parser.add_argument("--sink", choices=("local", "databricks"), default="local")
    parser.add_argument("--local-root", default="data/landing")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    written, failed = run(args.start, args.end, build_sink(args.sink, args.local_root))
    log.info("done: %d written, %d failed", written, failed)
    # Partial failures are normal (REE publishes some series late); only fail the run if nothing landed.
    return 0 if written else 1


if __name__ == "__main__":
    sys.exit(main())
