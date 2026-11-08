"""Independent pure-Python recomputation of key gold numbers from the landing files.

Run it on the same files uploaded to the volume and compare with the SQL in the
README: if both match, the Spark pipeline is doing what we think it does.

    python scripts/reconcile.py [data/landing]
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

MADRID = ZoneInfo("Europe/Madrid")


def latest_points(root: Path, dataset: str):
    """{(series_id, ts_utc): (fetched_at, value, share)}: latest fetch wins, like silver."""
    points = {}
    for file in sorted((root / "ree" / dataset).rglob("*.json")):
        env = json.loads(file.read_text(encoding="utf-8"))
        fetched = env["fetched_at"]
        for series in env["payload"]["included"]:
            for v in series["attributes"]["values"]:
                ts = datetime.fromisoformat(v["datetime"].replace(".000", ""))
                key = (series["id"], ts)
                if key not in points or points[key][0] < fetched:
                    points[key] = (fetched, v["value"], v["percentage"])
    return points


def main(root: Path) -> None:
    prices = latest_points(root, "prices")
    demand = latest_points(root, "demand")
    renewables = latest_points(root, "renewable_share")

    hourly = defaultdict(list)  # hour start (UTC) -> 15 min spot prices
    for (series_id, ts), (_, value, _) in prices.items():
        if series_id == "600":
            hour = ts.astimezone(ZoneInfo("UTC")).replace(minute=0)
            hourly[hour].append(value)

    days = defaultdict(lambda: {"hours": 0, "avg": [], "min": None, "max": None, "neg": 0})
    for hour, values in hourly.items():
        avg = sum(values) / len(values)
        d = days[hour.astimezone(MADRID).date()]
        d["hours"] += 1
        d["avg"].append(avg)
        d["neg"] += avg < 0
        d["min"] = min(values) if d["min"] is None else min(d["min"], min(values))
        d["max"] = max(values) if d["max"] is None else max(d["max"], max(values))

    renewable_share = {
        ts.astimezone(MADRID).date(): share
        for (sid, ts), (_, _, share) in renewables.items() if sid == "10299"
    }

    print(f"silver_ree_series rows (prices + demand + renewable_share only): "
          f"{len(prices) + len(demand) + len(renewables)}")
    print(f"gold_market_hourly hours with spot price: {len(hourly)}\n")
    print("date        hours  avg_eur  min_eur  max_eur  neg_h  renewable")
    for day in sorted(days):
        d = days[day]
        ren = renewable_share.get(day)
        print(f"{day}  {d['hours']:>5}  {sum(d['avg']) / len(d['avg']):7.2f}  {d['min']:7.2f}  "
              f"{d['max']:7.2f}  {d['neg']:>5}  {'' if ren is None else f'{ren:.3f}':>9}")


if __name__ == "__main__":
    main(Path(sys.argv[1] if len(sys.argv) > 1 else "data/landing"))
