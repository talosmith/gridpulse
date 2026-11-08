import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from ingestion.fetch import Envelope, FetchError, fetch_ree, fetch_weather, ree_url, weather_url
from ingestion.run import date_chunks, run
from ingestion.sinks import DatabricksVolumeSink, LocalSink, SinkError, normalize_host, relative_path
from ingestion.sources import REE_DATASETS, WEATHER_DATASET

PRICES = REE_DATASETS[0]


def envelope(**overrides):
    base = dict(
        source="ree", dataset="prices", window_start=date(2026, 11, 1),
        window_end=date(2026, 11, 7), request_url="https://x",
        fetched_at=datetime(2026, 11, 8, 10, 5, 0, tzinfo=timezone.utc), payload={"a": 1},
    )
    return Envelope(**{**base, **overrides})


class DateChunksTest(unittest.TestCase):
    def test_splits_inclusive_windows(self):
        chunks = list(date_chunks(date(2026, 10, 1), date(2026, 10, 15), size=7))
        self.assertEqual(chunks, [
            (date(2026, 10, 1), date(2026, 10, 7)),
            (date(2026, 10, 8), date(2026, 10, 14)),
            (date(2026, 10, 15), date(2026, 10, 15)),
        ])

    def test_single_day(self):
        self.assertEqual(list(date_chunks(date(2026, 1, 1), date(2026, 1, 1))),
                         [(date(2026, 1, 1), date(2026, 1, 1))])

    def test_rejects_reversed_window(self):
        with self.assertRaises(ValueError):
            list(date_chunks(date(2026, 1, 2), date(2026, 1, 1)))


class FetchTest(unittest.TestCase):
    def test_ree_url_covers_full_days(self):
        url = ree_url(PRICES, date(2026, 11, 1), date(2026, 11, 2))
        self.assertIn("start_date=2026-11-01T00%3A00", url)
        self.assertIn("end_date=2026-11-02T23%3A59", url)
        self.assertIn("time_trunc=hour", url)

    def test_ree_error_payload_raises(self):
        fake = lambda url: {"errors": [{"status": "400", "detail": "no disponible"}]}
        with self.assertRaises(FetchError):
            fetch_ree(PRICES, date(2026, 11, 1), date(2026, 11, 1), get=fake)

    def test_weather_tags_each_location(self):
        fake = lambda url: [{"latitude": p.latitude} for p in WEATHER_DATASET.points]
        env = fetch_weather(WEATHER_DATASET, date(2026, 11, 1), date(2026, 11, 1), get=fake)
        self.assertEqual([loc["point"] for loc in env.payload],
                         [p.name for p in WEATHER_DATASET.points])

    def test_weather_requested_in_utc(self):
        # local time loses the repeated hour of the autumn DST change
        url = weather_url(WEATHER_DATASET, date(2026, 10, 25), date(2026, 10, 25))
        self.assertIn("timezone=GMT", url)

    def test_old_windows_use_archive_recent_use_forecast(self):
        today = date(2026, 11, 8)
        old = weather_url(WEATHER_DATASET, date(2025, 11, 1), date(2025, 11, 7), today=today)
        recent = weather_url(WEATHER_DATASET, date(2026, 11, 2), date(2026, 11, 8), today=today)
        self.assertTrue(old.startswith("https://archive-api.open-meteo.com/"))
        self.assertTrue(recent.startswith("https://api.open-meteo.com/"))

    def test_weather_location_count_mismatch_raises(self):
        with self.assertRaises(FetchError):
            fetch_weather(WEATHER_DATASET, date(2026, 11, 1), date(2026, 11, 1),
                          get=lambda url: [{}])


class SinkTest(unittest.TestCase):
    def test_partitioned_path(self):
        self.assertEqual(
            relative_path(envelope()),
            "ree/prices/ingest_date=2026-11-08/prices_2026-11-01_2026-11-07_20261108T100500Z.json",
        )

    def test_local_sink_round_trip(self):
        with tempfile.TemporaryDirectory() as tmp:
            written = Path(LocalSink(tmp).write(envelope()))
            doc = json.loads(written.read_text(encoding="utf-8"))
        self.assertEqual(doc["envelope_version"], 1)
        self.assertEqual(doc["payload"], {"a": 1})
        self.assertEqual(doc["fetched_at"], "2026-11-08T10:05:00+00:00")


class DatabricksSinkTest(unittest.TestCase):
    def test_host_pasted_from_browser_is_normalized(self):
        for raw in ("dbc-1a2b.cloud.databricks.com",
                    "https://dbc-1a2b.cloud.databricks.com/",
                    " https://dbc-1a2b.cloud.databricks.com/?o=123456#workspace/Users \n"):
            self.assertEqual(normalize_host(raw), "https://dbc-1a2b.cloud.databricks.com")

    def test_volume_path_is_validated(self):
        with self.assertRaises(SinkError):
            DatabricksVolumeSink("dbc-1.cloud.databricks.com", "t", "/Volumes/only_catalog")
        sink = DatabricksVolumeSink("dbc-1.cloud.databricks.com", "t",
                                    "/Volumes/workspace/default/data_energy/gridpulse/data/landing/")
        self.assertEqual(sink.volume, "/Volumes/workspace/default/data_energy")
        self.assertEqual(sink.volume_root, "/Volumes/workspace/default/data_energy/gridpulse/data/landing")


class RunTest(unittest.TestCase):
    def test_sink_error_aborts_the_run(self):
        class BrokenSink:
            calls = 0

            def write(self, env):
                BrokenSink.calls += 1
                raise SinkError("404")

        import ingestion.run as module
        original_ree = module.fetch_ree
        module.fetch_ree = lambda d, s, e: envelope(dataset=d.name)
        try:
            with self.assertRaises(SinkError):
                run(date(2026, 1, 1), date(2026, 3, 1), BrokenSink())
        finally:
            module.fetch_ree = original_ree
        self.assertEqual(BrokenSink.calls, 1)

    def test_failures_are_counted_not_fatal(self):
        class FlakySink:
            def __init__(self):
                self.calls = 0

            def write(self, env):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("boom")
                return "ok"

        import ingestion.run as module
        original_ree, original_weather = module.fetch_ree, module.fetch_weather
        module.fetch_ree = lambda d, s, e: envelope(dataset=d.name)
        module.fetch_weather = lambda d, s, e: envelope(source="open_meteo", dataset=d.name)
        try:
            written, failed = run(date(2026, 11, 1), date(2026, 11, 1), FlakySink())
        finally:
            module.fetch_ree, module.fetch_weather = original_ree, original_weather
        self.assertEqual((written, failed), (len(REE_DATASETS), 1))


if __name__ == "__main__":
    unittest.main()
