# Lakeflow Declarative Pipeline: landing JSON envelopes -> bronze Delta tables.
#
# Bronze keeps the API payload as an untouched JSON string. Typing happens in
# silver, so a schema change upstream never breaks ingestion and every row can
# be replayed from the exact bytes the API returned.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get("gridpulse.landing", "/Volumes/gridpulse/raw/landing")  # noqa: F821

ENVELOPE_SCHEMA = (
    "envelope_version INT, source STRING, dataset STRING, window_start DATE, "
    "window_end DATE, request_url STRING, fetched_at TIMESTAMP"
)

VALID_ENVELOPE = {
    "known_version": "envelope_version = 1",
    "has_identity": "source IS NOT NULL AND dataset IS NOT NULL",
    "has_fetch_time": "fetched_at IS NOT NULL",
    "window_ordered": "window_start <= window_end",
}


def landing_stream(source: str):
    """Incrementally pick up new envelope files for one source with Auto Loader."""
    raw = (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "text")
        .option("wholetext", "true")  # one file = one envelope = one row
        .option("pathGlobFilter", "*.json")
        .load(f"{LANDING}/{source}/")
    )
    return raw.select(
        F.from_json("value", ENVELOPE_SCHEMA).alias("e"),
        F.get_json_object("value", "$.payload").alias("payload_json"),
        F.col("_metadata.file_path").alias("source_file"),
        F.col("_metadata.file_size").alias("source_file_bytes"),
        F.current_timestamp().alias("ingested_at"),
    ).select("e.*", "payload_json", "source_file", "source_file_bytes", "ingested_at")


@dp.table(
    name="bronze_ree",
    comment="Red Eléctrica de España API responses (prices, demand, generation mix).",
    table_properties={"quality": "bronze"},
)
@dp.expect_all_or_drop(VALID_ENVELOPE)
@dp.expect("payload_present", "payload_json IS NOT NULL")
def bronze_ree():
    return landing_stream("ree")


@dp.table(
    name="bronze_open_meteo",
    comment="Open-Meteo hourly weather for Spanish demand, wind and solar hotspots.",
    table_properties={"quality": "bronze"},
)
@dp.expect_all_or_drop(VALID_ENVELOPE)
@dp.expect("payload_present", "payload_json IS NOT NULL")
def bronze_open_meteo():
    return landing_stream("open_meteo")
