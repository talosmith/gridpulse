# Lakeflow Declarative Pipeline: bronze envelopes -> typed, deduplicated silver series.
#
# Rules applied here:
#   * every timestamp is stored as a UTC instant (ts_utc) plus its Madrid wall-clock (ts_madrid);
#     REE sends explicit offsets, so the repeated 02:00 of the autumn DST change survives
#   * collection windows overlap on purpose; the most recent fetch of a point wins
#   * the raw payload is parsed with an explicit schema: unknown fields are ignored, never fatal
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

MADRID = "Europe/Madrid"

REE_PAYLOAD = """STRUCT<included: ARRAY<STRUCT<
    type: STRING, id: STRING,
    attributes: STRUCT<title: STRING,
        values: ARRAY<STRUCT<value: DOUBLE, percentage: DOUBLE, datetime: STRING>>>>>>"""

# Stable, English keys for the REE series ids the rest of the lakehouse relies on.
REE_SERIES_KEYS = {
    "600": "spot_price",
    "1001": "pvpc_price",
    "2037": "demand_real",
    "2052": "demand_forecast",
    "2053": "demand_scheduled",
    "2054": "demand_scheduled_total",
    "10299": "renewable",
    "10298": "non_renewable",
}

GRANULARITY = {"prices": None, "demand": "5min", "generation_mix": "1d", "renewable_share": "1d"}

WEATHER_PAYLOAD = """ARRAY<STRUCT<point: STRING, timezone: STRING,
    hourly: STRUCT<time: ARRAY<STRING>, temperature_2m: ARRAY<DOUBLE>,
        wind_speed_100m: ARRAY<DOUBLE>, shortwave_radiation: ARRAY<DOUBLE>,
        cloud_cover: ARRAY<DOUBLE>>>>"""

WEATHER_VARIABLES = ("temperature_2m", "wind_speed_100m", "shortwave_radiation", "cloud_cover")


def _latest_wins(df, keys, order_cols):
    window = Window.partitionBy(*keys).orderBy(*order_cols)
    return df.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")


def _map(mapping: dict):
    return F.create_map(*[F.lit(x) for kv in mapping.items() for x in kv])


@dp.materialized_view(
    name="silver_ree_series",
    comment="One row per REE series and timestamp, latest fetch wins. Long format.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("has_timestamp", "ts_utc IS NOT NULL")
@dp.expect_or_drop("has_value", "value IS NOT NULL")
@dp.expect("known_dataset", "dataset IN ('prices', 'demand', 'generation_mix', 'renewable_share')")
def silver_ree_series():
    series = (
        spark.read.table("bronze_ree")  # noqa: F821
        .select("dataset", "fetched_at",
                F.explode(F.from_json("payload_json", REE_PAYLOAD).included).alias("s"))
        .select(
            "dataset", "fetched_at",
            F.col("s.id").alias("series_id"),
            F.col("s.attributes.title").alias("series_name"),
            F.explode("s.attributes.values").alias("v"),
        )
        .select(
            "dataset", "series_id", "series_name", "fetched_at",
            F.to_timestamp("v.datetime", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("ts_utc"),
            F.col("v.value").alias("value"),
            F.col("v.percentage").alias("share"),
        )
    )
    deduped = _latest_wins(series, ["dataset", "series_id", "ts_utc"], [F.col("fetched_at").desc()])
    return deduped.select(
        "dataset",
        "series_id",
        F.coalesce(_map(REE_SERIES_KEYS)[F.col("series_id")], F.col("series_name")).alias("series_key"),
        "series_name",
        # the EU day-ahead market moved from 60 to 15 minute products on 1 Oct 2025
        F.when((F.col("series_id") == "600") & (F.col("ts_utc") >= F.lit("2025-09-30 22:00:00").cast("timestamp")), "15min")
         .when(F.col("series_id") == "600", "1h")
         .when(F.col("series_id") == "1001", "1h")
         .otherwise(_map({k: v for k, v in GRANULARITY.items() if v})[F.col("dataset")])
         .alias("granularity"),
        "ts_utc",
        F.from_utc_timestamp("ts_utc", MADRID).alias("ts_madrid"),
        "value",
        "share",
        "fetched_at",
    )


@dp.materialized_view(
    name="silver_weather_hourly",
    comment="Hourly weather per location in UTC. is_forecast = the value was a forecast when fetched.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("has_timestamp", "ts_utc IS NOT NULL")
@dp.expect("plausible_temperature", "temperature_2m BETWEEN -30 AND 55")
@dp.expect("plausible_wind", "wind_speed_100m BETWEEN 0 AND 250")
def silver_weather_hourly():
    locations = (
        spark.read.table("bronze_open_meteo")  # noqa: F821
        .select("fetched_at", F.explode(F.from_json("payload_json", WEATHER_PAYLOAD)).alias("l"))
    )
    hours = locations.select(
        "fetched_at",
        F.col("l.point").alias("point"),
        F.col("l.timezone").alias("source_timezone"),
        F.col("l.hourly").alias("h"),
        F.posexplode("l.hourly.time").alias("i", "time_text"),
    )
    local_ts = F.to_timestamp("time_text", "yyyy-MM-dd'T'HH:mm")
    typed = hours.select(
        "point", "fetched_at", "source_timezone",
        # Envelopes fetched before the switch to GMT carry Madrid local time
        F.when(F.col("source_timezone").isin("GMT", "UTC"), local_ts)
         .otherwise(F.to_utc_timestamp(local_ts, F.col("source_timezone")))
         .alias("ts_utc"),
        *[F.col(f"h.{v}")[F.col("i")].alias(v) for v in WEATHER_VARIABLES],
    )
    deduped = _latest_wins(
        typed, ["point", "ts_utc"],
        # prefer native-UTC payloads, then the freshest observation
        [F.col("source_timezone").isin("GMT", "UTC").desc(), F.col("fetched_at").desc()],
    )
    return deduped.select(
        "point",
        "ts_utc",
        F.from_utc_timestamp("ts_utc", MADRID).alias("ts_madrid"),
        *WEATHER_VARIABLES,
        (F.col("ts_utc") > F.col("fetched_at")).alias("is_forecast"),
        "fetched_at",
    )
