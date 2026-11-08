# Lakeflow Declarative Pipeline: silver -> gold, the only layer the AI agent may query.
#
# Gold tables carry column comments on purpose: in phase 4 the agent reads them from
# Unity Catalog to know what each column means, instead of the prompt hard-coding it.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

MADRID = "Europe/Madrid"

DEMAND_CENTRES = ("madrid", "barcelona", "sevilla")
WIND_CLUSTERS = ("zaragoza", "a_coruna")
SOLAR_CLUSTERS = ("sevilla", "badajoz")

RENEWABLE_TECHNOLOGIES = (
    "Hidráulica", "Hidroeólica", "Eólica", "Solar fotovoltaica",
    "Solar térmica", "Otras renovables", "Residuos renovables",
)
TECHNOLOGY_EN = {
    "Hidráulica": "hydro", "Hidroeólica": "hydro_wind", "Nuclear": "nuclear",
    "Carbón": "coal", "Motores diésel": "diesel_engines", "Turbina de gas": "gas_turbine",
    "Turbina de vapor": "steam_turbine", "Ciclo combinado": "combined_cycle_gas",
    "Eólica": "wind", "Solar fotovoltaica": "solar_pv", "Solar térmica": "solar_thermal",
    "Otras renovables": "other_renewables", "Cogeneración": "cogeneration",
    "Residuos no renovables": "non_renewable_waste", "Residuos renovables": "renewable_waste",
}

# EU harmonised day-ahead price limits (SDAC), EUR/MWh
PRICE_FLOOR, PRICE_CAP = -500, 4000


def _avg_if(condition, column):
    return F.avg(F.when(condition, F.col(column)))


def _map(mapping: dict):
    return F.create_map(*[F.lit(x) for kv in mapping.items() for x in kv])


MARKET_HOURLY_SCHEMA = """
    ts_utc TIMESTAMP COMMENT 'Start of the hour, UTC. Primary key.',
    ts_madrid TIMESTAMP COMMENT 'Start of the hour, Madrid wall-clock time (02:00 repeats on the autumn DST day).',
    date_madrid DATE COMMENT 'Calendar date in Madrid.',
    hour_madrid INT COMMENT 'Hour of day in Madrid, 0-23.',
    spot_price_eur_mwh DOUBLE COMMENT 'Average Spanish day-ahead spot price for the hour, EUR/MWh (mean of the four 15-minute prices).',
    spot_price_min_15m DOUBLE COMMENT 'Lowest 15-minute spot price inside the hour, EUR/MWh.',
    spot_price_max_15m DOUBLE COMMENT 'Highest 15-minute spot price inside the hour, EUR/MWh.',
    is_negative_price BOOLEAN COMMENT 'True when the hourly average spot price is below zero.',
    pvpc_price_eur_mwh DOUBLE COMMENT 'PVPC regulated household tariff, EUR/MWh.',
    demand_real_mw DOUBLE COMMENT 'Average real peninsular electricity demand in the hour, MW.',
    demand_forecast_mw DOUBLE COMMENT 'Average demand forecast by Red Electrica for the hour, MW.',
    demand_forecast_error_mw DOUBLE COMMENT 'demand_real_mw minus demand_forecast_mw.',
    temperature_demand_centres_c DOUBLE COMMENT 'Mean 2 m temperature in Madrid, Barcelona and Sevilla, Celsius.',
    wind_speed_wind_clusters_kmh DOUBLE COMMENT 'Mean 100 m wind speed in Zaragoza and A Coruna (wind farm regions), km/h.',
    solar_radiation_solar_clusters_wm2 DOUBLE COMMENT 'Mean shortwave radiation in Sevilla and Badajoz (solar PV regions), W/m2.',
    weather_is_forecast BOOLEAN COMMENT 'True if any weather value was still a forecast when collected.'
"""


@dp.materialized_view(
    name="gold_market_hourly",
    comment="Hourly Spanish electricity market facts joined with the weather that drives them. One row per hour.",
    schema=MARKET_HOURLY_SCHEMA,
    table_properties={"quality": "gold"},
)
@dp.expect("price_within_eu_limits", f"spot_price_eur_mwh BETWEEN {PRICE_FLOOR} AND {PRICE_CAP}")
@dp.expect("plausible_demand", "demand_real_mw IS NULL OR demand_real_mw BETWEEN 10000 AND 50000")
def gold_market_hourly():
    series = spark.read.table("silver_ree_series").filter(  # noqa: F821
        F.col("dataset").isin("prices", "demand")
    )
    key = F.col("series_key")
    market = (
        series.groupBy(F.date_trunc("hour", "ts_utc").alias("ts_utc"))
        .agg(
            _avg_if(key == "spot_price", "value").alias("spot_price_eur_mwh"),
            F.min(F.when(key == "spot_price", F.col("value"))).alias("spot_price_min_15m"),
            F.max(F.when(key == "spot_price", F.col("value"))).alias("spot_price_max_15m"),
            _avg_if(key == "pvpc_price", "value").alias("pvpc_price_eur_mwh"),
            _avg_if(key == "demand_real", "value").alias("demand_real_mw"),
            _avg_if(key == "demand_forecast", "value").alias("demand_forecast_mw"),
        )
        .filter("spot_price_eur_mwh IS NOT NULL OR demand_real_mw IS NOT NULL")
    )

    point = F.col("point")
    weather = (
        spark.read.table("silver_weather_hourly")  # noqa: F821
        .groupBy("ts_utc")
        .agg(
            _avg_if(point.isin(*DEMAND_CENTRES), "temperature_2m").alias("temperature_demand_centres_c"),
            _avg_if(point.isin(*WIND_CLUSTERS), "wind_speed_100m").alias("wind_speed_wind_clusters_kmh"),
            _avg_if(point.isin(*SOLAR_CLUSTERS), "shortwave_radiation").alias("solar_radiation_solar_clusters_wm2"),
            F.max("is_forecast").alias("weather_is_forecast"),
        )
    )

    ts_madrid = F.from_utc_timestamp("ts_utc", MADRID)
    return market.join(weather, "ts_utc", "left").select(
        "ts_utc",
        ts_madrid.alias("ts_madrid"),
        F.to_date(ts_madrid).alias("date_madrid"),
        F.hour(ts_madrid).cast("int").alias("hour_madrid"),
        "spot_price_eur_mwh",
        "spot_price_min_15m",
        "spot_price_max_15m",
        (F.col("spot_price_eur_mwh") < 0).alias("is_negative_price"),
        "pvpc_price_eur_mwh",
        "demand_real_mw",
        "demand_forecast_mw",
        (F.col("demand_real_mw") - F.col("demand_forecast_mw")).alias("demand_forecast_error_mw"),
        "temperature_demand_centres_c",
        "wind_speed_wind_clusters_kmh",
        "solar_radiation_solar_clusters_wm2",
        "weather_is_forecast",
    )


GENERATION_DAILY_SCHEMA = """
    date_madrid DATE COMMENT 'Calendar date in Madrid.',
    technology STRING COMMENT 'Generation technology, English snake_case (wind, solar_pv, nuclear, combined_cycle_gas...).',
    technology_es STRING COMMENT 'Original Red Electrica technology name in Spanish.',
    is_renewable BOOLEAN COMMENT 'True for renewable technologies.',
    generation_mwh DOUBLE COMMENT 'Energy generated that day, MWh. Small negatives are pumping/auxiliary consumption.',
    share_of_total DOUBLE COMMENT 'Fraction (0-1) of total national generation that day.'
"""


@dp.materialized_view(
    name="gold_generation_daily",
    comment="Daily Spanish generation by technology. One row per date and technology.",
    schema=GENERATION_DAILY_SCHEMA,
    table_properties={"quality": "gold"},
)
@dp.expect("share_is_fraction", "share_of_total BETWEEN -0.01 AND 1")
def gold_generation_daily():
    mix = spark.read.table("silver_ree_series").filter(  # noqa: F821
        (F.col("dataset") == "generation_mix") & (F.col("series_name") != "Generación total")
    )
    return mix.select(
        F.to_date("ts_madrid").alias("date_madrid"),
        F.coalesce(_map(TECHNOLOGY_EN)[F.col("series_name")], F.col("series_name")).alias("technology"),
        F.col("series_name").alias("technology_es"),
        F.col("series_name").isin(*RENEWABLE_TECHNOLOGIES).alias("is_renewable"),
        F.col("value").alias("generation_mwh"),
        F.col("share").alias("share_of_total"),
    )


DAILY_SUMMARY_SCHEMA = """
    date_madrid DATE COMMENT 'Calendar date in Madrid. Primary key.',
    hours_in_day INT COMMENT 'Hours with market data: 23 or 25 on DST change days, fewer for today.',
    spot_price_avg_eur_mwh DOUBLE COMMENT 'Average of the hourly spot prices of the day, EUR/MWh.',
    spot_price_min_eur_mwh DOUBLE COMMENT 'Lowest 15-minute spot price of the day, EUR/MWh.',
    spot_price_max_eur_mwh DOUBLE COMMENT 'Highest 15-minute spot price of the day, EUR/MWh.',
    negative_price_hours INT COMMENT 'Number of hours whose average spot price was below zero.',
    demand_peak_mw DOUBLE COMMENT 'Highest hourly average real demand of the day, MW.',
    demand_forecast_mape DOUBLE COMMENT 'Mean absolute percentage error of the Red Electrica demand forecast, 0-1.',
    renewable_share DOUBLE COMMENT 'Fraction (0-1) of generation from renewables that day.',
    wind_share DOUBLE COMMENT 'Fraction (0-1) of generation from wind.',
    solar_pv_share DOUBLE COMMENT 'Fraction (0-1) of generation from solar photovoltaic.',
    nuclear_share DOUBLE COMMENT 'Fraction (0-1) of generation from nuclear.',
    gas_share DOUBLE COMMENT 'Fraction (0-1) of generation from combined-cycle gas plants.',
    avg_wind_speed_kmh DOUBLE COMMENT 'Daily mean 100 m wind speed in the wind farm regions, km/h.',
    avg_solar_radiation_wm2 DOUBLE COMMENT 'Daily mean shortwave radiation in the solar regions, W/m2.'
"""


@dp.materialized_view(
    name="gold_daily_summary",
    comment="One row per day: prices, demand, generation mix and weather. The agent's starting point.",
    schema=DAILY_SUMMARY_SCHEMA,
    table_properties={"quality": "gold"},
)
@dp.expect("renewable_share_is_fraction", "renewable_share IS NULL OR renewable_share BETWEEN 0 AND 1")
def gold_daily_summary():
    hourly = (
        spark.read.table("gold_market_hourly")  # noqa: F821
        .groupBy("date_madrid")
        .agg(
            F.count("spot_price_eur_mwh").cast("int").alias("hours_in_day"),
            F.avg("spot_price_eur_mwh").alias("spot_price_avg_eur_mwh"),
            F.min("spot_price_min_15m").alias("spot_price_min_eur_mwh"),
            F.max("spot_price_max_15m").alias("spot_price_max_eur_mwh"),
            F.count(F.when(F.col("is_negative_price"), 1)).cast("int").alias("negative_price_hours"),
            F.max("demand_real_mw").alias("demand_peak_mw"),
            F.avg(F.abs(F.col("demand_forecast_error_mw")) / F.col("demand_real_mw")).alias("demand_forecast_mape"),
            F.avg("wind_speed_wind_clusters_kmh").alias("avg_wind_speed_kmh"),
            F.avg("solar_radiation_solar_clusters_wm2").alias("avg_solar_radiation_wm2"),
        )
    )
    tech = F.col("technology")
    mix = (
        spark.read.table("gold_generation_daily")  # noqa: F821
        .groupBy("date_madrid")
        .agg(
            F.sum(F.when(tech == "wind", F.col("share_of_total"))).alias("wind_share"),
            F.sum(F.when(tech == "solar_pv", F.col("share_of_total"))).alias("solar_pv_share"),
            F.sum(F.when(tech == "nuclear", F.col("share_of_total"))).alias("nuclear_share"),
            F.sum(F.when(tech == "combined_cycle_gas", F.col("share_of_total"))).alias("gas_share"),
        )
    )
    # Renewable share exactly as Red Electrica publishes it (not re-derived from the mix)
    renewable = (
        spark.read.table("silver_ree_series")  # noqa: F821
        .filter((F.col("dataset") == "renewable_share") & (F.col("series_key") == "renewable"))
        .select(F.to_date("ts_madrid").alias("date_madrid"), F.col("share").alias("renewable_share"))
    )
    return (
        hourly.join(mix, "date_madrid", "full_outer")
        .join(renewable, "date_madrid", "full_outer")
    ).select(
        "date_madrid", "hours_in_day", "spot_price_avg_eur_mwh", "spot_price_min_eur_mwh",
        "spot_price_max_eur_mwh", "negative_price_hours", "demand_peak_mw", "demand_forecast_mape",
        "renewable_share", "wind_share", "solar_pv_share", "nuclear_share", "gas_share",
        "avg_wind_speed_kmh", "avg_solar_radiation_wm2",
    )
