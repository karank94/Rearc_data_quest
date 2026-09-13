from pyspark import pipelines as dp
from pyspark.sql.functions import *


@dp.table(
    name="productivity",
    comment="Cleaned and enriched BLS productivity data"
)
@dp.expect("series_not_null", "series_id IS NOT NULL")
@dp.expect("year_not_null", "year IS NOT NULL")
@dp.expect("value_not_null", "value IS NOT NULL")

def productivity():

    productivity = (
        dp.read("productivity_data")
        .withColumn("year", col("year").cast("int"))
        .withColumn("value", col("value").cast("double"))
    )

    series = dp.read("series")

    sector = (
        dp.read("sector")
        .select(
            "sector_code",
            "sector_name"
        )
    )

    measure = (
        dp.read("measure")
        .select(
            "measure_code",
            "measure_text"
        )
    )

    period = (
        dp.read("period")
        .select(
            "period",
            "period_name"
        )
    )

    return (
        productivity
        .join(series, "series_id", "left")
        .join(sector, "sector_code", "left")
        .join(measure, "measure_code", "left")
        .join(period, "period", "left")

        .select(
        "series_id",
        "year",
        "period",
        "period_name",
        "value",

        "sector_code",
        "sector_name",

        "measure_code",
        "measure_text",

        "class_code",

        "duration_code",

        "seasonal",

        "base_year",
        "begin_year",
        "end_year"
)
    )


@dp.table(
    name="population_clean",
    comment="Cleaned US population data"
)
@dp.expect("year_not_null", "year IS NOT NULL")

def population_clean():

    return (
        dp.read("population")

        .withColumn(
            "year",
            col("year").cast("int")
        )

        .withColumn(
            "population",
            col("population").cast("long")
        )

        .select(
            "year",
            "population",
            "nation"
        )
    )