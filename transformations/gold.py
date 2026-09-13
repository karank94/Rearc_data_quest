from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window


@dp.table(
    name="population_statistics",
    comment="Mean and standard deviation of annual US population (2013-2018)"
)
def population_statistics():

    population = dp.read("population_clean")

    return (
        population
        .filter(col("year").between(2013, 2018))
        .agg(
            avg("population").alias("mean_population"),
            stddev("population").alias("stddev_population")
        )
    )


@dp.table(
    name="best_year_per_series",
    comment="Best year for every BLS series"
)
def best_year_per_series():

    productivity = dp.read("productivity")

    yearly = (
        productivity
        .groupBy(
            "series_id",
            "year",
            "sector_name",
            "measure_text"
        )
        .agg(
            sum("value").alias("year_total")
        )
    )

    window = Window.partitionBy("series_id").orderBy(desc("year_total"))

    return (
        yearly
        .withColumn(
            "rn",
            row_number().over(window)
        )
        .filter(col("rn") == 1)
        .drop("rn")
        .withColumn(
            "series_label",
            concat_ws(" | ", col("sector_name"), col("measure_text"))
        )
    )


@dp.table(
    name="series_population",
    comment="PRS30006032 Q01 joined with annual population"
)
def series_population():

    productivity = dp.read("productivity")
    population = dp.read("population_clean")

    return (
        productivity
        .filter(
            (col("series_id") == "PRS30006032") &
            (col("period") == "Q01")
        )
        .select(
            "year",
            "value"
        )
        .join(
            population,
            "year",
            "left"
        )
        .orderBy("year")
    )