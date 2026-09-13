from pyspark import pipelines as dp
from pyspark.sql.functions import *
import re


def read_tsv(path):
    df = (
        spark.read
        .option("header", "true")
        .option("delimiter", "\t")
        .csv(path)
    )

    cleaned_columns = []

    for c in df.columns:
        new_name = (
            c.strip()
             .replace(" ", "_")
             .replace("-", "_")
             .replace(".", "_")
             .replace("/", "_")
        )

        new_name = re.sub(r"[^A-Za-z0-9_]", "", new_name)
        cleaned_columns.append(new_name)

    return df.toDF(*cleaned_columns)



@dp.table(
    name="sector",
    comment="BLS Sector Lookup"
)
def sector():
    return read_tsv("/Volumes/rearc/raw/landing/pr.sector")


@dp.table(
    name="measure",
    comment="BLS Measure Lookup"
)
def measure():
    return read_tsv("/Volumes/rearc/raw/landing/pr.measure")


@dp.table(
    name="period",
    comment="BLS Period Lookup"
)
def period():
    return read_tsv("/Volumes/rearc/raw/landing/pr.period")


@dp.table(
    name="class_lookup",
    comment="BLS Class Lookup"
)
def class_lookup():
    return read_tsv("/Volumes/rearc/raw/landing/pr.class")


@dp.table(
    name="duration",
    comment="BLS Duration Lookup"
)
def duration():
    return read_tsv("/Volumes/rearc/raw/landing/pr.duration")


@dp.table(
    name="seasonal",
    comment="BLS Seasonal Lookup"
)
def seasonal():
    return read_tsv("/Volumes/rearc/raw/landing/pr.seasonal")


@dp.table(
    name="footnote",
    comment="BLS Footnote Lookup"
)
def footnote():
    return read_tsv("/Volumes/rearc/raw/landing/pr.footnote")


@dp.table(
    name="series",
    comment="BLS Series Metadata"
)
def series():
    return read_tsv("/Volumes/rearc/raw/landing/pr.series")



@dp.table(
    name="productivity_data",
    comment="BLS Productivity Time Series"
)
def productivity_data():
    return read_tsv("/Volumes/rearc/raw/landing/pr.data.0.Current")


@dp.table(
    name="population",
    comment="US Population API"
)
def population():

    return (
        spark.read
        .option("multiline", "true")
        .json("/Volumes/rearc/raw/landing/population.json")
        .selectExpr("explode(data) as record")
        .select("record.*")
        .withColumnRenamed("Nation", "nation")
        .withColumnRenamed("Nation ID", "nation_id")
        .withColumnRenamed("ID Nation", "id_nation")
        .withColumnRenamed("Slug Nation", "slug_nation")
        .withColumnRenamed("Population", "population")
        .withColumnRenamed("Year", "year")
    )