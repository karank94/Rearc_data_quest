# Rearc Data Quest

## Overview

This repository contains my solution for the Rearc Data Quest.

The primary implementation uses **Databricks Spark Declarative Pipelines (Delta Live Tables)** following a Bronze → Silver → Gold Medallion Architecture.

The solution ingests BLS productivity datasets together with US population data from the DataUSA API, transforms the data into curated analytical datasets, and produces Gold tables answering the required business questions.

---

## Repository Structure

```
notebooks/
    01_ingestion.py

transformations/
    bronze.py
    silver.py
    gold.py

sql/
    question1.sql
    question2.sql
    question3.sql

screenshots/

README.md
PROCESS.md
requirements.txt
```

---

## Pipeline

The implementation consists of:

- **01_ingestion** – Downloads BLS datasets and Population API data into a Unity Catalog Volume.
- **Bronze** – Raw ingestion layer.
- **Silver** – Data cleaning, type casting, enrichment, and expectations.
- **Gold** – Business-ready analytical tables.

---

## Gold Outputs

The pipeline produces:

- `population_statistics`
- `best_year_per_series`
- `series_population`

These directly answer the three analytical questions in the assignment.

---

## Technologies

- Databricks Free Edition
- Apache Spark
- Spark Declarative Pipelines (Delta Live Tables)
- Unity Catalog
- PySpark
- Spark SQL

For implementation details, trade-offs, and design decisions, see `PROCESS.md`.