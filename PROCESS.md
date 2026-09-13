# PROCESS.md

# Rearc Data Quest – Thought Process

## Architecture

### Overall Design

I implemented the solution using a Medallion Architecture consisting of Bronze, Silver and Gold layers within Databricks using Spark Declarative Pipelines (Delta Live Tables).

The overall flow is:

```
BLS Website
            \
             \
              --> 01_ingestion --> Unity Catalog Volume
             /
Population API

                    │
                    ▼

                 Bronze
                    │
                    ▼

                 Silver
                    │
                    ▼

                  Gold
```

The ingestion notebook is responsible only for acquiring data from the source systems and storing it in a Unity Catalog Volume.

All transformation logic is implemented in the Spark Declarative Pipeline.

This separation keeps ingestion independent from transformations and allows the pipeline to be rerun without needing to re-download source data.

---

### Bronze Layer

The Bronze layer is intentionally minimal.

Responsibilities:

- Read raw TSV files
- Read the Population API JSON
- Standardize column names
- Persist raw data into Delta tables

No business transformations are performed at this stage.

---

### Silver Layer

The Silver layer prepares data for analytics.

Responsibilities include:

- Type casting
- Joining productivity data with lookup datasets
- Applying basic data quality expectations
- Cleaning population data
- Producing curated datasets suitable for reporting

Business logic is kept out of Bronze and concentrated here.

---

### Gold Layer

The Gold layer contains only business-ready outputs.

The three Gold tables directly answer the assignment questions:

- Mean and standard deviation of annual population (2013–2018)
- Best year for every BLS series
- PRS30006032 (Q01) joined with annual population

---

### PySpark vs Spark SQL

The Spark Declarative Pipeline was implemented primarily in PySpark because it provides better structure for reusable transformations, helper functions, and pipeline definitions.

Equivalent Spark SQL implementations of the analytical queries are also included in the repository to demonstrate both approaches.

---

### Re-running ingestion

The ingestion process was designed to be safely repeatable.

The notebook dynamically discovers available BLS files before downloading them and stores all raw data in a Unity Catalog Volume.

The transformation pipeline operates independently of the download process, allowing the pipeline to be rerun without modifying the source files.

---

# Trade-offs

This implementation is intended for the scope of the assignment.

For a production implementation I would additionally consider:

### Schema Drift

Rather than assuming a fixed schema, I would implement schema evolution using Auto Loader together with schema inference and evolution support.

### Data Volume

For significantly larger datasets I would move from full refreshes to incremental ingestion and partition the fact tables appropriately.

### Cost

The assignment was developed using Databricks Free Edition.

For production workloads I would optimise cluster sizing, use autoscaling where appropriate, and minimise unnecessary recomputation.

### Access Control

I used Unity Catalog for organizing data.

In a production environment I would define role-based permissions at the catalog, schema and table levels together with service principals for automated execution.

### Monitoring

For production I would add:

- Pipeline monitoring
- Alerting
- Logging
- Data quality dashboards
- Automated failure notifications

---

# Retrospective

The most challenging parts of the implementation were related to configuring Spark Declarative Pipelines rather than the analytical logic itself.

Some of the issues encountered included:

- Handling the BLS website's User-Agent requirement to avoid HTTP 403 responses.
- Cleaning incoming column names so they complied with Delta table naming rules.
- Resolving duplicate dataset names while building the Declarative Pipeline.
- Understanding pipeline dependencies between Bronze, Silver and Gold datasets.
- Flattening the Population API JSON into a relational structure suitable for joining.

Working through these issues improved my understanding of Spark Declarative Pipelines and how pipeline dependencies are resolved.

---

# AI Usage

I used ChatGPT as a development aid during the implementation of this assignment.

It was primarily used for:

- Explaining Spark Declarative Pipeline concepts.
- Assisting with debugging, explaining Spark Declarative Pipeline concepts, and reviewing portions of the Python implementation.
- Troubleshooting Databricks-specific issues such as pipeline configuration, schema cleanup, and dependency resolution.
- Reviewing and refactoring code for readability.
- Documentation and formatting

The overall architecture, transformation logic, SQL implementations, testing, debugging within Databricks, and validation of the final outputs were completed by me. All AI-generated suggestions were reviewed, adapted where necessary, and verified before being incorporated into the final solution.