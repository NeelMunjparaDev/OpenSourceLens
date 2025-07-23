# OpenSourceLens – GitHub Analytics Pipeline

Hey! This is my OpenSourceLens – GitHub Analytics Pipeline—a project I’ve been working on to pull hourly PushEvent data from the GitHub Archive API, process it using a medallion architecture, and store it locally or on AWS S3. I used Airflow, PySpark, and Iceberg to make it run automatically, and I’m excited to share what I’ve built.

This pipeline grabs JSON data from GitHub every hour, turns it into something useful through bronze, silver, and gold layers, and stores it in two places—locally and on S3. I set it all up with Airflow to run automatically, and there’s an optional Streamlit dashboard idea I’m tinkering with. It’s a fun way to show off my data engineering skills, and I hope you’ll like digging into it!

## What It Does

- Fetches JSON data hourly from GitHub.
- Uses a medallion architecture:
  - **Bronze**: Raw ingested data.
  - **Silver**: Cleaned and transformed data.
  - **Gold**: Aggregated insights.
- Stores data in `local_data/` and S3.
- Checks data readiness with Airflow sensors.
- Plans for a Streamlit dashboard are on my to-do list.

## Architecture

[GitHub Archive API]
| (Hourly PushEvent JSON)
v
+-----------------------------+
| Airflow DAG: github_pipeline|
| (@hourly) |
+-----------------------------+
|
v
+-----------------------------+
| Task: ingest_local_data |
+-----------------------------+
|
v
+-----------------------------+
| Split to Dual Storage |
|-----------------------------|
| [AWS S3 Bronze] | [Local Bronze] |
| mainfolder/ | local_data/bronze/y/m/d |
| bronze/y/m/d | |
+-----------------------------+
|
v
+-----------------------------+
| FileSensor: sense_bronze |
| (Senses bronze files) |
+-----------------------------+
|
v
+-----------------------------+
| Task: transform_bronze_to |
| silver |
+-----------------------------+
|
+----------------+
| |
v v
+------------------+ +------------------+
| S3 Silver | | Local Silver |
| mainfolder/ | | local_data/datalake/github_commits/silver/y/m/d |
| datalakehouse/ | | |
| github_commits/ | | |
| silver | | |
+------------------+ +------------------+
|
v
+-----------------------------+
| FileSensor: sense_silver |
| (Senses parquet files) |
+-----------------------------+
|
v
+-----------------------------+
| Task: perform_aggregations |
+-----------------------------+
|
+----------------+
| |
v v
+------------------+ +------------------+
| S3 Gold | | Local Gold |
| mainfolder/ | | local_data/datalake/github_commits/gold/y/m/d |
| datalakehouse/ | | |
| github_commits/ | | |
| gold | | |
+------------------+ +------------------+
|
v
+-----------------------------+
| Streamlit Dashboard |
| (Optional: Visualizes |
| metrics) |
+-----------------------------+

## Tech Stack

- **Languages**: Python
- **Frameworks/Tools**: PySpark, Apache Iceberg, Apache Airflow, AWS S3, GitHub API

## Features

- **Automated Ingestion**: Fetches PushEvent data hourly from GitHub Archive API.
- **Data Processing**: Process raw JSON data and transform it into structured Parquet files using Iceberg tables.
- **Dual Storage**: Stores data locally (`local_data/`) and on AWS S3.
- **Monitoring**: Uses Airflow sensors to ensure data readiness before processing.
- **Scalability**: Designed for local development with potential Databricks integration.

## Gold Layer Aggregations

- **Repository Health Score**: Aggregated stats like active contributors, commits, and files changed.
- **Contributor Consistency Score**: Measures how consistently each contributor pushes code.
- **Branch Activity Distribution**: Tracks contribution spread across branches.

## Challenges I Tackled

- **Local + S3 Flexibility**: Implemented logic to simulate cloud data lake architecture locally, while maintaining compatibility with S3-style paths.
- **Schema Normalization**: Parsed semi-structured JSON into structured formats using predefined schemas.
- **Custom Aggregations**: Built logic for computing health metrics from commit-level data using PySpark.
- **Iceberg Table Management**: Managed schema evolution, partition columns, and incremental writes to Iceberg tables.
- **Triggering Automation with Airflow**: Automated the whole pipeline and ensured that each step runs at the correct time.

## Project Outcome

This project simulates a real-world data pipeline and demonstrates:

- Practical understanding of data lake design.
- Efficient PySpark data processing.
- Handling partitioned datasets and aggregation metrics.
- Ability to work with both batch ingestion and structured table storage.
- Orchestrating a data pipeline with Airflow.

## How to Run

1. Clone the repo

2. Setup venv and install:
   python -m venv .venv && source .venv/bin/activate && pip install -e .

3. Config: Create .env with GH_ARCHIVE_URL, BRONZE_BUCKET, BRONZE_BASE, LOCAL_DB_PATH, ICEBERG_SILVER_TABLE...

4. Start Airflow:
   airflow api-server & airflow scheduler

5. Run: Open http://localhost:8080, log in.

6. Check: Look in local_data/ and S3.

## Folder Structure

gh_archive_pipeline/
├── src/gh_archive_pipeline/scripts/
├── airflow/dags/
├── pyproject.toml
├── .gitignore
├── .env
└── local_data/ (bronze/, datalake/github_commits/silver/, gold/)
