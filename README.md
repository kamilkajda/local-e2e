# Local E2E Data Engineering Project: Polish Economic Analysis

### 🚀 [View Interactive Report (Power BI)](https://app.powerbi.com/view?r=eyJrIjoiZmRiMDUyMTAtMTU0Mi00NjZhLWEwOTgtMmNlY2U1ZTc5YTY1IiwidCI6ImM5YWJlNDc4LTkwYWQtNDgxNC05MWZiLWI0NDY1MzljYmQwZSJ9)

## Project Overview
This project demonstrates a professional end-to-end data engineering pipeline built in a local environment to simulate a production cloud-scale architecture. The system extracts, transforms, and visualizes regional economic disparities in Poland using data from the Statistics Poland (GUS) BDL API.

**Key Technical Pillars:**
* **Cloud Simulation:** Full Azure Data Lake simulation using Azurite (Blob Storage) and local PySpark on Windows.
* **Infrastructure Portability:** Automated environment normalization (WinUtils/Hadoop integration) for seamless execution across multiple machines (Laptop/Desktop).
* **Data Quality & Imputation:** Advanced transformation logic implementing linear interpolation to fill data gaps or confidential records (e.g., Opolskie 2023).
* **Business Intelligence:** Enterprise-grade Power BI reporting featuring a Bento Grid layout, multi-page navigation, and dynamic DAX narratives.

## Architecture & Workflow
1. **Source:** Statistics Poland API (GUS BDL).
2. **Ingestion (Extract):** Python-based client with pagination handling and rate-limiting, persisting raw JSON telemetry.
3. **Storage (Data Lake):** Azurite Blob Storage Emulator organized into `raw`, `staging`, and `curated` zones.
4. **Processing (Transform):** Apache Spark (PySpark) executing:
   * Hierarchical JSON flattening.
   * **Data Imputation:** Linear interpolation for missing/confidential data (`attr_id != 1`).
   * Relational modeling (Star Schema).
5. **Serving (Load):** Final assets stored as Parquet files with static URI mapping (`data.parquet`) for stable BI connectivity.
6. **Visualization:** Power BI Desktop connected via HTTP/WASB, featuring dynamic trend analysis and regional benchmarking.

## Data Scope
The pipeline monitors a comprehensive set of indicators across 16 Voivodeships:
* **Labor Market:** Average Gross Wages, Registered Unemployment Rate.
* **Living Standards:** Disposable Income vs. Expenditures per capita.
* **Macroeconomics:** GDP per capita, Total GDP, Investment Outlays.
* **Housing Market:** Residential Price per m2, Dwellings Completed, Market Transactions Volume.
* **Public Finance & Business:** Budget Revenues/Expenditures, Business Entities per 10k population.

## Directory Structure
* `data/`: Raw (JSON), Staging (Parquet), and Curated data layers.
* `src/pyspark/`: Core ETL logic (Main orchestrator, Spark setup, GUS client, Transformers).
* `configs/`: Environment-specific settings (`dev`/`prod`) and metric definitions.
* `scripts/`: Automation scripts for service management and pipeline execution.
* `exploration/`: Advanced debugging tools, API inspectors, and data availability checkers.
* `assets/maps/`: TopoJSON files processed for Power BI Shape Map integration.
* `docs/`: Detailed technical documentation, DAX blueprints, and setup guides.

## Prerequisites
* **OS:** Windows 10/11.
* **Runtime:** Python 3.11 (optimized for PySpark 3.4.1 compatibility).
* **Java:** JDK 17 (Required for Spark/Hadoop ecosystem).
* **Emulator:** Node.js for Azurite Blob Storage.

## Quick Start
1. **Start Services (Azurite):** `.\scripts\start_all.ps1`
2. **Run ETL Pipeline:** `.\scripts\run_etl_dev.ps1`

## Maintenance & Cleanup
To maintain a clean environment or reset data states, use the following utility scripts:
* **Reset Cloud Storage:** `python .\exploration\tools\reset_azurite.py` (Wipes Azurite containers).
* **Clear Spark Staging:** `.\scripts\maintenance\clean_staging.ps1` (Removes transient Parquet files).
* **Purge Raw API Data:** `.\scripts\maintenance\clean_raw_data.ps1` (Deletes all JSON source files).

## Configuration Setup
Active `settings.json` files are ignored by Git. Use the provided templates:
* **Local/Server Mode:** Copy `settings.template.json` to `settings.json`.
* **LAN Client Mode:** Copy `settings.lan.template.json` to `settings.json` (update Host IP).

## AI Transparency
This project was developed in collaboration with **Google Gemini 2.5 Flash Preview**. The AI served as a pair-programmer for:
* Architecting the Windows-compatible Spark environment.
* Designing complex DAX measures for economic benchmarking.
* Refactoring code for professional documentation standards and idempotency.
