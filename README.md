Local E2E Data Engineering Project: Polish Economic Analysis

Project Overview

This project demonstrates a complete end-to-end data engineering pipeline built entirely on a local machine to simulate a cloud environment without costs. The goal is to analyze regional economic disparities in Poland using data from Statistics Poland (GUS).

Key Objectives:

Cost-Free Big Data Stack: Simulating Azure Data Lake (using Azurite) and Spark Cluster (local PySpark) on Windows.

ELT Architecture: Extracting raw data to JSON, then transforming it to optimized Parquet format using Spark.

Business Intelligence: Preparing data for Power BI to visualize purchasing power parity across regions.

Architecture & Workflow

Source: Statistics Poland API (GUS BDL).

Ingestion (Extract): Python scripts fetch data with pagination and dump raw JSON files to the local Landing Zone.

Storage (Data Lake): Azurite (Azure Blob Storage Emulator) stores raw and processed data locally.

Processing (Transform): Apache Spark (PySpark) cleanses data, flattens nested JSON structures, and generates Dimension tables (Calendar, Metrics).

Serving (Load): Processed data is stored as Parquet files in the etl-data container, ready for consumption.

Visualization: Power BI Desktop connects via HTTP to the local Blob Storage.

Data Scope

The pipeline processes the following key economic indicators for all Voivodships (Regions):

Income & Spending: Average disposable income vs. expenditures per capita.

Labor Market: Unemployment rate (BAEL/LFS).

Macroeconomics: GDP per capita, Investment outlays.

Housing: Price per m2, Dwellings completed.

Directory Structure

data\raw: raw input data (JSON dumps from API)

data\processed: intermediate processing results

data\curated: final analytical data (Parquet)

configs\dev, configs\prod: environment configurations

scripts: start/stop, execution, and maintenance scripts

src\pyspark: ETL code (PySpark)

logs: execution logs

exploration: scripts for testing, debugging, or learning (organized into debug/tools/playground)

Prerequisites

OS: Windows 10/11

Runtime: Python 3.11 (Crucial for Spark 3.5 compatibility on Windows)

Java: JDK 17 (Required for Spark/Hadoop)

Emulator: Node.js (Required for Azurite)

Usage

Start services (Azurite): scripts\start_all.ps1

Run ETL Pipeline (Dev): scripts\run_etl_dev.ps1

Stop services: scripts\stop_all.ps1

Configuration Setup

The actual settings.json files are excluded from Git. To run the project, create them from templates:

Development (configs\dev):

Single Machine: Copy settings.localhost.template.json to settings.json (Default 127.0.0.1).

LAN Client: Copy settings.lan.template.json to settings.json (Update IP to host machine).

Production (configs\prod):

Copy settings.template.json or settings.lan.template.json to settings.json.

AI Transparency

This project was developed with the assistance of Google Gemini PRO v3. The AI was utilized as a collaborative tool for:

Generating boilerplate code and scripts (PowerShell, Python).

Troubleshooting configuration issues (specifically Hadoop on Windows and Azurite connectivity).

Architecting the local data engineering pipeline.