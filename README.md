# Local E2E Data Engineering Project: Polish Economic Analysis

### 🚀 [View Interactive Report (Power BI)](https://app.powerbi.com/view?r=eyJrIjoiZmRiMDUyMTAtMTU0Mi00NjZhLWEwOTgtMmNlY2U1ZTc5YTY1IiwidCI6ImM5YWJlNDc4LTkwYWQtNDgxNC05MWZiLWI0NDY1MzljYmQwZSJ9) 

Only the Executive Summary Completed, Report is actively worked on.
**Backlog and roadmap are now dynamically managed by local AI Agents.**

## 🤖 Agentic Orchestration & AI Development
The project features a **local Multi-Agent System** that acts as a technical co-pilot for project management and requirements engineering.

> [!WARNING]
> **Conceptual Proof of Concept:** The Developer Agent demonstrates the limits of local LLMs (like Llama 3.1 8B). It serves as a "Stochastic Parrot" to showcase automated development workflows, but it is prone to hallucinating non-existent PySpark methods.

* **Analyst Agent:** Performs deep-scans of source code, infrastructure scripts (Azurite), and configuration files. It includes a **Secret Scrubber** to redact sensitive keys (like in `settings.json`) before processing.
* **Product Owner Agent:** Transforms raw ideas into **Gherkin-compliant User Stories** and prioritizes them using the **RICE Framework**.
* **Developer Agent (PoC):** Generates PySpark code skeletons based on prioritized tasks to demonstrate automated development workflows.
* **Performance Monitoring:** Every agentic interaction is measured (Performance Monitor) to optimize execution across different hardware profiles (**RTX 5070 Ti** vs **RTX 3070 Laptop**).

### Agent Workflows
* [View Phase 1: Context & Backlog Generation Diagram](./docs/diagrams/agent_phase1_backlog.mmd)
* [View Phase 2: Code Generation Diagram](./docs/diagrams/agent_phase2_developer.mmd)

## Project Overview
This project demonstrates a professional end-to-end data engineering pipeline built in a local environment to simulate a production cloud-scale architecture. The system extracts, transforms, and visualizes regional economic disparities in Poland using data from the Statistics Poland (GUS) BDL API.

**Key Technical Pillars:**
* **Cloud Simulation:** Full Azure Data Lake simulation using Azurite (Blob Storage) and local PySpark on Windows.
* **Local LLM Integration:** Powered by **Ollama** and **Llama 3.1**, allowing for private, offline project orchestration and decision-making.
* **Infrastructure Portability:** Automated environment normalization (WinUtils/Hadoop integration) for seamless execution across multiple machines (Laptop/Desktop).
* **Data Quality & Imputation:** Advanced transformation logic implementing linear interpolation to fill data gaps or confidential records (e.g., Opolskie 2023).
* **Professional Tooling:** Strict enforcement of code style using **Black** (formatter) and **Flake8** (linter), with unit tests powered by **Pytest**.
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
6. **AI Prioritization Loop:** Automated backlog generation where tasks are scored via RICE:
   $$RICE = \frac{Reach \times Impact \times Confidence}{Effort}$$
7. **Visualization:** Power BI Desktop connected via HTTP/WASB, featuring dynamic trend analysis and regional benchmarking.

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
* `src/ai_agent/`: Orchestration scripts for local LLM Analysts and PO Agents.
* `tests/`: Unit tests and mocks for pipeline and agent validation.
* `configs/`: Environment-specific settings (`dev`/`prod`) and metric definitions.
* `docs/backlog_output/`: AI-generated prioritized backlogs and Gherkin stories.
* `scripts/`: Automation scripts for service management and pipeline execution.
* `exploration/`: Advanced debugging tools, API inspectors, and data availability checkers.
* `assets/maps/`: TopoJSON files processed for Power BI Shape Map integration.
* `docs/`: Detailed technical documentation, DAX blueprints, and setup guides.

## Prerequisites
* **OS:** Windows 10/11.
* **AI Engine:** [Ollama](https://ollama.com/) with **Llama 3.1** model installed.
* **Runtime:** Python 3.11 (optimized for PySpark 3.4.1 compatibility).
* **Java:** JDK 17 (Required for Spark/Hadoop ecosystem).
* **Emulator:** Node.js for Azurite Blob Storage.

## Quick Start
1. **Install Project (Editable Mode):** `pip install -e .`
2. **Start Services (Azurite):** `.\scripts\start_all.ps1`
3. **Generate/Analyze Backlog:** `python src/ai_agent/backlog_orchestrator.py`
4. **Run ETL Pipeline:** `.\scripts\run_etl_dev.ps1`
5. **Run Quality Checks:** `pytest -v tests/`

## Maintenance & Cleanup
To maintain a clean environment or reset data states, use the following utility scripts:
* **Reset Cloud Storage:** `python .\exploration\tools\reset_azurite.py` (Wipes Azurite containers).
* **Clear Spark Staging:** `.\scripts\maintenance\clean_staging.ps1` (Removes transient Parquet files).
* **Purge Raw API Data:** `.\scripts\maintenance\clean_raw_data.ps1` (Deletes all JSON source files).

## Configuration Setup
Active `settings.json` files are ignored by Git. Use the provided templates:
* **Local/Server Mode:** Copy `settings.template.json` to `settings.json`.
* **LAN Client Mode:** Copy `settings.lan.template.json` to `settings.json` (update Host IP).
* **Security:** All `settings.json` content is automatically redacted by the AI Analyst Agent during context ingestion.

## AI Transparency
This project was developed in collaboration with **Gemini 3.1 Pro**. The AI served as a pair-programmer for:
* Architecting the Windows-compatible Spark environment.
* Designing complex DAX measures for economic benchmarking.
* Building a **Multi-Agent Orchestrator** for automated backlog management.
* Implementing **Secret Scrubbing** and professional code quality standards (Black/Flake8).

## Architecture Diagram
```mermaid
graph TD
    subgraph Data Extraction
        API[GUS BDL API<br>REST / JSON]
        GC[GusClient<br>Pagination & Rate Limiting]
        RAW[(Local Raw Layer<br>/data/raw/*.json)]
    end

    subgraph Data Processing
        SP[PySpark Engine<br>WinUtils / WASB]
        T_FLAT[Flattener & Parser]
        T_IMPUTE[Missing Value Imputation<br>Linear Interpolation]
        T_DIM[Dimension Builder]
    end

    subgraph Data Persistence
        STG[(Local Staging<br>/data/staging/)]
        AZ[Azurite Emulator<br>Blob Storage]
        CUR[(Curated Zone<br>data.parquet)]
    end

    subgraph Consumption
        PBI[Power BI Desktop<br>Web Connector]
    end

    API -->|HTTP GET| GC
    GC -->|Save JSON| RAW
    RAW -->|Read MultiLine| SP
    
    SP --> T_FLAT
    T_FLAT --> T_IMPUTE
    T_IMPUTE --> T_DIM
    
    T_DIM -->|coalesce 1| STG
    STG -->|BlobServiceClient<br>Cloud Sync| AZ
    AZ --> CUR
    
    CUR -->|HTTP/WASB| PBI


### Data Model
* [View Detailed Star Schema (Entity-Relationship Diagram)](./docs/diagrams/data_model.mmd)
