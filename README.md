# 📈 Enterprise Python ETL Pipeline: Automated Market Monitoring

## 📌 Project Overview
This project is an end-to-end, automated Extract, Transform, and Load (ETL) pipeline built in Python. Designed to mimic production-grade data engineering workloads, the pipeline automatically ingests live market data from the Yahoo Finance API, tracks daily performance for major technology and asset management equities (AAPL, GOOGL, MSFT, AMZN, IVZ), applies vector-based transformations to flag unusual market volatility, and loads the structured output into a local relational data warehouse.

The project utilizes a strict, modular, enterprise-grade directory structure separating extraction (API networking), transformation (business logic), and loading (database persistence) layers. It is built to demonstrate practical, code-first data pipeline architecture and aligns with modern cloud data strategies outlined in **Microsoft Azure Data Fundamentals (DP-900)**.

## ⚙️ Pipeline Execution Flow

1. **Extract (`src/extract/api_client.py`):** Connects to the Yahoo Finance API (`yfinance`) to batch-download 30 days of historical market data for target tickers. Handles JSON-to-tabular restructuring dynamically.
2. **Transform (`src/transform/stock_cleaner.py`):** Ingests raw data into Pandas DataFrames. Sorts chronologically, calculates daily percentage changes using vectorized operations, handles null values implicitly, and algorithmically flags any daily price swing exceeding a 5% threshold.
3. **Load (`src/load/db_handler.py`):** Establishes a secure connection to a local SQLite data warehouse (`stock_warehouse.db`). Translates the transformed DataFrame into a relational schema and executes the load process for downstream SQL analytics.
4. **Orchestrate (`main.py`):** The master control flow script. Executes the ETL phases sequentially, manages dependencies between modules, and utilizes Python's `logging` framework to create a permanent audit trail (`pipeline_execution.log`) of all batch runs and fatal errors.

## ☁️ Cloud Architecture Mapping (Azure DP-900)
While executing locally, this pipeline's modular architecture acts as a direct analog for enterprise Azure services:

| Local Implementation | ETL Phase | Azure Cloud Equivalent | Description |
| :--- | :--- | :--- | :--- |
| `api_client.py` | **Extract** | **Azure Data Factory (Copy Activity)** | Connecting to external REST APIs and pulling semi-structured data into a landing zone. |
| `stock_cleaner.py` | **Transform** | **Azure Synapse Analytics / Databricks** | High-performance, vector-based data cleaning and business logic application via Python/Pandas. |
| `db_handler.py` | **Load** | **Azure SQL Database** | Relational storage utilizing strict schema definitions for analytical querying and reporting. |
| `main.py` | **Orchestrate**| **Azure Data Factory (Control Flow)** | Pipeline orchestration, dependency management, error handling, and execution logging. |

## 📂 Project Structure
```text
stock_etl_pipeline/
├── src/
│   ├── extract/
│   │   └── api_client.py       # API connection and data ingestion
│   ├── transform/
│   │   └── stock_cleaner.py    # Pandas data cleaning and calculation logic
│   └── load/
│       └── db_handler.py       # Relational database persistence
├── config/                     # Environment and settings configurations
├── data/                       # Local SQLite data warehouse (Git-ignored)
├── tests/                      # Unit testing directory
├── output/                     # Generated CSV reports (Git-ignored)
├── main.py                     # Master pipeline orchestrator
├── requirements.txt            # Python dependencies
└── README.md                   # Project documentation

## 🛠️ Tech Stack & Libraries
* **Language:** Python 3.x
* **Data Processing:** `pandas`
* **Database:** `sqlite3` (Built-in)
* **API Integration:** yfinance
* **Auditing:** logging
