# Wind Turbine Data Engineering Project

This repository contains a PySpark-based data engineering pipeline for **wind turbine** data ingestion, cleaning, transformation, and analysis. It is designed to run on Databricks but can be adapted for local Spark environments if desired.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Folder Structure](#folder-structure)
3. [Getting Started](#getting-started)
   - [Prerequisites](#prerequisites)
   - [Setting Up the Project Locally](#setting-up-the-project-locally)
   - [Running on Databricks](#running-on-databricks)
4. [Pipeline Details](#pipeline-details)
   - [Bronze Layer (Ingestion)](#bronze-layer-ingestion)
   - [Silver Layer (Cleaning and Standardization)](#silver-layer-cleaning-and-standardization)
   - [Gold Layer (Transformations and Analytics)](#gold-layer-transformations-and-analytics)
5. [Jobs / Orchestration](#jobs--orchestration)
6. [Testing](#testing)
7. [License](#license)

---

## Project Overview

The primary goal is to demonstrate how to:

- **Ingest** wind turbine data (both original and newly uploaded data).
- **Clean** and **standardize** the data (handle missing values, transform columns, etc.).
- **Detect anomalies** at both the record and turbine levels using multiple methods (Z-score and Isolation Forest).
- **Generate analytics** and summary statistics for further downstream consumption.

The final output is stored in **Delta Tables** at three layers:

1. **Bronze**: Unmodified or lightly processed data.
2. **Silver**: Cleaned, standardized, or denormalized data.
3. **Gold**: Aggregated or enriched data for analytics and reporting.

---

## Folder Structure

```
wind_turbine_project/
├── data/
│   ├── data_group_1.csv
│   ├── data_group_2.csv
│   ├── Location1.csv
│   └── ...
├── notebooks/
│   ├── 0_setup_data.ipynb
│   ├── 1_bronze_ingestion.ipynb
│   ├── 2_silver_cleanup.ipynb
│   ├── 3_gold_transformations.ipynb
│   └── ...
├── config/
│   └── jobs/
│       └── turbine_data_pipeline.yaml
├── src/
│   ├── wt_ingestion.py
│   ├── wt_cleaning.py
│   ├── wt_transformations.py
│   ├── wt_logger.py
│   └── ...
├── tests/
│   ├── test_ingestion.py
│   ├── test_cleaning.py
│   ├── test_transformations.py
    ├── run_tests.py
├── README.md
└── ...
```

### Key Directories and Files

- **`data/`**: Local CSV files for original and new turbine data.
- **`notebooks/`**: Databricks notebooks that orchestrate ingestion (`1_bronze_ingestion`), cleaning (`2_silver_cleanup`), and transformation (`3_gold_transformations`).
- **`src/`**: Python modules containing the actual PySpark job logic.
  - **`wt_ingestion.py`**: Logic for reading CSVs and writing them to Bronze tables.
  - **`wt_cleaning.py`**: Cleans and transforms data into the Silver layer.
  - **`wt_transformations.py`**: Performs advanced transformations, anomaly detection, and writes results to Gold tables.
  - **`wt_logger.py`**: Centralized logging utility.
- **`config/jobs/turbine_data_pipeline.yaml`**: Example of a Databricks Job definition that orchestrates the notebooks.
- **`tests/`**: Pytest-based unit/integration tests for each module.
- **`running_tests`**: Script to run `pytest`

---

## Getting Started

### Prerequisites

1. [Python 3.x](https://www.python.org/)
2. [PySpark](https://spark.apache.org/) installed locally, or use a Databricks cluster.
3. [pytest](https://pytest.org/) for running tests (optional but recommended).
4. A Databricks workspace (if running in the cloud).

### Setting Up the Project Locally

1. **Clone** this repository:

   ```bash
   git clone https://github.com/tomov13/wind_turbine_project.git
   cd wind_turbine_project
   ```

2. **Create/activate a virtual environment** (recommended)

3. **Install dependencies** (PySpark, sklearn, pytest, etc.)

4. **Run 0_setup_data notebook** to prepare the data location

5. **Run tests (optional)** to ensure everything is working

> **Note**: Running the entire pipeline locally may require configuring Spark sessions and paths in each script. By default, the code is optimized for Databricks’ environment.

### Running on Databricks

1. **Import notebooks** (found in `notebooks/`) into your Databricks workspace.
2. **Upload data** to DBFS. This can be done by running the 0_setup_data notebook.
3. **Create or attach a cluster** (with the necessary Spark version and node configuration).
4. **Configure and run notebooks** in sequence:
   1. **`1_bronze_ingestion`** – Ingest CSVs into Bronze Delta Tables.
   2. **`2_silver_cleanup`** – Clean the Bronze data and save to Silver.
   3. **`3_gold_transformations`** – Perform anomaly detection and advanced transformations, then save to Gold.

Alternatively, you can use the **Job** definition file (`turbine_data_pipeline.yaml`) to set up a single multi-task job in Databricks.

---

## Pipeline Details

### Bronze Layer (Ingestion)

**Module**: `src/wt_ingestion.py`  
**Notebook**: `1_bronze_ingestion`

- **`RawDataIngestor`** class handles reading CSV files containing turbine data.
  - `load_original_turbine_data()`: Reads existing CSVs, infers schema, returns a DataFrame with the correct schema.
  - `load_new_turbine_data()`: Reads new CSVs (e.g., `Location*.csv`), extracts an offset for `turbine_id` to ensure uniqueness.
  - `write_bronze()`: Writes the resulting DataFrame to a **Delta** table in the `bronze_data` database.

The main steps:

1. **Load** the CSV files from `dbfs:/FileStore/...` (or local path if running outside Databricks).
2. **Assign** or **preserve** the turbine IDs.
3. **Persist** to Bronze tables (`original_turbine_bronze`, `new_turbine_bronze`).

### Silver Layer (Cleaning and Standardization)

**Module**: `src/wt_cleaning.py`  
**Notebook**: `2_silver_cleanup`

- **`DataCleaner`** class:
  - `transform_turbine_data()`: Converts partial columns to standardized columns (e.g., scaling power to MW, converting timestamps).
  - `merge_bronze_data()`: Merges original and new data into a single DataFrame.
  - `clean_turbine_data()`: Removes invalid or negative power outputs and discards rows with missing critical fields.
  - `save_silver_table()`: Writes the cleaned dataset to the `silver_data` schema.

Steps in the **`2_silver_cleanup`** notebook:

1. **Read** the two Bronze tables.
2. **Transform** the new data (rename columns, scale power, etc.).
3. **Merge** with the original Bronze data.
4. **Clean** the merged dataset (handle missing values, negative power, etc.).
5. **Write** the result to a **Silver** Delta table (`wind_turbine_silver`).

### Gold Layer (Transformations and Analytics)

**Module**: `src/wt_transformations.py`  
**Notebook**: `3_gold_transformations`

- **`DataTransformer`** class is responsible for advanced analytics:
  1. `compute_expected_power()`: Uses a simple wind power formula to estimate theoretical power generation.
  2. `detect_zscore_anomalies()`: Flags records whose `(power_output - expected_power)` is beyond a z‑score threshold.
  3. `detect_record_anomalies()`: Uses **Isolation Forest** (sklearn) to detect outliers on `wind_speed`, `expected_power`, and `power_output`.
  4. `combine_anomalies()`: Creates a unified anomaly indicator from both z‑score and Isolation Forest.
  5. `detect_turbine_anomalies()`: Groups by turbine ID to find anomaly rates and classify each turbine as **FAULTY_SENSOR**, **REVIEW_REQUIRED**, or **NORMAL**.
  6. `apply_smart_filtering()`: Removes turbines labeled as **FAULTY_SENSOR**.
  7. `calculate_summary_statistics()`: Aggregates min/max/avg power over 24-hour windows.

Finally, the data is saved to the **Gold** layer:

- **`save_turbine_analysis()`**: Writes the final anomaly-labeled turbines or the filtered records to a `gold_data` table.
- **`save_summary_table()`**: Writes aggregated stats (e.g., daily summary) to a `gold_data` table.

---

## Jobs / Orchestration

Databricks **Jobs** can automate the pipeline. An example job definition is in:

```
config/jobs/turbine_data_pipeline.yaml
```

This file sets up a **three-task** job:

1. **Task 01 (Bronze Ingestion)**
2. **Task 02 (Silver Cleanup)**
3. **Task 03 (Gold Transformations)**

Each task references one of the corresponding notebooks in the `notebooks/` folder.

You can import this YAML into Databricks (under **Jobs** -> **Create Job** -> **'Switch to code version (YAML)'**), or replicate the tasks manually in the Databricks UI.

---

## Testing

Unit tests are found under the `tests/` directory, matching the module structure:

- `test_ingestion.py`
- `test_cleaning.py`
- `test_transformations.py`

## License

This project is for demonstration and interview purposes.

---

### Contact

For any questions or clarifications regarding this project, feel free to reach out or open an issue in the repository.
