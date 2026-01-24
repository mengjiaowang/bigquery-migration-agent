# Utility Scripts

This directory contains utility scripts for the BigQuery Migration Tool.

## `extract_tables.py`

A tool to extract **Input** (read) and **Output** (write) tables from Spark SQL queries and verify their existence in BigQuery.

### Purpose

1.  **Parse Spark SQL**: Identifies all tables referenced in the query.
2.  **Categorize**: Distinguishes between Input tables (read sources) and Output tables (write targets).
3.  **Map to BigQuery**: Applies mapping rules to resolve Spark table names to BigQuery table names.
4.  **Verify Existence**: Checks if the mapped tables exist in the specified BigQuery datasets (Input) or output paths (Output).
5.  **Check Data**: For Input tables, it also checks metadata to report if the table contains data or is empty.

### Prerequisites

#### 1. Environment Variables

The script relies on the `.env` file in the project root. Ensure the following variables are set:

```ini
# GCP Project ID for BigQuery Client (Authentication & Quota)
GOOGLE_CLOUD_PROJECT=your-gcp-project-id

# Dataset to check for Input tables (Case-insensitive matching)
INPUT_DATASET=trip-htl-bi-dbprj.htl_bi_temp

# Dataset to map Output tables to
OUTPUT_DATASET=trip-htl-bi-dbprj.tool_results
```

#### 2. Dependencies

Ensure the virtual environment dependencies are installed:

```bash
pip install -r requirements.txt
```

(Specifically requires `sqlglot`, `google-cloud-bigquery`, `python-dotenv`, `google-auth`)

### Usage

Run the script from the project root:

```bash
./venv/bin/python scripts/extract_tables.py
```

#### Interactive Mode

The script runs in a continuous loop.

1.  **Paste** your Spark SQL block.
2.  Type `END` on a new line to process the block.
3.  The script will print the analysis and wait for the next input.
4.  Press `Ctrl+C` to exit.

#### Example

**Input:**

```sql
INSERT INTO db.target_table
SELECT * FROM db.source_table
UNION ALL
SELECT * FROM db.empty_table;
END
```

**Output:**

```text
INPUT TABLES REFERENCED:
Fetching table list from trip-htl-bi-dbprj.htl_bi_temp...
  [Spark] db.source_table                -> [BigQuery] trip-htl-bi-dbprj.htl_bi_temp.db_source_table (FOUND, Has Data/Row: 1000)
  [Spark] db.empty_table                 -> [BigQuery] trip-htl-bi-dbprj.htl_bi_temp.db_empty_table (FOUND, Empty)

OUTPUT TABLES TARGETED:
Fetching table list from trip-htl-bi-dbprj.tool_results...
  [Spark] db.target_table                -> [BigQuery] trip-htl-bi-dbprj.tool_results.db_target_table (NOT FOUND)

CSV FORMAT (Found Inputs Only):
db.source_table,trip-htl-bi-dbprj.htl_bi_temp.db_source_table
db.empty_table,trip-htl-bi-dbprj.htl_bi_temp.db_empty_table
```
