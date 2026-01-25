#!/usr/bin/env python3
import sys
import logging
import os
import re
from typing import Set, Dict, Optional, Tuple
from dotenv import load_dotenv
import google.auth

# Load environment variables
load_dotenv()

try:
    import sqlglot
    from sqlglot import exp
    from google.cloud import bigquery
except ImportError as e:
    print(f"Error: Missing required packages. {e}")
    print("Please ensure sqlglot and google-cloud-bigquery are installed.")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    stream=sys.stderr, 
    level=logging.WARNING, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

INPUT_DATASET = os.getenv("INPUT_DATASET", "trip-htl-bi-dbprj.htl_bi_temp")
OUTPUT_DATASET = os.getenv("OUTPUT_DATASET", "trip-htl-bi-dbprj.tool_results")

class BQMetadataService:
    def __init__(self):
        project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
        
        # Explicitly set quota_project_id to avoid ADC mismatch errors.
        credentials, project = google.auth.default(quota_project_id=project_id)
        
        self.client = bigquery.Client(project=project_id, credentials=credentials)
        # Cache stores: lowercase_name -> (real_name, row_count)
        self.input_tables_cache: Optional[Dict[str, Tuple[str, Optional[int]]]] = None
        self.output_tables_cache: Optional[Dict[str, Tuple[str, Optional[int]]]] = None

    def get_input_tables_lookup(self) -> Dict[str, Tuple[str, Optional[int]]]:
        """Fetches all table names and row counts from the input dataset."""
        if self.input_tables_cache is not None:
            return self.input_tables_cache
        
        print(f"Fetching table list from {INPUT_DATASET}...", file=sys.stderr)
        return self._fetch_tables(INPUT_DATASET, is_input=True)

    def get_output_tables_lookup(self) -> Dict[str, Tuple[str, Optional[int]]]:
        """Fetches all table names from the output dataset."""
        if self.output_tables_cache is not None:
            return self.output_tables_cache
        
        print(f"Fetching table list from {OUTPUT_DATASET}...", file=sys.stderr)
        return self._fetch_tables(OUTPUT_DATASET, is_input=False)

    def _fetch_tables(self, dataset: str, is_input: bool) -> Dict[str, Tuple[str, Optional[int]]]:
        # Fetch row counts from __TABLES__
        tables: Dict[str, Tuple[str, Optional[int]]] = {}
        try:
            query = f"SELECT table_id, row_count FROM `{dataset}.__TABLES__`"
            results = self.client.query(query).result()
            for row in results:
                real_name = row.table_id
                tables[real_name.lower()] = (real_name, row.row_count)
        except Exception as e:
            logger.warning(f"Failed to fetch from __TABLES__ for {dataset} ({e}). Falling back to INFORMATION_SCHEMA.")
            # Fallback to INFORMATION_SCHEMA for views or access issues
            try:
                query = f"SELECT table_name FROM `{dataset}.INFORMATION_SCHEMA.TABLES`"
                results = self.client.query(query).result()
                for row in results:
                    real_name = row.table_name
                    lower_name = real_name.lower()
                    if lower_name not in tables:
                        tables[lower_name] = (real_name, None)
            except Exception as e2:
                 logger.error(f"Failed to fetch tables from INFORMATION_SCHEMA ({dataset}): {e2}")

        if is_input:
            self.input_tables_cache = tables
        else:
            self.output_tables_cache = tables
        return tables

    def resolve_input_table(self, spark_table_name: str) -> Tuple[str, bool, Optional[int]]:
        """
        Resolves a Spark table name to a BigQuery table name.
        Returns: (bq_table_name, exists, row_count)
        """
        bq_table_short = self._spark_to_bq_short(spark_table_name)
        lookup = self.get_input_tables_lookup()
        
        normalized_name = bq_table_short.lower()
        if normalized_name in lookup:
            real_name, row_count = lookup[normalized_name]
            exists = True
            bq_table_short = real_name # Canonicalize name
        else:
            exists = False
            row_count = None
        
        full_bq_name = f"{INPUT_DATASET}.{bq_table_short}"
        return full_bq_name, exists, row_count

    def resolve_output_table(self, spark_table_name: str) -> Tuple[str, bool]:
        """Resolves output table name and checks existence."""
        bq_table_short = self._spark_to_bq_short(spark_table_name)
        lookup = self.get_output_tables_lookup()
        
        normalized_name = bq_table_short.lower()
        if normalized_name in lookup:
            real_name, _ = lookup[normalized_name]
            exists = True
            bq_table_short = real_name # Use real name from BQ
        else:
            exists = False
        
        full_bq_name = f"{OUTPUT_DATASET}.{bq_table_short}"
        return full_bq_name, exists

    def _spark_to_bq_short(self, spark_table_name: str) -> str:
        parts = spark_table_name.split('.')
        if len(parts) >= 2:
            return "_".join(parts)
        return spark_table_name


import re

def clean_spark_sql(spark_sql: str) -> str:
    """
    Clean Spark SQL for parsing:
    1. Remove markdown blocks.
    2. Strip outer quotes (e.g. from CSV copy-paste) and unescape inner quotes.
    3. Replace ${...} variables with placeholders for valid parsing.
    """
    spark_sql = spark_sql.strip()
    
    # 1. Remove markdown blocks
    if spark_sql.startswith("```"):
        lines = spark_sql.split("\n")
        if lines[-1].strip() == "```":
            spark_sql = "\n".join(lines[1:-1]).strip()
        else:
            spark_sql = "\n".join(lines[1:]).strip()

    # 2. Strip outer quotes if pasted as string literal and unescape
    if len(spark_sql) > 2 and spark_sql.startswith('"') and spark_sql.endswith('"'):
        inner = spark_sql[1:-1]
        spark_sql = inner.replace('""', '"').strip()

    # 3. Replace ${...} variables to avoid parsing errors in identifiers
    spark_sql = re.sub(r'\$\{([^}]+)\}', r'_VAR_', spark_sql)
    
    return spark_sql

def get_full_table_name(table: exp.Table) -> str:
    """Construct full table name from sqlglot Table expression."""
    table_name = table.name
    if table.db:
        table_name = f"{table.db}.{table_name}"
    if table.catalog:
        table_name = f"{table.catalog}.{table_name}"
    return table_name

def process_sql(spark_sql: str, bq_service: BQMetadataService):
    cleaned_sql = clean_spark_sql(spark_sql)
    if not cleaned_sql:
        return

    try:
        parsed = sqlglot.parse(cleaned_sql, read="spark")
    except Exception as e:
        print(f"Error parsing SQL: {e}")
        return

    all_inputs = set()
    all_outputs = set()

    for expression in parsed:
        cte_aliases = {cte.alias for cte in expression.find_all(exp.CTE)}
        output_nodes = set()
        
        # Identify modifications
        if isinstance(expression, exp.Insert):
            target = expression.this
            if isinstance(target, exp.Table): output_nodes.add(target)
            elif isinstance(target, exp.Schema) and isinstance(target.this, exp.Table): output_nodes.add(target.this)
        elif isinstance(expression, exp.Create):
            target = expression.this
            if isinstance(target, exp.Table): output_nodes.add(target)
            elif isinstance(target, exp.Schema) and isinstance(target.this, exp.Table): output_nodes.add(target.this)
        elif isinstance(expression, exp.Merge):
            target = expression.this
            if isinstance(target, exp.Table): output_nodes.add(target)
        elif isinstance(expression, (exp.Update, exp.Delete)):
             if isinstance(expression.this, exp.Table): output_nodes.add(expression.this)

        for table in expression.find_all(exp.Table):
            table_name = get_full_table_name(table)
            if table_name in cte_aliases: continue
            
            if table in output_nodes:
                all_outputs.add(table_name)
            else:
                all_inputs.add(table_name)

    print("-" * 60)
    print("INPUT TABLES REFERENCED:")
    if all_inputs:
        for t in sorted(all_inputs):
            bq_name, exists, row_count = bq_service.resolve_input_table(t)
            
            status_parts = []
            if exists:
                status_parts.append("FOUND")
                if row_count is not None:
                    if row_count > 0:
                        status_parts.append(f"Has Data/Row:{row_count}")
                    else:
                        status_parts.append("Empty")
                else:
                    # View or unknown row count
                    status_parts.append("Unknown Data")
            else:
                status_parts.append("NOT FOUND")
                
            status_str = ", ".join(status_parts)
            print(f"  [Spark] {t:<30} -> [BigQuery] {bq_name} ({status_str})")
    else:
        print("  (None)")

    print("-" * 60)
    print("OUTPUT TABLES TARGETED:")
    if all_outputs:
        for t in sorted(all_outputs):
            bq_name, exists = bq_service.resolve_output_table(t)
            status = "FOUND" if exists else "NOT FOUND"
            print(f"  [Spark] {t:<30} -> [BigQuery] {bq_name} ({status})")
    else:
        print("  (None)")
    print("-" * 60)

    # Print CSV format for found tables
    found_tables = []
    
    # Inputs
    for t in sorted(all_inputs):
        bq_name, exists, _ = bq_service.resolve_input_table(t)
        if exists:
            found_tables.append(f"{t},{bq_name}")

    # Outputs
    for t in sorted(all_outputs):
        bq_name, exists = bq_service.resolve_output_table(t)
        if exists:
            found_tables.append(f"{t},{bq_name}")
    
    if found_tables:
        print("CSV FORMAT (Found Tables):")
        for line in found_tables:
            print(line)
        print("-" * 60)

def main():
    print("=" * 60)
    print("Spark SQL Table Extractor & BQ Mapper")
    print("Interactive Mode:")
    print("1. Paste your Spark SQL.")
    print("2. Press Ctrl+D (EOF) to RUN analysis.")
    print("   (The script will auto-restart for the next query)")
    print("3. Press Ctrl+C to EXIT.")
    print("=" * 60)

    bq_service = BQMetadataService()

    try:
        # Read all input until EOF (Ctrl+D)
        print("\nPaste SQL below. Press Ctrl+D to run:", flush=True)
        spark_sql = sys.stdin.read()
        
        if spark_sql.strip():
            print("\nProcessing...", flush=True)
            process_sql(spark_sql, bq_service)
        else:
            print("\nNo input received.")
            
        print("\nRestarting for next query...", flush=True)
        # Restart the script to reset stdin for the next loop
        python = sys.executable
        os.execl(python, python, *sys.argv)
            
    except KeyboardInterrupt:
        print("\nExiting...", flush=True)
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
