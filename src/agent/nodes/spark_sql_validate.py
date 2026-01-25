"""Spark SQL validation node."""

import logging
import re
from typing import Any

import sqlglot
from sqlglot import exp

from src.agent.state import AgentState
from src.services.table_mapping import get_table_mapping_service

logger = logging.getLogger(__name__)


def preprocess_spark_sql(sql_content: str) -> str:
    """Preprocess Spark SQL to handle Hive variables/macros for validation purposes.
    
    Args:
        sql_content: Raw SQL content
        
    Returns:
        Processed SQL with variables substituted/removed
    """
    lines = sql_content.split('\n')
    cleaned_lines = []
    variables = {}
    
    # Regex to match: set hivevar:key=value; 
    # Supports semicolon optional, whitespace flexibility
    # Capture group 1: key
    # Capture group 2: value (until semicolon or end of line)
    hivevar_pattern = re.compile(r"^\s*set\s+hivevar:([a-zA-Z0-9_]+)\s*=\s*(.*);?", re.IGNORECASE)
    
    for line in lines:
        match = hivevar_pattern.match(line.strip())
        if match:
            key = match.group(1)
            raw_value = match.group(2).strip()
            if raw_value.endswith(';'):
                raw_value = raw_value[:-1]
                
            # If value contains complex expressions (nested ${} or function calls),
            # replace with a simple safe identifier
            if "${" in raw_value or "(" in raw_value:
                variables[key] = f"placeholder_{key}"
            else:
                variables[key] = raw_value.strip()
            
            # Skip this line in the output SQL (remove the set statement)
            continue
        
        cleaned_lines.append(line)
        
    processed_sql = "\n".join(cleaned_lines)
    
    # Replace known variables
    for key, value in variables.items():
        # Replace ${hivevar:key} and ${key}
        processed_sql = processed_sql.replace(f"${{hivevar:{key}}}", value)
        processed_sql = processed_sql.replace(f"${{{key}}}", value)
        
    # Catch-all: Replace any remaining ${...} with a dummy placeholder
    # This handles undefined variables or other macro forms to ensure parser compatibility
    # structure like table_${var}_suffix -> table_dummy_var_suffix
    processed_sql = re.sub(r'\$\{.*?\}', 'dummy_var', processed_sql)
    
    return processed_sql


def spark_sql_validate(state: AgentState) -> dict[str, Any]:
    """Validate the input Spark SQL syntax and extract table mappings.
    
    Args:
        state: Current agent state containing spark_sql.
        
    Returns:
        Updated state with spark_valid, spark_error, source_tables, and table_mapping.
    """
    logger.info("=" * 60)
    logger.info("[Node: spark_sql_validate] Starting Spark SQL validation", extra={"type": "status", "step": "spark_sql_validate", "status": "loading"})
    
    spark_sql = state["spark_sql"].strip()
    
    # Remove markdown blocks
    if spark_sql.startswith("```"):
        lines = spark_sql.split("\n")
        # Remove first line (```sql or ```)
        # Check if last line is ```
        if lines[-1].strip() == "```":
            spark_sql = "\n".join(lines[1:-1]).strip()
        else:
            # Handle case where only start block is present (unlikely but possible)
            spark_sql = "\n".join(lines[1:]).strip()
            
    # [Preprocess] Handle Hive variables (set hivevar:...) that sqlglot doesn't understand
    try:
        spark_sql = preprocess_spark_sql(spark_sql)
    except Exception as e:
        logger.warning(f"[Node: spark_sql_validate] Preprocessing failed: {e}, proceeding with original SQL")

    mapping_service = get_table_mapping_service()
    
    source_tables = set()
    table_map = {}
    
    try:
        parsed = sqlglot.parse(spark_sql, read="spark")
        
        for expression in parsed:
            cte_aliases = {cte.alias for cte in expression.find_all(exp.CTE)}
            for table in expression.find_all(exp.Table):
                table_name = table.name
                
                if table.db:
                    table_name = f"{table.db}.{table_name}"
                elif table_name in cte_aliases:
                    continue
                
                source_tables.add(table_name)
                
                bq_table = mapping_service.get_bigquery_table(table_name)
                if bq_table:
                    table_map[table_name] = bq_table
        
        source_tables_list = sorted(list(source_tables))
        
        logger.info("[Node: spark_sql_validate] ✓ Spark SQL is valid", extra={"type": "status", "step": "spark_sql_validate", "status": "success"})
        logger.info(f"[Node: spark_sql_validate] Extracted tables: {source_tables_list}")
        logger.info(f"[Node: spark_sql_validate] Mapped tables: {table_map}")
        
        return {
            "spark_valid": True,
            "spark_error": None,
            "source_tables": source_tables_list,
            "table_mapping": table_map,
        }
        
    except Exception as e:
        error_msg = str(e)
        logger.warning(f"[Node: spark_sql_validate] ✗ Spark SQL is invalid: {error_msg}", extra={"type": "status", "step": "spark_sql_validate", "status": "error"})
        
        return {
            "spark_valid": False,
            "spark_error": f"Spark SQL validation failed: {error_msg}",
            "source_tables": [],
            "table_mapping": {},
        }
