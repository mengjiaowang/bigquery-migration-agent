import logging
import re
from typing import Any

import sqlglot
from sqlglot import exp

from src.agent.state import AgentState
from src.services.table_mapping import get_table_mapping_service

logger = logging.getLogger(__name__)


def preprocess_spark_sql(sql_content: str) -> str:
    """Pre-process SQL to handle Hive variables/macros for parser compatibility."""
    lines = sql_content.split('\n')
    cleaned_lines = []
    variables = {}

    # Regex for: set hivevar:key=value;
    # Handles semicolon, various spacing, and complex values
    hivevar_pattern = re.compile(r"^\s*set\s+hivevar:([a-zA-Z0-9_]+)\s*=\s*(.*?)\s*;?\s*$", re.IGNORECASE)

    for line in lines:
        match = hivevar_pattern.match(line.strip())
        if match:
            key = match.group(1)
            value = match.group(2).strip().rstrip(';')
            # If value is a complex expression (contains ${ or function calls),
            # provide a safe placeholder to pass syntax validation.
            if "${" in value or "(" in value:
                variables[key] = f"placeholder_{key}"
            else:
                variables[key] = value
            # Remove the 'set' line as sqlglot's Spark dialect often chokes on 'hivevar:'
            continue

        cleaned_lines.append(line)

    processed_sql = "\n".join(cleaned_lines)

    # Substitute known variables
    for key, value in variables.items():
        # Replace both ${hivevar:key} and ${key}
        processed_sql = processed_sql.replace(f"${{hivevar:{key}}}", value)
        processed_sql = processed_sql.replace(f"${{{key}}}", value)

    # Catch-all: Replace all remaining ${...} patterns
    # Ensures that table names like table_${date} become table_dummy_var
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
            
    # Pre-process SQL to handle hivevars and macros
    spark_sql = preprocess_spark_sql(spark_sql)
    
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
