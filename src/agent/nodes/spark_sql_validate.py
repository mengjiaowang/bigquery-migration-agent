"""Spark SQL validation node."""

import logging
from typing import Any

import sqlglot
from sqlglot import exp

from src.agent.state import AgentState
from src.services.table_mapping import get_table_mapping_service

# Configure logger
logger = logging.getLogger(__name__)


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
    
    # Clean up markdown code blocks if present
    if spark_sql.startswith("```"):
        lines = spark_sql.split("\n")
        # Remove first line (```sql or ```)
        # Check if last line is ```
        if lines[-1].strip() == "```":
            spark_sql = "\n".join(lines[1:-1]).strip()
        else:
            # Handle case where only start block is present (unlikely but possible)
            spark_sql = "\n".join(lines[1:]).strip()
            
    mapping_service = get_table_mapping_service()
    
    source_tables = set()
    table_map = {}
    
    try:
        # Parse SQL using sqlglot (read="spark")
        # parse returns a list of expressions, handling multi-statement SQL
        parsed = sqlglot.parse(spark_sql, read="spark")
        
        for expression in parsed:
            # Collect CTE aliases to exclude them from source tables
            cte_aliases = {cte.alias for cte in expression.find_all(exp.CTE)}
            
            # Extract all tables from the expression
            for table in expression.find_all(exp.Table):
                table_name = table.name
                
                # Handle db.table format if present
                if table.db:
                    table_name = f"{table.db}.{table_name}"
                elif table_name in cte_aliases:
                    # Skip CTE references
                    continue
                
                source_tables.add(table_name)
                
                # Get BigQuery mapping
                bq_table = mapping_service.get_bigquery_table(table_name)
                if bq_table:
                    table_map[table_name] = bq_table
        
        # Convert to sorted list for determinism
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
