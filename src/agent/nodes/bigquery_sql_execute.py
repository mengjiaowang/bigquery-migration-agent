"""SQL execution node."""

import logging
import os
from typing import Any

import sqlglot
from sqlglot import exp

from src.agent.state import AgentState
from src.services.bigquery import BigQueryService

# Configure logger
logger = logging.getLogger(__name__)


def bigquery_sql_execute(state: AgentState) -> dict[str, Any]:
    """Execute the validated BigQuery SQL.
    
    Args:
        state: Current agent state.
        
    Returns:
        State update with execution results.
    """
    logger.info("============================================================")
    logger.info("[Node: bigquery_sql_execute] Starting BigQuery SQL execution", extra={"type": "status", "step": "bigquery_sql_execute", "status": "loading"})
    
    # ------------------------------------------------------------------
    # SQL Safety Check
    # ------------------------------------------------------------------
    allowed_prefix = os.getenv("DATA_VERIFICATION_ALLOWED_DATASET")
    if not allowed_prefix:
        raise ValueError("DATA_VERIFICATION_ALLOWED_DATASET environment variable is not set")
    bq_sql = state["bigquery_sql"]
    
    try:
        parsed = sqlglot.parse_one(bq_sql, read="bigquery")
    except Exception as e:
        logger.error(f"[Node: bigquery_sql_execute] ✗ Failed to parse SQL for safety check: {e}", extra={"type": "status", "step": "bigquery_sql_execute", "status": "error"})
        return {
            "execution_success": False,
            "execution_result": None,
            "execution_target_table": None,
            "execution_error": f"SQL Safety Check Failed: Could not parse SQL. Error: {str(e)}",
        }

    # Check for modification statements
    modification_types = (exp.Insert, exp.Update, exp.Delete, exp.Merge, exp.Create, exp.Drop)
    if isinstance(parsed, modification_types):
        target_table = None
        if isinstance(parsed, exp.Insert):
            target_table = parsed.this
        elif isinstance(parsed, exp.Update):
            target_table = parsed.this
        elif isinstance(parsed, exp.Delete):
            target_table = parsed.this
        elif isinstance(parsed, exp.Merge):
            target_table = parsed.this
        elif isinstance(parsed, exp.Create):
            target_table = parsed.this
        elif isinstance(parsed, exp.Drop):
            target_table = parsed.this
            
        if not target_table:
            logger.warning("[Node: bigquery_sql_execute] ? Could not determine target table for modification statement", extra={"type": "status", "step": "bigquery_sql_execute", "status": "warning"})
            return {
                "execution_success": False,
                "execution_result": None,
                "execution_target_table": None,
                "execution_error": "SQL Safety Check Failed: Detected data modification but could not identify target table.",
            }
            
        table_name = target_table.sql(dialect="bigquery")
        clean_table_name = table_name.replace("`", "").strip()
        
        logger.info(f"[Node: bigquery_sql_execute] Safety Check: Verifying target table '{clean_table_name}'")
        
        if not clean_table_name.startswith(allowed_prefix):
            error_msg = f"SQL Safety Check Failed: Modification not allowed on table '{clean_table_name}'. Target must be in '{allowed_prefix}'."
            logger.error(f"[Node: bigquery_sql_execute] ✗ {error_msg}", extra={"type": "status", "step": "bigquery_sql_execute", "status": "error"})
            return {
                "execution_success": False,
                "execution_result": None,
                "execution_target_table": None,
                "execution_error": error_msg,
            }
        logger.info(f"[Node: bigquery_sql_execute] ✓ Safety Check Passed: Target '{clean_table_name}' is allowed", extra={"type": "status", "step": "bigquery_sql_execute", "status": "success"})

    # ------------------------------------------------------------------
    # End Safety Check
    # ------------------------------------------------------------------

    bq_service = BigQueryService()
    execution_result = bq_service.execute_query(state["bigquery_sql"])
    bq_service.close()
    
    if execution_result.success:
        logger.info("[Node: bigquery_sql_execute] ✓ SQL execution successful", extra={"type": "status", "step": "bigquery_sql_execute", "status": "success", "data": {"target_table": execution_result.target_table}})
        logger.info(f"Target Table: {execution_result.target_table}")
        logger.info(f"Result: {execution_result.result}")
    else:
        logger.error(f"[Node: bigquery_sql_execute] ✗ SQL execution failed: {execution_result.error_message}", extra={"type": "status", "step": "bigquery_sql_execute", "status": "error"})
    
    return {
        "execution_success": execution_result.success,
        "execution_result": execution_result.result,
        "execution_target_table": execution_result.target_table,
        "execution_error": execution_result.error_message,
    }
