"""SQL execution node."""

import logging
from typing import Any

from src.agent.state import AgentState
from src.services.bigquery import BigQueryService

# Configure logger
logger = logging.getLogger(__name__)


def execute_node(state: AgentState) -> dict[str, Any]:
    """Execute the validated BigQuery SQL.
    
    Args:
        state: Current agent state.
        
    Returns:
        State update with execution results.
    """
    logger.info("============================================================")
    logger.info("[Node: execute] Starting BigQuery SQL execution", extra={"type": "status", "step": "execute", "status": "loading"})
    
    bq_service = BigQueryService()
    execution_result = bq_service.execute_query(state["bigquery_sql"])
    bq_service.close()
    
    if execution_result.success:
        logger.info("[Node: execute] ✓ SQL execution successful", extra={"type": "status", "step": "execute", "status": "success", "data": {"target_table": execution_result.target_table}})
        logger.info(f"Target Table: {execution_result.target_table}")
        logger.info(f"Result: {execution_result.result}")
    else:
        logger.error(f"[Node: execute] ✗ SQL execution failed: {execution_result.error_message}", extra={"type": "status", "step": "execute", "status": "error"})
    
    return {
        "execution_success": execution_result.success,
        "execution_result": execution_result.result,
        "execution_target_table": execution_result.target_table,
        "execution_error": execution_result.error_message,
    }
