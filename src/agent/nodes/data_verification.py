"""Data verification node."""

import logging
from typing import Any

from src.agent.state import AgentState
from src.services.bigquery import BigQueryService

# Configure logger
logger = logging.getLogger(__name__)


def data_verification_node(state: AgentState) -> dict[str, Any]:
    """Verify data in the target table after execution.
    
    Args:
        state: Current agent state.
        
    Returns:
        State update with verification results.
    """
    logger.info("============================================================")
    logger.info("[Node: verify] Starting Data Verification", extra={"type": "status", "step": "data_verification", "status": "loading"})
    
    target_table = state.get("execution_target_table")
    if not target_table:
        logger.warning("[Node: verify] No target table to verify. Skipping.")
        return {
            "data_verification_success": False,
            "data_verification_error": "No target table found.",
        }
        
    bq_service = BigQueryService()
    try:
        # Simple row count verification
        check_sql = f"SELECT count(*) as cnt FROM `{target_table}`"
        logger.info(f"[Node: verify] Running check: {check_sql}")
        
        result = bq_service.execute_query(check_sql)
        
        if result.success and isinstance(result.result, list) and len(result.result) > 0:
            count = result.result[0].get("cnt", 0)
            logger.info(f"[Node: verify] ✓ Verification successful. Row count: {count}", extra={"type": "status", "step": "data_verification", "status": "success"})
            return {
                "data_verification_success": True,
                "data_verification_result": {"row_count": count},
                "data_verification_error": None,
            }
        else:
            error_msg = result.error_message or "Failed to get row count"
            logger.error(f"[Node: verify] ✗ Verification failed: {error_msg}", extra={"type": "status", "step": "data_verification", "status": "error"})
            return {
                "data_verification_success": False,
                "data_verification_result": None,
                "data_verification_error": error_msg,
            }
            
    except Exception as e:
        logger.error(f"[Node: verify] ✗ Verification error: {str(e)}")
        return {
            "data_verification_success": False,
            "data_verification_result": None,
            "data_verification_error": str(e),
        }
    finally:
        bq_service.close()
