"""BigQuery SQL validation node."""

import logging
from typing import Any

from src.agent.state import AgentState
from src.schemas.models import ConversionHistory
from src.services.validation import validate_bigquery_sql

# Configure logger
logger = logging.getLogger(__name__)


def validate_node(state: AgentState) -> dict[str, Any]:
    """Validate BigQuery SQL using configured validation mode.
    
    The validation mode is controlled by BQ_VALIDATION_MODE environment variable:
    - "dry_run": Use BigQuery API dry run (default)
    - "llm": Use LLM prompt-based validation
    
    Args:
        state: Current agent state containing bigquery_sql.
        
    Returns:
        Updated state with validation_success, validation_error, and updated conversion_history.
    """
    attempt = len(state.get("conversion_history", [])) + 1
    
    logger.info("=" * 60)
    logger.info(f"[Node: validate] Starting BigQuery SQL validation (attempt {attempt})")
    logger.info(f"BigQuery SQL to validate:\n{state['bigquery_sql']}")
    
    result = validate_bigquery_sql(state["bigquery_sql"])
    
    logger.info(f"[Node: validate] Validation mode: {result.validation_mode}")
    
    if result.success:
        logger.info(f"[Node: validate] ✓ BigQuery SQL validation passed")
    else:
        logger.error("=" * 60)
        logger.error(f"[Node: validate] ✗ BigQuery SQL validation FAILED (attempt {attempt})")
        logger.error(f"[Node: validate] Error Details:")
        logger.error("-" * 40)
        # 打印完整的错误信息，每行都打印
        for line in str(result.error_message).split('\n'):
            logger.error(f"  {line}")
        logger.error("-" * 40)
    
    # Update conversion history
    history = list(state.get("conversion_history", []))
    history.append(
        ConversionHistory(
            attempt=attempt,
            bigquery_sql=state["bigquery_sql"],
            error=result.error_message if not result.success else None,
        )
    )
    
    return {
        "validation_success": result.success,
        "validation_error": result.error_message,
        "validation_mode": result.validation_mode,
        "conversion_history": history,
    }
