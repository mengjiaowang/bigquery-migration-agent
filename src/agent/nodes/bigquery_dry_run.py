"""BigQuery SQL validation node (Dry Run)."""

import logging
from typing import Any

from src.agent.state import AgentState
from src.schemas.models import ConversionHistory
from src.services.validation import validate_bigquery_sql

# Configure logger
logger = logging.getLogger(__name__)


def bigquery_dry_run(state: AgentState) -> dict[str, Any]:
    """Validate BigQuery SQL using BigQuery dry run.
    
    Args:
        state: Current agent state containing bigquery_sql.
        
    Returns:
        Updated state with validation_success, validation_error, and updated conversion_history.
    """
    attempt = len(state.get("conversion_history", [])) + 1
    
    logger.info("=" * 60)
    logger.info(f"[Node: bigquery_dry_run] Starting BigQuery SQL validation (attempt {attempt})", extra={"type": "status", "step": "bigquery_dry_run", "status": "loading", "attempt": attempt})
    logger.debug(f"BigQuery SQL to validate:\n{state['bigquery_sql']}")
    
    result = validate_bigquery_sql(state["bigquery_sql"])
    
    if result.success:
        logger.info(f"[Node: bigquery_dry_run] ✓ BigQuery SQL validation passed", extra={"type": "status", "step": "bigquery_dry_run", "status": "success"})
    else:
        logger.error("=" * 60)
        logger.error(f"[Node: bigquery_dry_run] ✗ BigQuery SQL validation FAILED (attempt {attempt})", extra={"type": "status", "step": "bigquery_dry_run", "status": "error"})
        logger.error(f"[Node: bigquery_dry_run] Error Details:")
        logger.error("-" * 40)
        # Print complete error message, line by line
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
        "validation_mode": "dry_run",
        "conversion_history": history,
    }
