"""SQL fix node."""

import logging
import time
from typing import Any

from src.agent.state import AgentState
from src.prompts.templates import FIX_BIGQUERY_PROMPT
from src.services.llm import get_llm
from src.services.table_mapping import get_table_mapping_service
from src.services.utils import get_content_text, accumulate_token_usage
from src.services.usage_logger import UsageLogger

logger = logging.getLogger(__name__)


def bigquery_error_fix(state: AgentState) -> dict[str, Any]:
    """Fix BigQuery SQL based on validation error.
    
    Args:
        state: Current agent state containing bigquery_sql and validation_error.
        
    Returns:
        Updated state with corrected bigquery_sql and incremented retry_count.
    """
    retry_count = state["retry_count"] + 1
    
    retry_count = state["retry_count"] + 1
    
    # Check for execution error first, then LLM check error, then validation error
    if state.get("validation_success") and state.get("execution_error"):
        error_message = state["execution_error"]
        error_type = "execution"
    elif state.get("llm_check_error"): # Handle LLM check error
        error_message = state["llm_check_error"]
        error_type = "llm_check"
    else:
        error_message = state.get("validation_error")
        error_type = "validation"
    
    logger.info("=" * 60)
    logger.info(f"[Node: bigquery_error_fix] Starting SQL fix (retry {retry_count})", extra={"type": "status", "step": "bigquery_error_fix", "status": "loading", "attempt": retry_count})

    logger.debug(f"[Node: bigquery_error_fix] SQL to fix:\n{state['bigquery_sql']}")
    
    llm = get_llm("bigquery_error_fix")
    
    # Get table mapping information
    table_mapping_service = get_table_mapping_service()
    
    # Format conversion history
    history_str = ""
    for entry in state.get("conversion_history", []):
        history_str += f"\nAttempt {entry.attempt}:\n"
        history_str += f"SQL: {entry.bigquery_sql}\n"
        if entry.error:
            history_str += f"Error: {entry.error}\n"
    
    if not history_str:
        history_str = "No previous attempts."
    
    prompt = FIX_BIGQUERY_PROMPT.format(
        spark_sql=state["spark_sql"],
        bigquery_sql=state["bigquery_sql"],
        error_message=error_message,
        table_ddls=state.get("table_ddls", "No DDLs available."),
        conversion_history=history_str,
    )
    
    start_time = time.time()
    response = llm.invoke(prompt)
    end_time = time.time()
    latency_ms = int((end_time - start_time) * 1000)
    
    token_usage = state.get("token_usage", {})
    usage = response.response_metadata.get("token_usage") or response.usage_metadata
    model_name = getattr(llm, "model", getattr(llm, "model_name", "unknown"))
    token_usage = accumulate_token_usage(token_usage, usage, node_name="bigquery_error_fix", model_name=model_name)
    
    # Log usage to BQ
    UsageLogger().log_usage(
        agent_session_id=state.get("agent_session_id", "unknown"),
        node_name="bigquery_error_fix",
        model_name=model_name,
        usage=usage,
        status="SUCCESS",
        latency_ms=latency_ms,
        run_id=state.get("run_id")
    )
    
    # Remove markdown code blocks
    fixed_sql = get_content_text(response.content).strip()
    if fixed_sql.startswith("```"):
        lines = fixed_sql.split("\n")
        fixed_sql = "\n".join(lines[1:-1]).strip()
    
    # Apply table name mapping
    fixed_sql = table_mapping_service.replace_table_names(fixed_sql)
    
    logger.debug(f"[Node: bigquery_error_fix] Fixed BigQuery SQL:\n{fixed_sql}")
    
    logger.info(f"[Node: bigquery_error_fix] SQL fixed successfully", extra={"type": "status", "step": "bigquery_error_fix", "status": "success"})

    return {
        "bigquery_sql": fixed_sql,
        "retry_count": retry_count,
        "token_usage": token_usage,
    }
