"""SQL fix node."""

import logging
from typing import Any

from src.agent.state import AgentState
from src.prompts.templates import FIX_BIGQUERY_PROMPT
from src.services.llm import get_llm
from src.services.table_mapping import get_table_mapping_service
from src.services.utils import get_content_text

# Configure logger
logger = logging.getLogger(__name__)


def fix_node(state: AgentState) -> dict[str, Any]:
    """Fix BigQuery SQL based on validation error.
    
    Args:
        state: Current agent state containing bigquery_sql and validation_error.
        
    Returns:
        Updated state with corrected bigquery_sql and incremented retry_count.
    """
    retry_count = state["retry_count"] + 1
    
    # Determine which error to fix
    # If validation passed but we are here, it must be an execution error
    if state.get("validation_success") and state.get("execution_error"):
        error_message = state["execution_error"]
        error_type = "execution"
    else:
        error_message = state.get("validation_error")
        error_type = "validation"
    
    logger.info("=" * 60)
    logger.info(f"[Node: fix] Starting SQL fix (retry {retry_count})", extra={"type": "status", "step": "fix", "status": "loading", "attempt": retry_count})
    logger.info(f"[Node: fix] Previous error ({error_type}): {error_message}")
    logger.debug(f"[Node: fix] SQL to fix:\n{state['bigquery_sql']}")
    
    llm = get_llm()
    
    # Get table mapping information
    table_mapping_service = get_table_mapping_service()
    table_mapping_info = table_mapping_service.get_mapping_info_for_prompt()
    
    # Format conversion history for the prompt
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
    
    response = llm.invoke(prompt)
    
    # Clean up response - remove markdown code blocks if present
    fixed_sql = get_content_text(response.content).strip()
    if fixed_sql.startswith("```"):
        lines = fixed_sql.split("\n")
        fixed_sql = "\n".join(lines[1:-1]).strip()
    
    # Apply table name replacement as a safety net
    # (in case the LLM didn't apply all mappings correctly)
    fixed_sql = table_mapping_service.replace_table_names(fixed_sql)
    
    logger.debug(f"[Node: fix] Fixed BigQuery SQL:\n{fixed_sql}")
    
    return {
        "bigquery_sql": fixed_sql,
        "retry_count": retry_count,
    }
