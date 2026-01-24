"""LLM SQL check node."""

import json
import logging
from typing import Any

from src.agent.state import AgentState
from src.prompts.templates import LLM_SQL_CHECK_PROMPT
from src.services.llm import get_llm
from src.services.table_mapping import get_table_mapping_service
from src.services.utils import get_content_text

# Configure logger
logger = logging.getLogger(__name__)


def llm_sql_check(state: AgentState) -> dict[str, Any]:
    """Validate BigQuery SQL using LLM before dry run.
    
    Args:
        state: Current agent state containing bigquery_sql.
        
    Returns:
        Updated state with llm_check_success and error message if failed.
    """
    logger.info("=" * 60)
    logger.info("[Node: llm_sql_check] Starting LLM SQL check", extra={"type": "status", "step": "llm_sql_check", "status": "loading"})
    
    spark_sql = state["spark_sql"]
    bigquery_sql = state["bigquery_sql"]
    
    # Get table mapping information
    table_mapping_service = get_table_mapping_service()
    
    # Get DDLs
    table_ddls = state.get("table_ddls", "No DDLs available.")
    
    prompt = LLM_SQL_CHECK_PROMPT.format(
        spark_sql=spark_sql,
        bigquery_sql=bigquery_sql,
        table_ddls=table_ddls,
    )
    
    llm = get_llm("llm_sql_check")
    response = llm.invoke(prompt)
    
    try:
        # Clean up response - remove markdown code blocks if present
        response_text = get_content_text(response.content).strip()
        if response_text.startswith("```"):
            lines = response_text.split("\n")
            # Remove first line (```json or ```)
            # Check if last line is ```
            if lines[-1].strip() == "```":
                response_text = "\n".join(lines[1:-1]).strip()
            else:
                 response_text = "\n".join(lines[1:]).strip()
        
        result = json.loads(response_text)
        is_valid = result.get("is_valid", False)
        error = result.get("error")
        
        if is_valid:
            logger.info(f"[Node: llm_sql_check] ✓ LLM SQL check passed", extra={"type": "status", "step": "llm_sql_check", "status": "success"})
            return {
                "llm_check_success": True,
                "llm_check_error": None,
            }
        else:
            logger.warning(f"[Node: llm_sql_check] ✗ LLM SQL check failed: {error}", extra={"type": "status", "step": "llm_sql_check", "status": "error"})
            return {
                "llm_check_success": False,
                "llm_check_error": error,
            }
            
    except json.JSONDecodeError:
        logger.error(f"[Node: llm_sql_check] ✗ Failed to parse LLM response: {response.content}")
        # If parse fail, strictly treating as error? or warn and pass? 
        # Requirement says "failed state with specific error message", so treating as error.
        return {
            "llm_check_success": False,
            "llm_check_error": "Failed to parse LLM Check response",
        }
    except Exception as e:
        logger.error(f"[Node: llm_sql_check] ✗ Error during LLM check: {e}")
        return {
            "llm_check_success": False,
            "llm_check_error": str(e),
        }
