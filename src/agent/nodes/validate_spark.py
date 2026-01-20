"""Spark SQL validation node."""

import json
import logging
from typing import Any

from src.agent.state import AgentState
from src.prompts.templates import SPARK_VALIDATION_PROMPT
from src.services.llm import get_llm
from src.services.utils import get_content_text

# Configure logger
logger = logging.getLogger(__name__)


def validate_spark_node(state: AgentState) -> dict[str, Any]:
    """Validate the input Spark SQL syntax.
    
    Args:
        state: Current agent state containing spark_sql.
        
    Returns:
        Updated state with spark_valid and spark_error.
    """
    logger.info("=" * 60)
    logger.info("[Node: validate_spark] Starting Spark SQL validation", extra={"type": "status", "step": "spark", "status": "loading"})
    logger.info(f"Input Spark SQL:\n{state['spark_sql']}")
    
    llm = get_llm()
    
    prompt = SPARK_VALIDATION_PROMPT.format(spark_sql=state["spark_sql"])
    response = llm.invoke(prompt)
    
    logger.debug(f"LLM raw response: {response.content}")
    
    # Parse the JSON response
    try:
        # Clean up response - remove markdown code blocks if present
        response_text = get_content_text(response.content).strip()
        if response_text.startswith("```"):
            # Remove markdown code block
            lines = response_text.split("\n")
            response_text = "\n".join(lines[1:-1])
        
        result = json.loads(response_text)
        is_valid = result.get("is_valid", False)
        error = result.get("error")
        
        if is_valid:
            logger.info("[Node: validate_spark] ✓ Spark SQL is valid", extra={"type": "status", "step": "spark", "status": "success"})
        else:
            logger.warning(f"[Node: validate_spark] ✗ Spark SQL is invalid: {error}", extra={"type": "status", "step": "spark", "status": "error"})
        
        return {
            "spark_valid": is_valid,
            "spark_error": error if not is_valid else None,
        }
    except json.JSONDecodeError:
        # If we can't parse the response, assume invalid with the raw response as error
        logger.error(f"[Node: validate_spark] Failed to parse LLM response: {response.content}", extra={"type": "status", "step": "spark", "status": "error"})
        return {
            "spark_valid": False,
            "spark_error": f"Failed to validate Spark SQL: {response.content}",
        }
