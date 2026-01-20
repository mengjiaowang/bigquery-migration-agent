"""Node implementations for LangGraph workflow."""

import json
import logging
import os
from typing import Any, Union

from src.agent.state import AgentState
from src.prompts.templates import (
    FIX_BIGQUERY_PROMPT,
    SPARK_TO_BIGQUERY_PROMPT,
    SPARK_VALIDATION_PROMPT,
)
from src.schemas.models import ConversionHistory
from src.services.bigquery import BigQueryService
from src.services.llm import get_llm
from src.services.sql_chunker import SQLChunker, ChunkedConverter
from src.services.table_mapping import get_table_mapping_service
from src.services.utils import get_content_text
from src.services.validation import validate_bigquery_sql

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
    logger.info("[Node: validate_spark] Starting Spark SQL validation")
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
            logger.info("[Node: validate_spark] ✓ Spark SQL is valid")
        else:
            logger.warning(f"[Node: validate_spark] ✗ Spark SQL is invalid: {error}")
        
        return {
            "spark_valid": is_valid,
            "spark_error": error if not is_valid else None,
        }
    except json.JSONDecodeError:
        # If we can't parse the response, assume invalid with the raw response as error
        logger.error(f"[Node: validate_spark] Failed to parse LLM response: {response.content}")
        return {
            "spark_valid": False,
            "spark_error": f"Failed to validate Spark SQL: {response.content}",
        }


def _convert_single_chunk(spark_sql: str, table_mapping_info: str) -> str:
    """Convert a single SQL chunk using LLM.
    
    Args:
        spark_sql: The Spark SQL chunk to convert.
        table_mapping_info: Table mapping information for the prompt.
        
    Returns:
        The converted BigQuery SQL.
    """
    llm = get_llm()
    
    prompt = SPARK_TO_BIGQUERY_PROMPT.format(
        spark_sql=spark_sql,
        table_mapping_info=table_mapping_info,
    )
    response = llm.invoke(prompt)
    
    # Clean up response - remove markdown code blocks if present
    bigquery_sql = get_content_text(response.content).strip()
    if bigquery_sql.startswith("```"):
        lines = bigquery_sql.split("\n")
        # Remove first line (```sql or ```) and last line (```)
        bigquery_sql = "\n".join(lines[1:-1]).strip()
    
    return bigquery_sql


def convert_node(state: AgentState) -> dict[str, Any]:
    """Convert Spark SQL to BigQuery SQL.
    
    For long SQL statements, this function will:
    1. Analyze the SQL structure (CTE, UNION, INSERT...SELECT, etc.)
    2. Split into manageable chunks
    3. Convert each chunk separately
    4. Merge the results
    
    Args:
        state: Current agent state containing hive_sql.
        
    Returns:
        Updated state with bigquery_sql.
    """
    logger.info("=" * 60)
    logger.info("[Node: convert] Starting Spark to BigQuery conversion")
    
    spark_sql = state['spark_sql']
    sql_length = len(spark_sql)
    sql_lines = spark_sql.count('\n')
    
    logger.info(f"[Node: convert] Input SQL: {sql_length} chars, {sql_lines} lines")
    
    # Get table mapping information
    table_mapping_service = get_table_mapping_service()
    table_mapping_info = table_mapping_service.get_mapping_info_for_prompt()
    
    logger.info(f"[Node: convert] Using {len(table_mapping_service.get_all_mappings())} table mappings")
    
    # Check if chunking is needed
    chunker = SQLChunker(spark_sql)
    use_chunking = chunker.should_chunk()
    
    # Also check environment variable to force/disable chunking
    chunking_mode = os.getenv("SQL_CHUNKING_MODE", "auto").lower()
    if chunking_mode == "disabled":
        use_chunking = False
        logger.info("[Node: convert] SQL chunking disabled by configuration")
    elif chunking_mode == "always":
        use_chunking = True
        logger.info("[Node: convert] SQL chunking forced by configuration")
    
    if use_chunking:
        logger.info("[Node: convert] Using chunked conversion strategy")
        
        # Analyze and chunk
        chunks = chunker.analyze_and_chunk()
        
        if len(chunks) > 1:
            logger.info(f"[Node: convert] Split into {len(chunks)} chunks")
            
            # Create converter with the single-chunk converter function
            def converter_func(sql: str) -> str:
                return _convert_single_chunk(sql, table_mapping_info)
            
            chunked_converter = ChunkedConverter(converter_func)
            bigquery_sql = chunked_converter.convert_chunks(chunks)
            
            logger.info("[Node: convert] Chunked conversion completed")
        else:
            # Only one chunk, convert normally
            logger.info("[Node: convert] SQL analyzed but no chunking needed")
            bigquery_sql = _convert_single_chunk(spark_sql, table_mapping_info)
    else:
        # Direct conversion without chunking
        logger.info("[Node: convert] Using direct conversion (no chunking)")
        bigquery_sql = _convert_single_chunk(spark_sql, table_mapping_info)
    
    # Apply table name replacement as a safety net
    # (in case the LLM didn't apply all mappings correctly)
    bigquery_sql = table_mapping_service.replace_table_names(bigquery_sql)
    
    logger.info(f"[Node: convert] Final BigQuery SQL ({len(bigquery_sql)} chars):\n{bigquery_sql}")
    
    return {
        "bigquery_sql": bigquery_sql,
        "retry_count": 0,
        "conversion_history": [],
    }


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


def fix_node(state: AgentState) -> dict[str, Any]:
    """Fix BigQuery SQL based on validation error.
    
    Args:
        state: Current agent state containing bigquery_sql and validation_error.
        
    Returns:
        Updated state with corrected bigquery_sql and incremented retry_count.
    """
    retry_count = state["retry_count"] + 1
    
    logger.info("=" * 60)
    logger.info(f"[Node: fix] Starting SQL fix (retry {retry_count})")
    logger.info(f"[Node: fix] Previous error: {state['validation_error']}")
    logger.info(f"[Node: fix] SQL to fix:\n{state['bigquery_sql']}")
    
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
        error_message=state["validation_error"],
        conversion_history=history_str,
    )
    
    response = llm.invoke(prompt)
    
    # Clean up response - remove markdown code blocks if present
    fixed_sql = get_content_text(response.content).strip()
    if fixed_sql.startswith("```"):
        lines = fixed_sql.split("\n")
        fixed_sql = "\n".join(lines[1:-1]).strip()
    
    # Apply table name replacement as a safety net
    fixed_sql = table_mapping_service.replace_table_names(fixed_sql)
    
    logger.info(f"[Node: fix] Fixed BigQuery SQL:\n{fixed_sql}")
    
    return {
        "bigquery_sql": fixed_sql,
        "retry_count": retry_count,
    }


def execute_node(state: AgentState) -> dict[str, Any]:
    """Execute the validated BigQuery SQL.
    
    Args:
        state: Current agent state.
        
    Returns:
        State update with execution results.
    """
    logger.info("============================================================")
    logger.info("[Node: execute] Starting BigQuery SQL execution")
    
    bq_service = BigQueryService()
    execution_result = bq_service.execute_query(state["bigquery_sql"])
    bq_service.close()
    
    if execution_result.success:
        logger.info("[Node: execute] ✓ SQL execution successful")
        logger.info(f"Target Table: {execution_result.target_table}")
        logger.info(f"Result: {execution_result.result}")
    else:
        logger.error(f"[Node: execute] ✗ SQL execution failed: {execution_result.error_message}")
    
    return {
        "execution_success": execution_result.success,
        "execution_result": execution_result.result,
        "execution_target_table": execution_result.target_table,
        "execution_error": execution_result.error_message,
    }


def data_verification_node(state: AgentState) -> dict[str, Any]:
    """Verify data in the target table after execution.
    
    Args:
        state: Current agent state.
        
    Returns:
        State update with verification results.
    """
    logger.info("============================================================")
    logger.info("[Node: verify] Starting Data Verification")
    
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
            logger.info(f"[Node: verify] ✓ Verification successful. Row count: {count}")
            return {
                "data_verification_success": True,
                "data_verification_result": {"row_count": count},
                "data_verification_error": None,
            }
        else:
            error_msg = result.error_message or "Failed to get row count"
            logger.error(f"[Node: verify] ✗ Verification failed: {error_msg}")
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
