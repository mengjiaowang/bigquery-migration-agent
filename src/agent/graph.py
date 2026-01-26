"""LangGraph workflow definition for Hive to BigQuery SQL conversion."""

import logging
import os
import uuid
from typing import Literal, Optional

logger = logging.getLogger(__name__)

from langgraph.graph import END, StateGraph

from src.agent.nodes import (
    spark_sql_validate,
    sql_convert,
    llm_sql_check,
    bigquery_dry_run,
    bigquery_error_fix,
    bigquery_sql_execute,
    data_verification
)
from src.agent.state import AgentState
from src.services.tracing import trace_node


def should_continue_after_spark_validation(state: AgentState) -> Literal["sql_convert", "end"]:
    """Determine if we should continue after Spark validation.
    
    Args:
        state: Current agent state.
    
    Returns:
        "sql_convert" if Hive SQL is valid, "end" otherwise.
    """
    if state["spark_valid"]:
        return "sql_convert"
    return "end"


def should_continue_after_dry_run(state: AgentState) -> Literal["llm_sql_check", "bigquery_error_fix", "end"]:
    """Determine if we should continue to LLM check or fix after dry run.
    
    Args:
        state: Current agent state.
        
    Returns:
        "llm_sql_check" if dry run passed, "bigquery_error_fix" if failed and retries available, "end" otherwise.
    """
    if state["validation_success"]:
        return "llm_sql_check"
    
    # Check if we have retries left
    max_retries = state.get("max_retries", 3)
    if state["retry_count"] < max_retries:
        return "bigquery_error_fix"
    
    return "end"


def should_continue_after_llm_check(state: AgentState) -> Literal["bigquery_sql_execute", "bigquery_error_fix", "end"]:
    """Determine if we should continue to execution or fix after LLM check.
    
    Args:
        state: Current agent state.
        
    Returns:
        "bigquery_sql_execute" if LLM check passed and execution enabled,
        "bigquery_error_fix" if LLM check failed,
        "end" if execution is disabled.
    """
    if not state.get("llm_check_success", True): # Default to True if not present
        return "bigquery_error_fix"
        
    # Check if execution is enabled
    if os.getenv("EXECUTE_ENABLED", "true").lower() != "true":
        return "end"
        
    return "bigquery_sql_execute"


def should_retry_after_execution(state: AgentState) -> Literal["data_verification", "bigquery_error_fix", "end"]:
    """Determine if we should verify data or retry after BigQuery execution.
    
    Args:
        state: Current agent state.
        
    Returns:
        "data_verification" if execution passed and verification enabled,
        "bigquery_error_fix" if failed and retries available,
        "end" otherwise.
    """
    if state["execution_success"]:
        # Check if data verification is enabled
        if os.getenv("DATA_VERIFICATION_ENABLED", "true").lower() != "true":
            return "end"
        return "data_verification"
    
    # Check if we have retries left
    max_retries = state.get("max_retries", 3)
    if state["retry_count"] < max_retries:
        return "bigquery_error_fix"
    
    return "end"


def create_sql_converter_graph() -> StateGraph:
    """Create the LangGraph workflow for SQL conversion.
    
    Returns:
        Compiled StateGraph for the SQL converter agent.
    """
    workflow = StateGraph(AgentState)
    
    workflow.add_node("spark_sql_validate", trace_node("spark_sql_validate", spark_sql_validate))
    workflow.add_node("sql_convert", trace_node("sql_convert", sql_convert))
    workflow.add_node("llm_sql_check", trace_node("llm_sql_check", llm_sql_check))
    workflow.add_node("bigquery_dry_run", trace_node("bigquery_dry_run", bigquery_dry_run))
    workflow.add_node("bigquery_error_fix", trace_node("bigquery_error_fix", bigquery_error_fix))
    workflow.add_node("bigquery_sql_execute", trace_node("bigquery_sql_execute", bigquery_sql_execute))
    workflow.add_node("data_verification", trace_node("data_verification", data_verification))
    
    workflow.set_entry_point("spark_sql_validate")
    
    workflow.add_conditional_edges(
        "spark_sql_validate",
        should_continue_after_spark_validation,
        {
            "sql_convert": "sql_convert",
            "end": END,
        }
    )
    
    workflow.add_edge("sql_convert", "bigquery_dry_run")
    
    workflow.add_conditional_edges(
        "bigquery_dry_run",
        should_continue_after_dry_run,
        {
            "llm_sql_check": "llm_sql_check",
            "bigquery_error_fix": "bigquery_error_fix",
            "end": END,
        }
    )
    
    workflow.add_conditional_edges(
        "llm_sql_check",
        should_continue_after_llm_check,
        {
            "bigquery_sql_execute": "bigquery_sql_execute",
            "bigquery_error_fix": "bigquery_error_fix",
            "end": END,
        }
    )
    
    workflow.add_conditional_edges(
        "bigquery_sql_execute",
        should_retry_after_execution,
        {
            "data_verification": "data_verification",
            "bigquery_error_fix": "bigquery_error_fix",
            "end": END,
        }
    )
    
    workflow.add_edge("data_verification", END)
    
    workflow.add_edge("bigquery_error_fix", "bigquery_dry_run")
    
    return workflow.compile()


def run_conversion(spark_sql: str, max_retries: Optional[int] = None) -> AgentState:
    """Run the SQL conversion workflow.
    
    Args:
        spark_sql: The Spark SQL to convert.
        max_retries: Maximum number of retry attempts for fixing BigQuery SQL.
                     If None, reads from AUTO_FIX_MAX_RETRIES env var (default 10).
        
    Returns:
        Final agent state with conversion results.
    """
    if max_retries is None:
        max_retries = int(os.getenv("AUTO_FIX_MAX_RETRIES", "10"))

    # Generate session ID
    session_id = str(uuid.uuid4())
    logger.info(f"[Workflow] Starting session: {session_id}")

    graph = create_sql_converter_graph()
    
    initial_state: AgentState = {
        "agent_session_id": session_id,
        "spark_sql": spark_sql,
        "spark_valid": False,
        "spark_error": None,
        "bigquery_sql": None,
        "validation_success": False,
        "validation_error": None,
        "validation_mode": "dry_run",
        "retry_count": 0,
        "max_retries": max_retries,
        "conversion_history": [],
        "execution_success": None,
        "execution_result": None,
        "execution_target_table": None,
        "execution_error": None,
        "data_verification_success": None,
        "data_verification_result": None,
        "data_verification_error": None,
        "llm_check_success": None,
        "llm_check_error": None,
        "token_usage": {},
    }
    
    final_state = graph.invoke(initial_state)
    
    # Log token usage summary
    token_usage = final_state.get("token_usage")
    if token_usage:
        logger.info("=" * 60)
        logger.info("[Token Usage Summary]")
        
        # Log Global Totals
        if "total" in token_usage:
            logger.info("  Total Usage:")
            total = token_usage["total"]
            logger.info(f"    Input Tokens:   {total.get('input_tokens', 0)}")
            logger.info(f"    Output Tokens:  {total.get('output_tokens', 0)}")
            logger.info(f"    Total Tokens:   {total.get('total_tokens', 0)}")
            logger.info(f"    Cached Tokens:  {total.get('cached_content_tokens', 0)}")
            
        if "nodes" in token_usage:
            logger.info("-" * 40)
            logger.info("  Breakdown by Node:")
            for node_name, stats in token_usage["nodes"].items():
                model_name = stats.get('model', 'unknown')
                
                usage = stats.get("usage", {})
                input_tokens = usage.get('input_tokens', 0)
                output_tokens = usage.get('output_tokens', 0)
                cached_tokens = usage.get('cached_content_tokens', 0)
                
                logger.info(f"    [{node_name}]")
                logger.info(f"      Model:      {model_name}")
                logger.info(f"      Calls:      {stats.get('call_count', 0)}")
                logger.info(f"      Input:      {input_tokens}")
                logger.info(f"      Output:     {output_tokens}")
                logger.info(f"      Cached:     {cached_tokens}")

        logger.info("=" * 60)
    
    return final_state
