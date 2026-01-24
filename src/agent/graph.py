"""LangGraph workflow definition for Hive to BigQuery SQL conversion."""

import os
from typing import Literal, Optional

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


def should_continue_after_llm_check(state: AgentState) -> Literal["bigquery_sql_execute", "bigquery_error_fix"]:
    """Determine if we should continue to execution or fix after LLM check.
    
    Args:
        state: Current agent state.
        
    Returns:
        "bigquery_sql_execute" if LLM check passed, "bigquery_error_fix" otherwise.
    """
    if state.get("llm_check_success", True): # Default to True if not present for some reason
        return "bigquery_sql_execute"
    return "bigquery_error_fix"


def should_retry_after_execution(state: AgentState) -> Literal["data_verification", "bigquery_error_fix", "end"]:
    """Determine if we should verify data or retry after BigQuery execution.
    
    Args:
        state: Current agent state.
        
    Returns:
        "data_verification" if execution passed, "bigquery_error_fix" if failed and retries available, "end" otherwise.
    """
    if state["execution_success"]:
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
    # Create the graph
    workflow = StateGraph(AgentState)
    
    # Add nodes
    workflow.add_node("spark_sql_validate", spark_sql_validate)
    workflow.add_node("sql_convert", sql_convert)
    workflow.add_node("llm_sql_check", llm_sql_check)
    workflow.add_node("bigquery_dry_run", bigquery_dry_run)
    workflow.add_node("bigquery_error_fix", bigquery_error_fix)
    workflow.add_node("bigquery_sql_execute", bigquery_sql_execute)
    workflow.add_node("data_verification", data_verification)
    
    # Set entry point
    workflow.set_entry_point("spark_sql_validate")
    
    # Add conditional edge after Spark validation
    workflow.add_conditional_edges(
        "spark_sql_validate",
        should_continue_after_spark_validation,
        {
            "sql_convert": "sql_convert",
            "end": END,
        }
    )
    
    # Add edge from convert to dry_run (New Flow: Convert -> Dry Run)
    workflow.add_edge("sql_convert", "bigquery_dry_run")
    
    # Add conditional edge after dry run
    workflow.add_conditional_edges(
        "bigquery_dry_run",
        should_continue_after_dry_run,
        {
            "llm_sql_check": "llm_sql_check",
            "bigquery_error_fix": "bigquery_error_fix",
            "end": END,
        }
    )
    
    # Add conditional edge from llm_sql_check
    workflow.add_conditional_edges(
        "llm_sql_check",
        should_continue_after_llm_check,
        {
            "bigquery_sql_execute": "bigquery_sql_execute",
            "bigquery_error_fix": "bigquery_error_fix",
        }
    )
    
    # Add conditional edge after execution
    workflow.add_conditional_edges(
        "bigquery_sql_execute",
        should_retry_after_execution,
        {
            "data_verification": "data_verification",
            "bigquery_error_fix": "bigquery_error_fix",
            "end": END,
        }
    )
    
    # Add edge from data_verification to END
    workflow.add_edge("data_verification", END)
    
    # Add edge from fix back to dry_run (Always validate syntax first after fix)
    workflow.add_edge("bigquery_error_fix", "bigquery_dry_run")
    
    # Compile the graph
    return workflow.compile()


def run_conversion(spark_sql: str, max_retries: Optional[int] = None) -> AgentState:
    """Run the SQL conversion workflow.
    
    Args:
        spark_sql: The Spark SQL to convert.
        max_retries: Maximum number of retry attempts for fixing BigQuery SQL.
                     If None, reads from MAX_RETRIES env var (default 10).
        
    Returns:
        Final agent state with conversion results.
    """
    if max_retries is None:
        max_retries = int(os.getenv("MAX_RETRIES", "10"))

    graph = create_sql_converter_graph()
    
    initial_state: AgentState = {
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
    }
    
    # Run the graph
    final_state = graph.invoke(initial_state)
    
    return final_state
