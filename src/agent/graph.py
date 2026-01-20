"""LangGraph workflow definition for Hive to BigQuery SQL conversion."""

from typing import Literal

from langgraph.graph import END, StateGraph

from src.agent.nodes import (
    convert_node,
    validate_node,
    fix_node,
    validate_spark_node,
    execute_node,
    data_verification_node
)
from src.agent.state import AgentState


def should_continue_after_spark_validation(state: AgentState) -> Literal["convert", "end"]:
    """Determine if we should continue after Spark validation.
    
    Args:
        state: Current agent state.
        
    Returns:
        "convert" if Hive SQL is valid, "end" otherwise.
    """
    if state["spark_valid"]:
        return "convert"
    return "end"


def should_retry_after_validation(state: AgentState) -> Literal["execute", "fix", "end"]:
    """Determine if we should execute or retry after BigQuery validation.
    
    Args:
        state: Current agent state.
        
    Returns:
        "execute" if validation passed, "fix" if failed and retries available, "end" otherwise.
    """
    if state["validation_success"]:
        return "execute"
    
    # Check if we have retries left
    max_retries = state.get("max_retries", 3)
    if state["retry_count"] < max_retries:
        return "fix"
    
    return "end"


def create_sql_converter_graph() -> StateGraph:
    """Create the LangGraph workflow for SQL conversion.
    
    Returns:
        Compiled StateGraph for the SQL converter agent.
    """
    # Create the graph
    workflow = StateGraph(AgentState)
    
    # Add nodes
    workflow.add_node("validate_spark", validate_spark_node)
    workflow.add_node("convert", convert_node)
    workflow.add_node("validate", validate_node)
    workflow.add_node("fix", fix_node)
    workflow.add_node("execute", execute_node)
    workflow.add_node("data_verification", data_verification_node)
    
    # Set entry point
    workflow.set_entry_point("validate_spark")
    
    # Add conditional edge after Spark validation
    workflow.add_conditional_edges(
        "validate_spark",
        should_continue_after_spark_validation,
        {
            "convert": "convert",
            "end": END,
        }
    )
    
    # Add edge from convert to validate
    workflow.add_edge("convert", "validate")
    
    # Add conditional edge after validation
    workflow.add_conditional_edges(
        "validate",
        should_retry_after_validation,
        {
            "execute": "execute",
            "fix": "fix",
            "end": END,
        }
    )
    
    # Add edge from execute to data_verification
    workflow.add_edge("execute", "data_verification")
    
    # Add edge from data_verification to END
    workflow.add_edge("data_verification", END)
    
    # Add edge from fix back to validate
    workflow.add_edge("fix", "validate")
    
    # Compile the graph
    return workflow.compile()


def run_conversion(spark_sql: str, max_retries: int = 3) -> AgentState:
    """Run the SQL conversion workflow.
    
    Args:
        spark_sql: The Spark SQL to convert.
        max_retries: Maximum number of retry attempts for fixing BigQuery SQL.
        
    Returns:
        Final agent state with conversion results.
    """
    graph = create_sql_converter_graph()
    
    initial_state: AgentState = {
        "spark_sql": spark_sql,
        "spark_valid": False,
        "spark_error": None,
        "bigquery_sql": None,
        "validation_success": False,
        "validation_error": None,
        "validation_mode": None,
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
    }
    
    # Run the graph
    final_state = graph.invoke(initial_state)
    
    return final_state
