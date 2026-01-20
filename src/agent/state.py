"""Agent state definition for LangGraph workflow."""

from typing import Optional, TypedDict

from src.schemas.models import ConversionHistory


class AgentState(TypedDict):
    """State for the Hive to BigQuery SQL conversion agent."""
    
    # Input
    hive_sql: str
    
    # Hive validation results
    hive_valid: bool
    hive_error: Optional[str]
    
    # Conversion results
    bigquery_sql: Optional[str]
    
    # BigQuery validation results (supports both dry_run and llm modes)
    validation_success: bool
    validation_error: Optional[str]
    validation_mode: Optional[str]  # "dry_run" or "llm"
    
    # Retry tracking
    retry_count: int
    max_retries: int
    
    # Conversion history for iterative fixing
    conversion_history: list[ConversionHistory]
