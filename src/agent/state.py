"""Agent state definition for LangGraph workflow."""

from typing import Optional, TypedDict

from src.schemas.models import ConversionHistory


class AgentState(TypedDict):
    """State for the Hive to BigQuery SQL conversion agent."""
    
    # Input
    spark_sql: str
    
    # Spark validation results
    spark_valid: bool
    spark_error: Optional[str]
    
    # Conversion results
    bigquery_sql: Optional[str]
    source_tables: list[str]
    table_mapping: dict[str, str]
    table_ddls: Optional[str]
    
    # BigQuery validation results (supports both dry_run and llm modes)
    validation_success: bool
    validation_error: Optional[str]
    validation_mode: Optional[str]  # "dry_run" or "llm"
    
    # Retry tracking
    retry_count: int
    max_retries: int
    
    # Conversion history for iterative fixing
    conversion_history: list[ConversionHistory]
    
    # Execution results
    execution_success: Optional[bool]
    execution_result: Optional[list[dict] | str]
    execution_target_table: Optional[str]
    execution_error: Optional[str]

    # Data Verification results
    data_verification_success: Optional[bool]
    data_verification_result: Optional[dict | str]
    data_verification_error: Optional[str]
