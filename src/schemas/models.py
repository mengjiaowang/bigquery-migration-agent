"""Pydantic models for the service."""

from typing import Optional, List, Any
from pydantic import BaseModel


class ConvertRequest(BaseModel):
    """Request model for SQL conversion."""
    spark_sql: str


class ConversionHistory(BaseModel):
    """Model for conversion attempt history."""
    attempt: int
    bigquery_sql: Optional[str] = None
    error: Optional[str] = None


class ConvertResponse(BaseModel):
    """Response model for SQL conversion."""
    success: bool
    spark_sql: str
    spark_valid: bool
    spark_error: Optional[str] = None
    
    bigquery_sql: Optional[str] = None
    
    # Validation results
    validation_success: bool
    validation_error: Optional[str] = None
    validation_mode: Optional[str] = None
    
    # LLM Check results
    llm_check_success: Optional[bool] = None
    llm_check_error: Optional[str] = None

    # Execution results
    execution_success: Optional[bool] = None
    execution_result: Optional[Any] = None
    execution_target_table: Optional[str] = None
    execution_error: Optional[str] = None

    # Data Verification results
    data_verification_success: Optional[bool] = None
    data_verification_result: Optional[Any] = None
    data_verification_error: Optional[str] = None
    
    retry_count: int
    conversion_history: List[ConversionHistory] = []
    warning: Optional[str] = None
