"""Pydantic models for request/response schemas."""

from typing import Optional
from pydantic import BaseModel, Field


class ConvertRequest(BaseModel):
    """Request model for SQL conversion."""
    
    spark_sql: str = Field(..., description="The Spark SQL statement to convert")


class ConversionHistory(BaseModel):
    """Model for tracking conversion attempts."""
    
    attempt: int = Field(..., description="Attempt number")
    bigquery_sql: str = Field(..., description="The converted BigQuery SQL")
    error: Optional[str] = Field(None, description="Error message if validation failed")


class ConvertResponse(BaseModel):
    """Response model for SQL conversion."""
    
    success: bool = Field(..., description="Whether the conversion was successful")
    spark_sql: str = Field(..., description="The original Spark SQL")
    spark_valid: bool = Field(..., description="Whether the Spark SQL is valid")
    spark_error: Optional[str] = Field(None, description="Spark SQL validation error if any")
    bigquery_sql: Optional[str] = Field(None, description="The converted BigQuery SQL")
    validation_success: bool = Field(False, description="Whether BigQuery validation passed")
    validation_error: Optional[str] = Field(None, description="BigQuery validation error if any")
    validation_mode: Optional[str] = Field(None, description="Validation mode used: 'dry_run' or 'llm'")
    retry_count: int = Field(0, description="Number of retry attempts made")
    conversion_history: list[ConversionHistory] = Field(
        default_factory=list,
        description="History of conversion attempts"
    )
    warning: Optional[str] = Field(None, description="Warning message if max retries exceeded")
    
    # Execution results
    execution_success: Optional[bool] = Field(None, description="Whether execution was successful")
    execution_result: Optional[list[dict] | str] = Field(None, description="Execution result data or message")
    execution_target_table: Optional[str] = Field(None, description="Target table affected by execution")
    execution_error: Optional[str] = Field(None, description="Execution error message if any")

    # Data Verification results
    data_verification_success: Optional[bool] = Field(None, description="Whether data verification was successful")
    data_verification_result: Optional[dict | str] = Field(None, description="Data verification result or message")
    data_verification_error: Optional[str] = Field(None, description="Data verification error message if any")
