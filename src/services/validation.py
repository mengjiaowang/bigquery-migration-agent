"""BigQuery validation service with configurable validation mode."""

import json
import os
import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from src.prompts.templates import BIGQUERY_VALIDATION_PROMPT
from src.services.bigquery import BigQueryService, DryRunResult
from src.services.llm import get_llm


class ValidationMode(str, Enum):
    """Supported BigQuery validation modes."""
    
    DRY_RUN = "dry_run"  # Use BigQuery API dry run
    LLM = "llm"          # Use LLM prompt-based validation


@dataclass
class ValidationResult:
    """Result of BigQuery SQL validation."""
    
    success: bool
    error_message: Optional[str] = None
    validation_mode: str = "dry_run"


def replace_template_variables(sql: str) -> str:
    """Replace template variables with BigQuery equivalent syntax for dry run validation.
    
    This function replaces common template variable patterns (like ${zdt.format(...)})
    with BigQuery native functions so dry run can validate the SQL syntax.
    
    Args:
        sql: The SQL statement with template variables.
        
    Returns:
        SQL with template variables replaced by BigQuery equivalent syntax.
    """
    result = sql
    
    # Replace quoted template variables with BigQuery syntax
    # Pattern: '${zdt.addDay(N).format("yyyy-MM-dd")}' → FORMAT_DATE('%Y-%m-%d', DATE_ADD(CURRENT_DATE(), INTERVAL N DAY))
    
    # zdt.addDay(-N).format patterns (quoted)
    result = re.sub(
        r"'?\$\{zdt\.addDay\((-?\d+)\)\.format\(['\"]yyyy-MM-dd['\"]\)\}'?",
        lambda m: f"FORMAT_DATE('%Y-%m-%d', DATE_ADD(CURRENT_DATE(), INTERVAL {m.group(1)} DAY))",
        result
    )
    
    result = re.sub(
        r"'?\$\{zdt\.addDay\((-?\d+)\)\.format\(['\"]yyyy-MM-dd HH:mm:ss['\"]\)\}'?",
        lambda m: f"FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {m.group(1)} DAY))",
        result
    )
    
    result = re.sub(
        r"'?\$\{zdt\.addDay\((-?\d+)\)\.format\(['\"]yyyyMMdd['\"]\)\}'?",
        lambda m: f"FORMAT_DATE('%Y%m%d', DATE_ADD(CURRENT_DATE(), INTERVAL {m.group(1)} DAY))",
        result
    )
    
    # zdt.format patterns (without addDay, quoted)
    result = re.sub(
        r"'?\$\{zdt\.format\(['\"]yyyy-MM-dd['\"]\)\}'?",
        "FORMAT_DATE('%Y-%m-%d', CURRENT_DATE())",
        result
    )
    
    result = re.sub(
        r"'?\$\{zdt\.format\(['\"]yyyy-MM-dd HH:mm:ss['\"]\)\}'?",
        "FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', CURRENT_TIMESTAMP())",
        result
    )
    
    result = re.sub(
        r"'?\$\{zdt\.format\(['\"]yyyyMMdd['\"]\)\}'?",
        "FORMAT_DATE('%Y%m%d', CURRENT_DATE())",
        result
    )
    
    # Generic fallback for any remaining ${zdt...} patterns
    result = re.sub(
        r"'?\$\{zdt\.[^}]+\}'?",
        "FORMAT_DATE('%Y-%m-%d', CURRENT_DATE())",
        result
    )
    
    # Final catch-all: replace any remaining ${...} with a placeholder string
    result = re.sub(r"'?\$\{[^}]+\}'?", "'PLACEHOLDER'", result)
    
    return result


def get_validation_mode() -> ValidationMode:
    """Get the configured validation mode from environment.
    
    Returns:
        ValidationMode enum value.
        
    Raises:
        ValueError: If mode is not supported.
    """
    mode = os.getenv("BQ_VALIDATION_MODE", "dry_run").lower()
    
    try:
        return ValidationMode(mode)
    except ValueError:
        supported = ", ".join([m.value for m in ValidationMode])
        raise ValueError(
            f"Unsupported validation mode: {mode}. "
            f"Supported modes: {supported}"
        )


def validate_with_dry_run(sql: str) -> ValidationResult:
    """Validate BigQuery SQL using BigQuery API dry run.
    
    Template variables (like ${zdt.format(...)}) are replaced with valid
    placeholder values before validation to avoid syntax errors.
    
    Args:
        sql: The BigQuery SQL to validate.
        
    Returns:
        ValidationResult with success status and error message.
    """
    # Replace template variables with valid placeholder values
    sql_for_validation = replace_template_variables(sql)
    
    bq_service = BigQueryService()
    
    try:
        result = bq_service.dry_run(sql_for_validation)
        return ValidationResult(
            success=result.success,
            error_message=result.error_message,
            validation_mode="dry_run",
        )
    finally:
        bq_service.close()


def validate_with_llm(sql: str) -> ValidationResult:
    """Validate BigQuery SQL using LLM prompt-based validation.
    
    Args:
        sql: The BigQuery SQL to validate.
        
    Returns:
        ValidationResult with success status and error message.
    """
    llm = get_llm()
    
    prompt = BIGQUERY_VALIDATION_PROMPT.format(bigquery_sql=sql)
    response = llm.invoke(prompt)
    
    try:
        # Clean up response - remove markdown code blocks if present
        response_text = response.content.strip()
        if response_text.startswith("```"):
            lines = response_text.split("\n")
            response_text = "\n".join(lines[1:-1])
        
        result = json.loads(response_text)
        is_valid = result.get("is_valid", False)
        error = result.get("error")
        
        return ValidationResult(
            success=is_valid,
            error_message=error if not is_valid else None,
            validation_mode="llm",
        )
    except json.JSONDecodeError:
        # If we can't parse the response, assume invalid
        return ValidationResult(
            success=False,
            error_message=f"Failed to parse LLM validation response: {response.content}",
            validation_mode="llm",
        )


def validate_bigquery_sql(sql: str) -> ValidationResult:
    """Validate BigQuery SQL using the configured validation mode.
    
    Args:
        sql: The BigQuery SQL to validate.
        
    Returns:
        ValidationResult with success status and error message.
    """
    mode = get_validation_mode()
    
    if mode == ValidationMode.DRY_RUN:
        return validate_with_dry_run(sql)
    elif mode == ValidationMode.LLM:
        return validate_with_llm(sql)
    else:
        raise ValueError(f"Unsupported validation mode: {mode}")
