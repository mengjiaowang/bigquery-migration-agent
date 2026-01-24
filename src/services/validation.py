"""BigQuery validation service using dry run only."""

import re
from dataclasses import dataclass
from typing import Optional

from src.services.bigquery import BigQueryService


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


def validate_bigquery_sql(sql: str) -> ValidationResult:
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
