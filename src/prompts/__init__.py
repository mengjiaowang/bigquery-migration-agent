"""Prompts package."""

from .templates import (
    SPARK_TO_BIGQUERY_PROMPT,
    LLM_SQL_CHECK_PROMPT,
    FIX_BIGQUERY_PROMPT,
)

__all__ = [
    "SPARK_TO_BIGQUERY_PROMPT",
    "LLM_SQL_CHECK_PROMPT",
    "FIX_BIGQUERY_PROMPT",
]
