"""LLM service with configurable provider support (Vertex AI only)."""

import logging
import os
from typing import Optional

import google.auth
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_google_genai import ChatGoogleGenerativeAI

logger = logging.getLogger(__name__)


def get_model_name(node_name: Optional[str] = None) -> str:
    """Get the model name for a specific node.
    
    Args:
        node_name: Name of the node. Required for model resolution.
        
    Returns:
        Model name string.
    """
    # 1. Check {NODE_NAME}_MODEL
    if node_name:
        node_model = os.getenv(f"{node_name.upper()}_MODEL")
        if node_model:
            return node_model
    else:
        raise ValueError("node_name is required to determine the model configuration.")

    raise ValueError(
        f"No LLM model configured for node '{node_name}'. "
        f"Please set {node_name.upper()}_MODEL in environment variables. "
    )


def get_llm(node_name: Optional[str] = None) -> BaseChatModel:
    """Get LLM instance based on configuration for the specific node.
    
    Args:
        node_name: Optional name of the node requesting the LLM.
                   Common values: 'sql_convert', 'llm_sql_check', 'bigquery_error_fix'.
    
    Returns:
        BaseChatModel instance (always Vertex AI).
    """
    model = get_model_name(node_name)
    
    credentials, auth_project_id = google.auth.default()
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT") or auth_project_id
    location = os.getenv("GOOGLE_CLOUD_LOCATION")
    
    if not project_id:
        raise ValueError("Google Cloud project ID is required for Vertex AI.")
    if not location:
        raise ValueError("GOOGLE_CLOUD_LOCATION environment variable is required.")
        
    logger.info(f"[LLM] Request for node '{node_name}' -> Model: {model} (Vertex AI), Project: {project_id}, Location: {location}")
    
    return ChatGoogleGenerativeAI(
        model=model,
        project=project_id,
        location=location,
        temperature=0.1,
    )
