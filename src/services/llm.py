"""LLM service with configurable provider support."""

import logging
import os
from enum import Enum
from typing import Union

import google.auth
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_google_vertexai import ChatVertexAI
from langchain_openai import ChatOpenAI

logger = logging.getLogger(__name__)


class LLMProvider(str, Enum):
    """Supported LLM providers."""
    
    GEMINI = "gemini"
    OPENAI = "openai"


def get_llm_provider() -> LLMProvider:
    """Get the configured LLM provider from environment.
    
    Returns:
        LLMProvider enum value.
        
    Raises:
        ValueError: If provider is not supported.
    """
    provider = os.getenv("LLM_PROVIDER", "gemini").lower()
    
    try:
        return LLMProvider(provider)
    except ValueError:
        supported = ", ".join([p.value for p in LLMProvider])
        raise ValueError(
            f"Unsupported LLM provider: {provider}. "
            f"Supported providers: {supported}"
        )


def get_gemini_llm() -> ChatVertexAI:
    """Get Google Gemini LLM instance via Vertex AI.
    
    Uses Application Default Credentials (ADC) for authentication.
    Run 'gcloud auth application-default login' to set up local credentials.
    
    Returns:
        ChatVertexAI instance.
        
    Raises:
        ValueError: If project ID cannot be determined.
    """
    # Get credentials and project ID using ADC
    credentials, auth_project_id = google.auth.default()
    
    # Priority: env var > credentials project
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT") or auth_project_id
    if not project_id:
        raise ValueError(
            "Google Cloud project ID is required. Set GOOGLE_CLOUD_PROJECT "
            "environment variable or configure default project in gcloud."
        )
    
    location = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
    model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
    
    logger.info(f"[LLM] Using Vertex AI - Project: {project_id}, Location: {location}, Model: {model}")
    
    return ChatVertexAI(
        model=model,
        project=project_id,
        location=location,
        temperature=0.1,
    )


def get_openai_llm() -> ChatOpenAI:
    """Get OpenAI LLM instance.
    
    Returns:
        ChatOpenAI instance.
        
    Raises:
        ValueError: If OPENAI_API_KEY is not set.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError(
            "OPENAI_API_KEY environment variable is required for OpenAI provider"
        )
    
    model = os.getenv("OPENAI_MODEL", "gpt-4o")
    base_url = os.getenv("OPENAI_API_BASE")  # Optional: for third-party OpenAI-compatible APIs
    
    kwargs = {
        "model": model,
        "api_key": api_key,
        "temperature": 0.1,
    }
    
    if base_url:
        kwargs["base_url"] = base_url
    
    return ChatOpenAI(**kwargs)


def get_llm() -> BaseChatModel:
    """Get LLM instance based on configured provider.
    
    Returns:
        BaseChatModel instance (either Gemini or OpenAI).
        
    Raises:
        ValueError: If provider is not supported or required API key is missing.
    """
    provider = get_llm_provider()
    
    if provider == LLMProvider.GEMINI:
        return get_gemini_llm()
    elif provider == LLMProvider.OPENAI:
        return get_openai_llm()
    else:
        raise ValueError(f"Unsupported LLM provider: {provider}")
