
from typing import Any, Union, Optional

def get_content_text(content: Union[str, list[Any]]) -> str:
    """Extract text content from LLM response safely.
    
    Args:
        content: The content from LLM response (can be string or list of blocks).
        
    Returns:
        Extracted text string.
    """
    if isinstance(content, str):
        return content
    
    if isinstance(content, list):
        text_parts = []
        for block in content:
            if isinstance(block, str):
                text_parts.append(block)
            elif hasattr(block, "text"):
                text_parts.append(block.text)
            elif isinstance(block, dict) and "text" in block:
                text_parts.append(block["text"])
            else:
                text_parts.append(str(block))
        return "".join(text_parts)
    
    return str(content)

def accumulate_token_usage(current_usage: Optional[dict[str, Any]], new_usage: Optional[dict[str, Any]], node_name: Optional[str] = None, model_name: Optional[str] = None) -> dict[str, Any]:
    """Accumulate token usage statistics, including per-node breakdown.
    
    Args:
        current_usage: Existing usage dictionary (can be None).
        new_usage: New usage metadata from LLM response (can be None).
        node_name: Name of the node consuming tokens.
        model_name: Name of the model used.
        
    Returns:
        Updated usage dictionary.
    """
    if current_usage is None:
        current_usage = {}
    
    # Initialize total if not present
    if "total" not in current_usage:
        current_usage["total"] = {
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
            "cached_content_tokens": 0,
        }
    
    if not new_usage:
        return current_usage
        
    # extract raw counts needed
    input_tokens = new_usage.get("input_tokens", 0)
    output_tokens = new_usage.get("output_tokens", 0)
    total_tokens = new_usage.get("total_tokens", 0)
    
    cached_tokens = 0
    input_details = new_usage.get("input_token_details", {})
    if isinstance(input_details, dict):
        cached_tokens += input_details.get("cache_read", 0)
        cached_tokens += input_details.get("cached_content_tokens", 0)
    cached_tokens += new_usage.get("cached_content_tokens", 0)

    # Update Global Total
    current_usage["total"]["input_tokens"] += input_tokens
    current_usage["total"]["output_tokens"] += output_tokens
    current_usage["total"]["total_tokens"] += total_tokens
    current_usage["total"]["cached_content_tokens"] += cached_tokens
    
    # Update Per-Node Stats
    if node_name:
        if "nodes" not in current_usage:
            current_usage["nodes"] = {}
            
        if node_name not in current_usage["nodes"]:
            current_usage["nodes"][node_name] = {
                "call_count": 0,
                "model": model_name or "unknown",
                "usage": {
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "total_tokens": 0,
                    "cached_content_tokens": 0,
                }
            }
        
        node_stats = current_usage["nodes"][node_name]
        node_stats["call_count"] += 1
        # Update model if it was previously unknown or changed (though usually static per node)
        if model_name:
            node_stats["model"] = model_name
            
        node_stats["usage"]["input_tokens"] += input_tokens
        node_stats["usage"]["output_tokens"] += output_tokens
        node_stats["usage"]["total_tokens"] += total_tokens
        node_stats["usage"]["cached_content_tokens"] += cached_tokens
    
    return current_usage
