
from typing import Any, Union

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
        # Handle list of content blocks (e.g. from Anthropic/Vertex AI)
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
