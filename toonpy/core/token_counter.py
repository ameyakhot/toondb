"""
Token counting utilities for TOON format comparison
Uses tiktoken for accurate token counting with fallback to character approximation
"""
from typing import Optional

# Try to import tiktoken, but make it optional
try:
    import tiktoken
    HAS_TIKTOKEN = True
except ImportError:
    HAS_TIKTOKEN = False
    tiktoken = None


def count_chars(text: str) -> int:
    """
    Count characters in text.
    
    Args:
        text: Text to count characters for
    
    Returns:
        int: Number of characters
    """
    return len(text)


def count_tokens(text: str, model: str = "gpt-4") -> int:
    """
    Count tokens in text using tiktoken.
    
    Args:
        text: Text to count tokens for
        model: Model name for tokenizer (default: "gpt-4")
               Supported: gpt-4, gpt-3.5-turbo, cl100k_base, etc.
    
    Returns:
        int: Number of tokens
    
    Note:
        Falls back to character-based approximation if tiktoken not available
        (approximately 1 token = 4 characters)
    """
    if not HAS_TIKTOKEN:
        # Fallback: approximate token count (1 token ≈ 4 characters)
        return len(text) // 4
    
    try:
        encoding = get_encoding(model)
        return len(encoding.encode(text))
    except (KeyError, ValueError):
        # If model not found, try to use cl100k_base (GPT-4 encoding) as fallback
        try:
            encoding = tiktoken.get_encoding("cl100k_base")
            return len(encoding.encode(text))
        except Exception:
            # Final fallback: character approximation
            return len(text) // 4


def get_encoding(model: str):
    """
    Get tiktoken encoding for a model.
    
    Args:
        model: Model name (e.g., "gpt-4", "gpt-3.5-turbo")
    
    Returns:
        tiktoken.Encoding: Encoding object
    
    Raises:
        KeyError: If model encoding not found
    """
    if not HAS_TIKTOKEN:
        raise ImportError("tiktoken is not installed. Install with: pip install tiktoken")
    
    return tiktoken.encoding_for_model(model)


def get_tokenizer_name(model: str) -> str:
    """
    Get human-readable tokenizer name for a model.
    
    Args:
        model: Model name (e.g., "gpt-4", "gpt-3.5-turbo")
    
    Returns:
        str: Human-readable tokenizer name
    
    Examples:
        "gpt-4" -> "gpt-4"
        "gpt-3.5-turbo" -> "gpt-3.5-turbo"
        "cl100k_base" -> "cl100k_base"
    """
    # Map common models to their tokenizer names
    model_mapping = {
        "gpt-4": "gpt-4",
        "gpt-4-turbo": "gpt-4-turbo",
        "gpt-4o": "gpt-4o",
        "gpt-3.5-turbo": "gpt-3.5-turbo",
        "gpt-35-turbo": "gpt-3.5-turbo",  # Azure naming
        "cl100k_base": "cl100k_base",
        "p50k_base": "p50k_base",
        "r50k_base": "r50k_base",
    }
    
    # Return mapped name or original model name
    return model_mapping.get(model.lower(), model)


def is_tiktoken_available() -> bool:
    """
    Check if tiktoken is available.
    
    Returns:
        bool: True if tiktoken is installed, False otherwise
    """
    return HAS_TIKTOKEN

