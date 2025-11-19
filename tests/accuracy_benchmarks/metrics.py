"""
Metrics calculation functions for comparing JSON vs TOON format accuracy.
"""
from typing import List, Dict, Any, Optional, Set
import re
import numpy as np

try:
    from sentence_transformers import SentenceTransformer
    HAS_SENTENCE_TRANSFORMERS = True
except ImportError:
    HAS_SENTENCE_TRANSFORMERS = False


# Lazy load sentence transformer model
_semantic_model = None


def _get_semantic_model():
    """Lazy load sentence transformer model"""
    global _semantic_model
    if _semantic_model is None and HAS_SENTENCE_TRANSFORMERS:
        _semantic_model = SentenceTransformer('all-MiniLM-L6-v2')
    return _semantic_model


def exact_match(output1: str, output2: str) -> float:
    """
    Calculate exact match accuracy (0.0 or 1.0).
    
    Args:
        output1: First output string
        output2: Second output string
    
    Returns:
        1.0 if exact match, 0.0 otherwise
    """
    return 1.0 if output1.strip() == output2.strip() else 0.0


def semantic_similarity(output1: str, output2: str) -> float:
    """
    Calculate semantic similarity using sentence transformers.
    
    Args:
        output1: First output string
        output2: Second output string
    
    Returns:
        Similarity score between 0.0 and 1.0 (1.0 = identical meaning)
    """
    if not HAS_SENTENCE_TRANSFORMERS:
        # Fallback to simple string comparison if sentence-transformers not available
        return exact_match(output1, output2)
    
    model = _get_semantic_model()
    if model is None:
        return exact_match(output1, output2)
    
    try:
        embeddings = model.encode([output1, output2])
        # Calculate cosine similarity
        similarity = np.dot(embeddings[0], embeddings[1]) / (
            np.linalg.norm(embeddings[0]) * np.linalg.norm(embeddings[1])
        )
        return float(similarity)
    except Exception:
        # Fallback on error
        return exact_match(output1, output2)


def extract_emails(text: str) -> Set[str]:
    """Extract email addresses from text"""
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    return set(re.findall(email_pattern, text, re.IGNORECASE))


def extract_numbers(text: str) -> List[float]:
    """Extract numeric values from text"""
    # Match integers and floats
    pattern = r'-?\d+\.?\d*'
    matches = re.findall(pattern, text)
    return [float(m) for m in matches if m]


def extraction_accuracy(
    output: str, 
    ground_truth: Any, 
    field: str = "emails"
) -> float:
    """
    Calculate extraction accuracy for specific fields.
    
    Args:
        output: LLM output text
        ground_truth: Expected values (list or set)
        field: Type of field to extract ("emails", "numbers", "names")
    
    Returns:
        Accuracy score between 0.0 and 1.0
    """
    if field == "emails":
        extracted = extract_emails(output)
        if isinstance(ground_truth, (list, set)):
            expected = set(str(e).lower() for e in ground_truth)
            extracted = set(e.lower() for e in extracted)
        else:
            expected = {str(ground_truth).lower()}
            extracted = set(e.lower() for e in extracted)
    elif field == "numbers":
        extracted = set(extract_numbers(output))
        if isinstance(ground_truth, (list, set)):
            expected = set(float(e) for e in ground_truth)
        else:
            expected = {float(ground_truth)}
    else:
        # Generic extraction - treat as exact match
        return exact_match(output, str(ground_truth))
    
    if not expected:
        return 0.0
    
    # Calculate precision and recall
    if not extracted:
        return 0.0
    
    intersection = extracted & expected
    precision = len(intersection) / len(extracted) if extracted else 0.0
    recall = len(intersection) / len(expected) if expected else 0.0
    
    # F1 score
    if precision + recall == 0:
        return 0.0
    f1 = 2 * (precision * recall) / (precision + recall)
    return f1


def numeric_accuracy(
    output1: str, 
    output2: str, 
    tolerance: float = 0.01
) -> float:
    """
    Calculate numeric accuracy by comparing extracted numbers.
    
    Args:
        output1: First output string
        output2: Second output string
        tolerance: Tolerance for numeric comparison
    
    Returns:
        Accuracy score between 0.0 and 1.0
    """
    numbers1 = extract_numbers(output1)
    numbers2 = extract_numbers(output2)
    
    if not numbers1 and not numbers2:
        # No numbers found - check if outputs are similar
        return semantic_similarity(output1, output2)
    
    if len(numbers1) != len(numbers2):
        return 0.0
    
    if not numbers1:
        return 1.0
    
    # Compare each number
    matches = sum(
        1 for n1, n2 in zip(numbers1, numbers2) 
        if abs(n1 - n2) <= tolerance
    )
    
    return matches / len(numbers1)


def information_fidelity(
    output1: str, 
    output2: str, 
    expected_fields: Optional[List[str]] = None
) -> float:
    """
    Calculate information fidelity (F1 score on key information).
    
    Args:
        output1: First output string
        output2: Second output string
        expected_fields: List of field names to check (optional)
    
    Returns:
        F1 score between 0.0 and 1.0
    """
    # If no specific fields, use semantic similarity
    if not expected_fields:
        return semantic_similarity(output1, output2)
    
    # Extract key information from both outputs
    info1 = _extract_key_info(output1, expected_fields)
    info2 = _extract_key_info(output2, expected_fields)
    
    # Calculate F1 score
    intersection = info1 & info2
    precision = len(intersection) / len(info2) if info2 else 0.0
    recall = len(intersection) / len(info1) if info1 else 0.0
    
    if precision + recall == 0:
        return 0.0
    f1 = 2 * (precision * recall) / (precision + recall)
    return f1


def _extract_key_info(text: str, fields: List[str]) -> Set[str]:
    """Extract key information based on field names"""
    info = set()
    text_lower = text.lower()
    
    for field in fields:
        # Look for field name followed by value
        pattern = rf'\b{re.escape(field.lower())}\s*[:=]\s*([^\s,;]+)'
        matches = re.findall(pattern, text_lower)
        info.update(matches)
        
        # Also look for field values in common formats
        pattern2 = rf'{re.escape(field.lower())}\s*([^\s,;]+)'
        matches2 = re.findall(pattern2, text_lower)
        info.update(matches2)
    
    return info

