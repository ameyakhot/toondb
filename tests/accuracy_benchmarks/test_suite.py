"""
Predefined test cases for accuracy benchmarks.
Each test case defines a task, evaluation method, and optional ground truth.
"""
from typing import Dict, List, Any, Optional
from dataclasses import dataclass


@dataclass
class TestCase:
    """Single test case definition"""
    name: str
    task: str
    evaluation_method: str  # "semantic_similarity", "extraction", "numeric", "exact_match"
    ground_truth: Optional[Any] = None
    field: Optional[str] = None  # For extraction tasks (e.g., "emails", "numbers")
    tolerance: Optional[float] = None  # For numeric tasks


# Predefined test cases
TEST_CASES: List[TestCase] = [
    TestCase(
        name="Email Extraction",
        task="List all email addresses from the user data. Return ONLY the email addresses, one per line. Do not include any explanations, code, or additional text.",
        evaluation_method="extraction",
        field="emails",
        ground_truth=None  # Will be extracted from data
    ),
    TestCase(
        name="Data Summarization",
        task="Summarize the user data in exactly 2 sentences. Include key statistics like total count and main characteristics. Do not provide code or examples.",
        evaluation_method="semantic_similarity",
    ),
    TestCase(
        name="Filtering - Age",
        task="Find all users with age greater than 30 (not including 30). List their names and ages only. Format: Name, Age (one per line). Do not provide code or explanations.",
        evaluation_method="semantic_similarity",
    ),
    TestCase(
        name="Aggregation - Average Age",
        task="Calculate the average age of all users. Return ONLY the number, nothing else. No code, no explanations, no text. Just the numeric value.",
        evaluation_method="numeric",
        tolerance=0.1
    ),
    TestCase(
        name="Multi-field Query",
        task="Find users with role='admin' and age > 25. List their names, roles, and ages only. Format: Name, Role, Age (one per line). Do not provide code or explanations.",
        evaluation_method="semantic_similarity",
    ),
    TestCase(
        name="Count Operations",
        task="How many users have role='user'? Return ONLY the number. No code, no explanations, no text. Just the number.",
        evaluation_method="numeric",
        tolerance=0.0  # Exact match for counts
    ),
    TestCase(
        name="Field Extraction - Roles",
        task="Extract all unique roles from the data. Return ONLY a comma-separated list of roles. No explanations, no code, no additional text.",
        evaluation_method="semantic_similarity",
    ),
    TestCase(
        name="Data Structure Understanding",
        task="Describe the structure of the data. What fields are present and what types of values do they contain? Provide a clear description without code examples.",
        evaluation_method="semantic_similarity",
    ),
]


def get_test_cases() -> List[TestCase]:
    """Get all predefined test cases"""
    return TEST_CASES.copy()


def get_test_case_by_name(name: str) -> Optional[TestCase]:
    """Get a specific test case by name"""
    for test_case in TEST_CASES:
        if test_case.name == name:
            return test_case
    return None


def create_prompt(task: str, data: str, format_type: str = "JSON") -> str:
    """
    Create a prompt for the LLM with task and data.
    
    Args:
        task: The task description
        data: The data in JSON or TOON format
        format_type: "JSON" or "TOON"
    
    Returns:
        Complete prompt string
    """
    if format_type == "TOON":
        return f"""{task}

Here is the data in TOON format:
{data}

Please provide your response based on this data."""
    else:
        return f"""{task}

Here is the data in JSON format:
{data}

Please provide your response based on this data."""

