from toon import encode, decode
from typing import List, Dict, Any
from datetime import datetime, date

def to_toon(data: List[Dict]) -> str:
    """
    Convert query results to TOON format.

    Args: 
        data: List of dictionaries (query results)

    Returns:
        str: TOON formattedstring
    """

    cleaned_data = _clean_data(data)
    return encode(cleaned_data)

def from_toon(toon_string: str) -> List[Dict]:
    """
    Convert TOON format back to Python data structure.

    Args:
        toon_string: TOON formatted string

    Returns:
        List of dictionaries
    """
    return decode(toon_string)

def _clean_data(data: List[Dict]) -> List[Dict]:
    """
    Clean data for TOON encoding
    """
    cleaned = []
    for item in data: 
        cleaned_item = {}
        for key, value in item.items():
            if value is None:
                cleaned_item[key] = None
            elif isinstance(value, (datetime, date)):
                cleaned_item[key] = value.isoformat()
            else:
                cleaned_item[key] = value
        cleaned.append(cleaned_item)
    
    return cleaned