import json
from typing import Any, Dict

def read_json(json_path: str) -> Dict[str, Any]:
    """
    Reads a JSON file and returns a dictionary.
    Args:
        json_path: path to the JSON file
    Returns:
        A dictionary of JSON data.
    """
    with open(json_path, 'r') as f:
        return json.load(f)