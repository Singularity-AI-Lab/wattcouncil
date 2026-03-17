"""
Council utility functions shared across multiple council members.
"""
import re


def extract_json(text: str) -> str:
    """
    Extract JSON from LLM output that may contain markdown or extra text.
    Handles markdown code blocks and both JSON objects and arrays.
    
    Args:
        text: Raw LLM output
    
    Returns:
        Extracted JSON string
    """
    # Remove <think>...</think> blocks if present
    text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
    
    # Try to find JSON in markdown code block
    json_match = re.search(r'```(?:json)?\s*(\{.*?\}|\[.*?\])\s*```', text, re.DOTALL)
    if json_match:
        return json_match.group(1).strip()
    
    # Try to find raw JSON array or object
    # Look for first '[' or '{' and last ']' or '}'
    start_markers = [(text.find('['), '['), (text.find('{'), '{')]
    start_markers = [(idx, marker) for idx, marker in start_markers if idx != -1]
    
    if start_markers:
        start_idx, start_marker = min(start_markers)
        end_marker = ']' if start_marker == '[' else '}'
        end_idx = text.rfind(end_marker)
        
        if end_idx > start_idx:
            return text[start_idx:end_idx+1].strip()

    # Try to find raw JSON object (fallback)
    json_match = re.search(r'({\s*".*?})', text, re.DOTALL)
    if json_match:
        return json_match.group(1)
        
    # Final fallback: Try to interpret as Python literal (handles single quotes, etc.)
    try:
        import ast
        import json
        # Find start/end brackets again to minimize noise
        start_markers = [(text.find('['), '['), (text.find('{'), '{')]
        start_markers = [(idx, marker) for idx, marker in start_markers if idx != -1]
        
        if start_markers:
            start_idx, start_marker = min(start_markers)
            end_marker = ']' if start_marker == '[' else '}'
            end_idx = text.rfind(end_marker)
            
            if end_idx > start_idx:
                candidate = text[start_idx:end_idx+1]
                # ast.literal_eval handles Python dict syntax (single quotes, True/False capitalized)
                obj = ast.literal_eval(candidate)
                return json.dumps(obj)
    except Exception:
        pass

    # Return as-is and let JSON parser handle it
    return text.strip()


def get_stage_folder(stage: str) -> str:
    """
    Map stage name to folder name for 3-stage pipeline.
    
    Args:
        stage: Stage name (e.g., "Stage 1", "Stage1", etc.)
    
    Returns:
        Folder name for the stage
    """
    # Normalize stage name to extract number
    stage_lower = stage.lower().replace(" ", "")
    
    # Stage 1: Families (with embedded work regimes)
    if "1" in stage_lower or "family" in stage_lower or "families" in stage_lower:
        return "stage1_family"  # Singular to match prompt folder
    # Stage 2: Weather Profiles (ranges + hourly combined)
    elif "2" in stage_lower or "weather" in stage_lower:
        return "stage2_weather"
    # Stage 3: Consumption
    elif "3" in stage_lower or "consumption" in stage_lower:
        return "stage3_consumption"
    else:
        # Default fallback
        return "unknown_stage"
