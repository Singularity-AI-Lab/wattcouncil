"""
Fine-grained checkpoint helpers for resuming from raw outputs.

These functions scan raw output directories and determine which items
have already been generated, allowing the pipeline to skip them and
save API costs.
"""
import json
import re
from pathlib import Path
from typing import Set, Tuple, List, Dict


def scan_stage1_checkpoints(raw_dir: Path) -> Set[Tuple[str, int]]:
    """
    Scan Stage 1 raw directory for completed families.
    Validates files and excludes corrupted ones.
    
    Args:
        raw_dir: Raw output directory for Stage 1
        
    Returns:
        Set of (household_id, variant) tuples that have been generated
    """
    if not raw_dir.exists():
        return set()
    
    completed = set()
    pattern = re.compile(r'^(.+?)_var(\d+)_\d{8}_\d{6}\.json$')
    
    for file in raw_dir.glob('*.json'):
        match = pattern.match(file.name)
        if match and validate_checkpoint_file(file):
            household_id = match.group(1)
            variant = int(match.group(2))
            completed.add((household_id, variant))
    
    return completed


def scan_stage2_checkpoints(raw_dir: Path) -> Set[Tuple[int, str]]:
    """
    Scan Stage 2 raw directory for completed weather range variations.
    
    Args:
        raw_dir: Raw output directory for Stage 2
        
    Returns:
        Set of (variation, season) tuples that have been generated
    """
    if not raw_dir.exists():
        return set()
    
    completed = set()
    pattern = re.compile(r'^variation(\d+)_([^_]+)_\d{8}_\d{6}\.json$')
    
    for file in raw_dir.glob('*.json'):
        match = pattern.match(file.name)
        if match and validate_checkpoint_file(file):
            variation = int(match.group(1))
            season = match.group(2)
            completed.add((variation, season))
    
    return completed


def scan_stage3_checkpoints(raw_dir: Path) -> Set[Tuple[str, int, str, int, str, int]]:
    """
    Scan Stage 5 raw directory for completed consumption profiles.
    
    Args:
        raw_dir: Raw output directory for Stage 5
        
    Returns:
        Set of (family_id, work_var, season, s3_var, day_type, variant) tuples that have been generated
    """
    if not raw_dir.exists():
        return set()
    
    completed = set()
    # New format with s3var: FamilyID_workXX_s3varXX_Season_DayType_varXX_timestamp.json
    pattern_s3var = re.compile(r'^(.+?)_work(\d+)_s3var(\d+)_([^_]+)_([^_]+)_var(\d+)_\d{8}_\d{6}\.json$')
    # Format without s3var: FamilyID_workXX_Season_DayType_varXX_timestamp.json
    pattern_work = re.compile(r'^(.+?)_work(\d+)_([^_]+)_([^_]+)_var(\d+)_\d{8}_\d{6}\.json$')
    # Legacy format without work variant: ID_Season_DayType_varXX_timestamp.json
    pattern_legacy = re.compile(r'^(.+?)_([^_]+)_([^_]+)_var(\d+)_\d{8}_\d{6}\.json$')
    
    for file in raw_dir.glob('*.json'):
        # Try s3var pattern first (newest format)
        match = pattern_s3var.match(file.name)
        if match and validate_checkpoint_file(file):
            family_id = match.group(1)
            work_var = int(match.group(2))
            s3_var = int(match.group(3))
            season = match.group(4)
            day_type = match.group(5)
            variant = int(match.group(6))
            completed.add((family_id, work_var, season, s3_var, day_type, variant))
            continue
        
        # Try work pattern (assume s3_var=1 for backward compatibility)
        match = pattern_work.match(file.name)
        if match and validate_checkpoint_file(file):
            family_id = match.group(1)
            work_var = int(match.group(2))
            season = match.group(3)
            day_type = match.group(4)
            variant = int(match.group(5))
            completed.add((family_id, work_var, season, 1, day_type, variant))
            continue
            
        # Try legacy pattern (assume work_var=1, s3_var=1 for backward compatibility)
        match = pattern_legacy.match(file.name)
        if match and validate_checkpoint_file(file):
            family_id = match.group(1)
            season = match.group(2)
            day_type = match.group(3)
            variant = int(match.group(4))
            # Default to work_var=1, s3_var=1 for legacy files
            completed.add((family_id, 1, season, 1, day_type, variant))
    
    return completed


def load_checkpoint_data(raw_dir: Path, pattern: str) -> List[Dict]:
    """
    Load all data from checkpoint files matching a pattern.
    Skips corrupted files and logs warnings.
    
    Args:
        raw_dir: Raw output directory
        pattern: Glob pattern for files
        
    Returns:
        List of parsed JSON data from all matching files
    """
    import logging
    logger = logging.getLogger(__name__)
    
    if not raw_dir.exists():
        return []
    
    data = []
    corrupted_files = []
    
    for file in sorted(raw_dir.glob(pattern)):
        try:
            with open(file, 'r') as f:
                item = json.load(f)
                data.append(item)
        except json.JSONDecodeError as e:
            # JSON is corrupted - log and skip
            corrupted_files.append(file.name)
            logger.warning(f"Corrupted checkpoint file (will regenerate): {file.name} - {e}")
        except Exception as e:
            # Other errors - log and skip
            corrupted_files.append(file.name)
            logger.error(f"Failed to load checkpoint file: {file.name} - {e}")
    
    if corrupted_files:
        logger.info(f"Skipped {len(corrupted_files)} corrupted checkpoint files - they will be regenerated")
    
    return data


def validate_checkpoint_file(filepath: Path) -> bool:
    """
    Validate that a checkpoint file is not corrupted.
    
    Args:
        filepath: Path to checkpoint file
        
    Returns:
        True if valid, False if corrupted
    """
    try:
        with open(filepath, 'r') as f:
            json.load(f)
        return True
    except Exception:
        return False
