"""
Config snapshot utility - Copies configuration files to run directory for reproducibility.
"""
import shutil
from pathlib import Path
from typing import List
import logging

logger = logging.getLogger(__name__)


def snapshot_configs(output_dir: Path, config_files: List[str] = None) -> Path:
    """
    Copy all configuration files to the run directory for reproducibility.
    
    This ensures that even if main configs change, we can always see exactly
    what configuration was used for a specific run.
    
    Args:
        output_dir: The run output directory (e.g., outputs/run_20260112_153619)
        config_files: Optional list of config file paths to copy. 
                     If None, copies all .yaml files from config/ directory.
    
    Returns:
        Path to the config snapshot directory
    """
    # Create config subdirectory in output_dir
    config_snapshot_dir = output_dir / "config"
    config_snapshot_dir.mkdir(parents=True, exist_ok=True)
    
    if config_files is None:
        # Default: copy all YAML files from config/ directory
        config_dir = Path("config")
        if config_dir.exists():
            config_files = list(config_dir.glob("*.yaml"))
        else:
            logger.warning("config/ directory not found")
            return config_snapshot_dir
    
    # Copy each config file
    copied_count = 0
    for config_file in config_files:
        config_path = Path(config_file)
        if config_path.exists():
            dest_path = config_snapshot_dir / config_path.name
            shutil.copy2(config_path, dest_path)
            logger.info(f"Copied config: {config_path.name}")
            copied_count += 1
        else:
            logger.warning(f"Config file not found: {config_file}")
    
    logger.info(f"Snapshot complete: {copied_count} config files copied to {config_snapshot_dir}")
    return config_snapshot_dir
