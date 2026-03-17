"""
Pipeline helper utilities for main.py execution.

Contains functions for logging setup, output path management, and file saving.
"""
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

from utils.output_formatting import colored, Colors, print_success


def setup_logging(output_dir: Path) -> Path:
    """
    Setup file and terminal logging inside the run's output directory.
    
    Args:
        output_dir: Run-specific output directory
        
    Returns:
        Path to the log directory for this run.
    """
    log_dir = output_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = log_dir / "pipeline.log"
    
    # File Handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    
    # Terminal Handler (Managed by utils, but we configure root here)
    logging.basicConfig(level=logging.INFO, handlers=[file_handler])
    
    return log_dir


def get_output_paths(runtime_config: Dict[str, Any], output_dir: Path) -> Dict[str, Path]:
    """
    Get output paths from configuration and create raw directories.
    
    Args:
        runtime_config: Runtime configuration dictionary
        output_dir: Run-specific output directory
        
    Returns:
        Dictionary with:
            - stage{N}_combined: Path to combined output file
            - stage{N}_raw: Path to raw output directory
    """
    output_config = runtime_config.get("pipeline", {}).get("output", {})
    
    paths = {}
    
    # Combined output files
    for stage_key in ["stage1_combined", "stage2_combined", "stage3_combined", "stage4_combined", "stage5_combined"]:
        filename = output_config.get(stage_key, f"{stage_key}.json")
        paths[stage_key] = output_dir / filename
    
    # Raw output directories
    raw_dirs = output_config.get("raw_dirs", {})
    for stage_key, dir_name in raw_dirs.items():
        raw_dir = output_dir / dir_name
        raw_dir.mkdir(parents=True, exist_ok=True)
        paths[f"{stage_key}_raw"] = raw_dir
    
    return paths


def save_combined_output(filepath: Path, data: Any) -> None:
    """
    Save combined stage output to JSON file.
    
    Args:
        filepath: Path to save the output
        data: Data to save (dict or list)
    """
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    
    print_success(f"Saved: {colored(filepath.name, Colors.CYAN)}", 1)


def save_raw_outputs(raw_dir: Path, items: list, filename_fn: callable) -> None:
    """
    Save individual items to raw output directory.
    
    Args:
        raw_dir: Raw output directory
        items: List of items to save
        filename_fn: Function that takes (item, index, timestamp) and returns filename
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    for idx, item in enumerate(items):
        filename = filename_fn(item, idx, timestamp)
        filepath = raw_dir / filename
        with open(filepath, "w") as f:
            json.dump(item, f, indent=2)


def display_log_analysis(output_dir: Path, log_dir: Path):
    """
    Display colored log analysis summary at the end of pipeline execution.
    
    Args:
        output_dir: Run output directory
        log_dir: Logs directory containing pipeline.log
    """
    from scripts.analyze_logs import (
        load_model_roles_from_config,
        parse_log_file
    )
    from utils.output_formatting import (
        print_header, print_section, print_info, print_success, print_warning,
        Icons, Colors, colored
    )
    
    print_header(f"{Icons.CHART} PIPELINE STATISTICS")
    
    try:
        # Load model roles from config snapshot
        model_roles = load_model_roles_from_config(output_dir)
        
        # Parse log file - pass directory so it can find API JSON logs
        members = parse_log_file(log_dir, model_roles)
        
        if not members:
            print_warning("No API call data found in logs")
            return
        
        # Sort by role for display
        sorted_members = sorted(members.values(), key=lambda x: (x.role, x.name))
        
        # Calculate totals
        total_calls = sum(m.calls for m in sorted_members)
        total_time = sum(m.total_time for m in sorted_members)
        total_input_tokens = sum(m.total_input_tokens for m in sorted_members)
        total_output_tokens = sum(m.total_output_tokens for m in sorted_members)
        total_tokens = total_input_tokens + total_output_tokens
        total_cost = sum(m.total_cost for m in sorted_members)
        
        # Display summary
        print_section("Council Member Statistics", Icons.INFO)
        
        # Header
        print_info("")
        if total_tokens > 0:
            # Show token information with input/output breakdown
            header = f"{'Role':<18} {'Model':<35} {'Calls':<6} {'Time (s)':<10} {'In Tokens':<12} {'Out Tokens':<12} {'Cost ($)':<10}"
        else:
            # Backward compatibility for old runs without token data
            header = f"{'Role':<18} {'Model':<35} {'Calls':<6} {'Time (s)':<10} {'Chars':<12}"
        
        print_info(colored(header, Colors.CYAN))
        print_info(colored("─" * len(header), Colors.DIM))
        
        # Data rows
        for member in sorted_members:
            # Truncate model name if too long
            model_display = member.name
            if len(model_display) > 35:
                model_display = model_display[:32] + "..."
            
            if total_tokens > 0:
                # Show token breakdown and cost information
                row = (f"{member.role:<18} {model_display:<35} {member.calls:<6} "
                       f"{member.total_time:<10.1f} {member.total_input_tokens:<12,} "
                       f"{member.total_output_tokens:<12,} {member.total_cost:<10.4f}")
            else:
                # Show character count for old runs
                row = (f"{member.role:<18} {model_display:<35} {member.calls:<6} "
                       f"{member.total_time:<10.1f} {member.total_chars:<12,}")
            
            print_info(row, indent=1)
        
        # Summary row
        print_info(colored("─" * len(header), Colors.DIM))
        if total_tokens > 0:
            summary = (f"{'TOTAL':<17} {'':<35} {total_calls:<6} "
                      f"{total_time:<10.1f} {total_input_tokens:<12,} "
                      f"{total_output_tokens:<12,} {total_cost:<10.4f}")
        else:
            total_chars = sum(m.total_chars for m in sorted_members)
            summary = (f"{'TOTAL':<17} {'':<32} {total_calls:<6} "
                      f"{total_time:<10.1f} {total_chars:<12,}")
        
        print_success(summary)
        print_info("")
        
        # Additional insights
        avg_time_per_call = total_time / total_calls if total_calls > 0 else 0
        print_info(f"Average time per call: {colored(f'{avg_time_per_call:.2f}s', Colors.YELLOW)}", indent=1)
        
        if total_tokens > 0:
            avg_cost = total_cost / total_calls if total_calls > 0 else 0
            print_info(f"Average cost per call: {colored(f'${avg_cost:.4f}', Colors.YELLOW)}", indent=1)
            print_info(f"Total pipeline cost:   {colored(f'${total_cost:.4f}', Colors.GREEN)}", indent=1)
        
        print_info("")
        
    except Exception as e:
        print_warning(f"Could not parse log statistics: {e}")

