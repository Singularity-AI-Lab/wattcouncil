#!/usr/bin/env python3
"""
Log Analysis Script - Extract Council Member Statistics

Parses pipeline logs to generate summary statistics for each council member:
- Number of calls
- Total execution time
- Average execution time
- Total characters generated
- Average characters per call

Usage:
    python scripts/analyze_logs.py outputs/run_20260112_153619/logs/pipeline.log
    python scripts/analyze_logs.py outputs/run_20260112_153619/logs/pipeline.log --format markdown
"""
import argparse
import json
import re
import yaml
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Tuple


class CouncilMember:
    """Represents statistics for a council member (model)."""
    
    def __init__(self, name: str, role: str):
        self.name = name
        self.role = role
        self.calls = 0
        self.total_time = 0.0
        self.total_chars = 0
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.total_cost = 0.0
        self.execution_times: List[float] = []
        self.char_counts: List[int] = []
    
    def add_call(self, execution_time: float, char_count: int, 
                 input_tokens: int = 0, output_tokens: int = 0, cost: float = 0.0):
        """Record a single API call."""
        self.calls += 1
        self.total_time += execution_time
        self.total_chars += char_count
        self.total_input_tokens += input_tokens
        self.total_output_tokens += output_tokens
        self.total_cost += cost
        self.execution_times.append(execution_time)
        self.char_counts.append(char_count)
    
    @property
    def avg_time(self) -> float:
        """Average execution time per call."""
        return self.total_time / self.calls if self.calls > 0 else 0.0
    
    @property
    def avg_chars(self) -> float:
        """Average characters per call."""
        return self.total_chars / self.calls if self.calls > 0 else 0.0
    
    @property
    def total_tokens(self) -> int:
        """Total tokens (input + output)."""
        return self.total_input_tokens + self.total_output_tokens
    
    @property
    def avg_tokens(self) -> float:
        """Average tokens per call."""
        return self.total_tokens / self.calls if self.calls > 0 else 0.0


def load_model_roles_from_config(run_dir: Path) -> Dict[str, str]:
    """
    Load model-to-role mapping from snapshotted config in run directory.
    
    Args:
        run_dir: Path to the run directory containing config/ snapshot
    
    Returns:
        Dictionary mapping model name to role name
    """
    config_snapshot = run_dir / "config" / "models.yaml"
    
    if not config_snapshot.exists():
        # Fallback: check if log file path contains run directory
        # Try parent directory (in case log_file was passed)
        parent_config = run_dir.parent / "config" / "models.yaml"
        if parent_config.exists():
            config_snapshot = parent_config
        else:
            return get_default_model_roles()
    
    try:
        with open(config_snapshot, 'r') as f:
            config = yaml.safe_load(f)
        
        roles_config = config.get('roles', {})
        model_roles = {}
        
        for role_name, role_config in roles_config.items():
            model = role_config.get('model')
            if model:
                # Convert role name to title case (e.g., "generator" -> "Generator")
                display_role = role_name.replace('_', ' ').title()
                model_roles[model] = display_role
        
        return model_roles
    
    except Exception as e:
        print(f"Warning: Could not load model roles from config: {e}")
        return get_default_model_roles()


def get_default_model_roles() -> Dict[str, str]:
    """
    Fallback hardcoded model-to-role mapping for backward compatibility.
    Used when config snapshot is not available.
    """
    return {
        "google/gemini-2.5-flash": "Generator",
        "meta-llama/Llama-4-Maverick-17B-128E-Instruct-FP8": "Cultural Auditor",
        "anthropic/claude-4-sonnet": "Physical Auditor",
        "moonshotai/Kimi-K2-Instruct-0905": "CEO",
        "meta-llama/Llama-3.3-70B-Instruct": "Editor",
        "mistralai/Mistral-Small-3.2-24B-Instruct-2506": "Approver",
    }


def parse_log_file(log_path: Path, model_roles: Dict[str, str]) -> Dict[str, CouncilMember]:
    """
    Parse log file and extract statistics for each council member.
    Tries to parse API JSON log first (newer format with tokens),
    falls back to text log if JSON not available.
    
    Pre-initializes all configured roles from model_roles to ensure they appear
    in statistics even if they weren't called during the run.
    
    Args:
        log_path: Path to pipeline.log file or directory containing logs
        model_roles: Dictionary mapping model names to role names
    
    Returns:
        Dictionary mapping model name to CouncilMember statistics
    """
    # Pre-initialize all configured members with 0 calls
    members = {}
    for model, role in model_roles.items():
        members[model] = CouncilMember(model, role)
    
    # If a directory is provided, look for API JSON log
    if log_path.is_dir():
        json_logs = sorted(log_path.glob("api_calls_*.json"))
        
        # If not found, check logs subdirectory
        if not json_logs:
            json_logs = sorted((log_path / "logs").glob("api_calls_*.json"))
            
        if json_logs:
            # Parse and aggregate ALL API call logs (original + resume runs)
            print(f"Found {len(json_logs)} API call log(s), aggregating statistics...")
            for json_log in json_logs:
                parse_api_json_log(json_log, members)
            return members
        
        # Fall back to pipeline.log in the directory
        text_log = log_path / "pipeline.log"
        if not text_log.exists():
            text_log = log_path / "logs" / "pipeline.log"
            
        if text_log.exists():
            parse_text_log(text_log, members)
            return members
        return members
    
    # If a file is provided, detect type
    if log_path.suffix == ".json":
        parse_api_json_log(log_path, members)
        return members
    
    # Otherwise try as text log (or if JSON was empty)
    if log_path.suffix == ".log" or not log_path.suffix:
        # Check for API JSON log in same directory
        json_logs = sorted(log_path.parent.glob("api_calls_*.json"))
        if json_logs:
            print(f"Found {len(json_logs)} API call log(s), aggregating statistics...")
            for json_log in json_logs:
                parse_api_json_log(json_log, members)
            return members
        
        # Fall back to text log
        parse_text_log(log_path, members)
        return members
    
    return members


def normalize_role_name(role: str) -> str:
    """
    Normalize a role name for comparison by sorting the parts alphabetically.
    This handles mismatches like 'cultural_auditor' vs 'auditor_cultural'.
    
    Examples:
        'auditor_cultural' -> 'auditor cultural'
        'cultural_auditor' -> 'auditor cultural'
        'physical_auditor' -> 'auditor physical'
        'auditor_physical' -> 'auditor physical'
        'generator' -> 'generator'
        'ceo' -> 'ceo'
    """
    parts = role.replace('_', ' ').lower().split()
    return ' '.join(sorted(parts))


def parse_api_json_log(log_path: Path, members: Dict[str, CouncilMember]) -> None:
    """
    Parse API JSON log file (newer format with token information).
    Updates the provided members dictionary in-place.
    
    Args:
        log_path: Path to api_calls_*.json file
        members: Pre-initialized dictionary of CouncilMembers to update
    """
    try:
        with open(log_path, 'r') as f:
            api_calls = json.load(f)
        
        if not api_calls:
            print(f"Warning: JSON log {log_path} is empty or invalid format")
            return

        for call in api_calls:
            model = call.get("model")
            metadata = call.get("metadata", {})
            
            # Try to get role from metadata first (new format with role tracking)
            role_from_metadata = metadata.get("role")
            
            # Find the member to update
            target_member = None
            
            if role_from_metadata:
                # Normalize the role from metadata for matching
                normalized_metadata_role = normalize_role_name(role_from_metadata)
                
                # Find member by normalized role comparison
                for member in members.values():
                    # Normalize the member's role for comparison
                    normalized_member_role = normalize_role_name(member.role)
                    
                    if normalized_metadata_role == normalized_member_role:
                        target_member = member
                        break
            
            # Fallback: find by model name (backward compatibility)
            if not target_member and model and model in members:
                target_member = members[model]
            
            if not target_member:
                continue
            
            # Extract metrics
            duration = call.get("duration_seconds", 0.0)
            char_count = len(call.get("response", ""))
            input_tokens = call.get("tokens", {}).get("input", 0)
            output_tokens = call.get("tokens", {}).get("output", 0)
            cost = call.get("cost_usd", {}).get("total", 0.0)
            
            target_member.add_call(duration, char_count, input_tokens, output_tokens, cost)
    
    except (json.JSONDecodeError, FileNotFoundError) as e:
        # Attempt to recover incomplete JSON (e.g., if run is still in progress)
        try:
            with open(log_path, 'r') as f:
                content = f.read().strip()
            
            # If it doesn't end with ']', try appending it
            if content and not content.endswith(']'):
                # Handle potential trailing comma
                if content.endswith(','):
                    content = content[:-1]
                
                # Append closing bracket
                content += ']'
                
                api_calls = json.loads(content)
                
                # If successful, process the calls (same logic as above)
                for call in api_calls:
                    # Logic duplicated to avoid massive refactor - extracting to helper would be better but keeping it simple for now
                    model = call.get("model")
                    metadata = call.get("metadata", {})
                    role_from_metadata = metadata.get("role")
                    target_member = None
                    
                    if role_from_metadata:
                        normalized_metadata_role = normalize_role_name(role_from_metadata)
                        for member in members.values():
                            normalized_member_role = normalize_role_name(member.role)
                            if normalized_metadata_role == normalized_member_role:
                                target_member = member
                                break
                    
                    if not target_member and model and model in members:
                        target_member = members[model]
                    
                    if not target_member:
                        continue
                    
                    duration = call.get("duration_seconds", 0.0)
                    char_count = len(call.get("response", ""))
                    input_tokens = call.get("tokens", {}).get("input", 0)
                    output_tokens = call.get("tokens", {}).get("output", 0)
                    cost = call.get("cost_usd", {}).get("total", 0.0)
                    
                    target_member.add_call(duration, char_count, input_tokens, output_tokens, cost)
                
                return # Success after repair

        except Exception as e2:
            # Still failed, nothing we can do
            pass
            
        # Silently fail - members dict remains with 0s for all roles
        pass


def parse_text_log(log_path: Path, members: Dict[str, CouncilMember]) -> None:
    """
    Parse text pipeline.log file (older format without token information).
    Updates the provided members dictionary in-place.
    
    Args:
        log_path: Path to pipeline.log file
        members: Pre-initialized dictionary of CouncilMembers to update
    """
    current_model = None
    
    # Regex patterns
    model_call_pattern = re.compile(r"Calling \w+ with model (.+)$")
    generation_pattern = re.compile(r"Successfully generated (\d+) characters in ([\d.]+)s")
    
    with open(log_path, 'r') as f:
        for line in f:
            # Check for model call
            model_match = model_call_pattern.search(line)
            if model_match:
                current_model = model_match.group(1)
                # Only process if this model is in our pre-initialized members
                if current_model not in members:
                    current_model = None
                continue
            
            # Check for generation completion
            gen_match = generation_pattern.search(line)
            if gen_match and current_model:
                char_count = int(gen_match.group(1))
                execution_time = float(gen_match.group(2))
                members[current_model].add_call(execution_time, char_count)
                current_model = None  # Reset for next call


def print_summary_table(members: Dict[str, CouncilMember], format_type: str = "text"):
    """
    Print summary table of council member statistics.
    
    Args:
        members: Dictionary of council members
        format_type: Output format - "text", "markdown", or "csv"
    """
    if not members:
        print("No data found in log file.")
        return
    
    # Sort by role then model name for consistent output
    sorted_members = sorted(members.values(), key=lambda x: (x.role, x.name))
    
    if format_type == "markdown":
        print_markdown_table(sorted_members)
    elif format_type == "csv":
        print_csv_table(sorted_members)
    else:
        print_text_table(sorted_members)


def print_text_table(members: List[CouncilMember]):
    """Print text-formatted table."""
    # Header
    print("\n" + "=" * 130)
    print(f"{'Role':<20} {'Model':<50} {'Calls':<8} {'Total Time':<12} {'Avg Time':<12} {'Total Chars':<12} {'Avg Chars':<12}")
    print("=" * 160)
    
    # Data rows
    total_calls = 0
    total_time = 0.0
    total_chars = 0
    
    for member in members:
        print(f"{member.role:<20} {member.name:<50} {member.calls:<8} "
              f"{member.total_time:<12.2f} {member.avg_time:<12.2f} "
              f"{member.total_chars:<12} {member.avg_chars:<12.0f}")
        total_calls += member.calls
        total_time += member.total_time
        total_chars += member.total_chars
    
    # Summary row
    print("=" * 160)
    print(f"{'TOTAL':<20} {'':<50} {total_calls:<8} "
          f"{total_time:<12.2f} {total_time/total_calls if total_calls > 0 else 0:<12.2f} "
          f"{total_chars:<12} {total_chars/total_calls if total_calls > 0 else 0:<12.0f}")
    print("=" * 160 + "\n")


def print_markdown_table(members: List[CouncilMember]):
    """Print markdown-formatted table."""
    print("\n## Council Member Statistics\n")
    print("| Role | Model | Calls | Total Time (s) | Avg Time (s) | Total Chars | Avg Chars |")
    print("|------|-------|-------|----------------|--------------|-------------|-----------|")
    
    total_calls = 0
    total_time = 0.0
    total_chars = 0
    
    for member in members:
        print(f"| {member.role} | `{member.name}` | {member.calls} | "
              f"{member.total_time:.2f} | {member.avg_time:.2f} | "
              f"{member.total_chars} | {member.avg_chars:.0f} |")
        total_calls += member.calls
        total_time += member.total_time
        total_chars += member.total_chars
    
    print(f"| **TOTAL** | | **{total_calls}** | "
          f"**{total_time:.2f}** | **{total_time/total_calls if total_calls > 0 else 0:.2f}** | "
          f"**{total_chars}** | **{total_chars/total_calls if total_calls > 0 else 0:.0f}** |")
    print()


def print_csv_table(members: List[CouncilMember]):
    """Print CSV-formatted table."""
    print("Role,Model,Calls,Total Time (s),Avg Time (s),Total Chars,Avg Chars")
    
    for member in members:
        print(f"{member.role},{member.name},{member.calls},"
              f"{member.total_time:.2f},{member.avg_time:.2f},"
              f"{member.total_chars},{member.avg_chars:.0f}")


def main():
    parser = argparse.ArgumentParser(
        description="Analyze council pipeline logs and generate statistics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze specific run
  python scripts/analyze_logs.py outputs/run_20260112_153619/logs/pipeline.log
  
  # Generate markdown table
  python scripts/analyze_logs.py outputs/run_20260112_153619/logs/pipeline.log --format markdown
  
  # Generate CSV output
  python scripts/analyze_logs.py outputs/run_20260112_153619/logs/pipeline.log --format csv
        """
    )
    
    parser.add_argument(
        "log_file",
        type=Path,
        help="Path to pipeline.log file"
    )
    
    parser.add_argument(
        "--format",
        choices=["text", "markdown", "csv"],
        default="text",
        help="Output format (default: text)"
    )
    
    args = parser.parse_args()
    
    # Validate log file exists
    if not args.log_file.exists():
        print(f"Error: Log file not found: {args.log_file}")
        return 1
    
    # Determine run directory to load model roles
    log_path = args.log_file
    if log_path.is_dir():
        run_dir = log_path
    elif "run_" in str(log_path):
        # Extract run directory from log file path
        # Example: outputs/run_20260112_153619/logs/pipeline.log -> outputs/run_20260112_153619
        parts = log_path.parts
        for i, part in enumerate(parts):
            if part.startswith("run_"):
                run_dir = Path(*parts[:i+1])
                break
        else:
            # Couldn't find run directory, use current directory
            run_dir = Path(".")
    else:
        run_dir = Path(".")
    
    # Load model roles from config snapshot (or use defaults)
    model_roles = load_model_roles_from_config(run_dir)
    
    # Parse log file
    members = parse_log_file(args.log_file, model_roles)
    
    # Print summary
    print(f"\nAnalyzing: {args.log_file}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print_summary_table(members, args.format)
    
    return 0


if __name__ == "__main__":
    exit(main())
