#!/usr/bin/env python3
"""
Sequential Consumption Generator (Step 2)

Generates daily energy consumption profiles for a specific family based on the 
chronological weather sequence extracted in Step 1.

COMPARISON WITH MAIN PIPELINE (STAGE 5):
----------------------------------------
1.  **Strict Chronology vs. Combinatorics**:
    -   *Main Pipeline*: Generates variations combinatorially (Family x Work Regime x Season x Profile).
        It picks a "Season" (e.g., Summer) and a "Day Type" (e.g., Weekday) and generates a representative day.
    -   *This Script*: Follows a strict date sequence (e.g., June 1st, June 2nd, June 3rd...).
        It injects the specific date and day name into the context, ensuring continuity.

2.  **Context Injection**:
    -   *Main Pipeline*: Context is generic (e.g., "Summer Weekday").
    -   *This Script*: Context is specific (e.g., "2009-06-01 (Monday)").
        This allows the LLM to understand week-long patterns (e.g., laundry on Saturday).

3.  **Input Source**:
    -   *Main Pipeline*: Reads from Stage 3/4 combined JSONs.
    -   *This Script*: Reads from `weather_sequence.json`, which is a specific time-series extract.

Usage:
    python scripts/generate_sequential_consumption.py --step generate --family-index 0
"""

import sys
import argparse
import json
import logging
import random
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from council.config import Config
from council.pipeline import initialize_council, run_council_pipeline
from utils.constants import CONSTANTS
from utils import (
    Icons, Colors, colored,
    print_header, print_section, print_success, print_warning, print_error,
    print_info, setup_logging
)

# Setup logging
logger = logging.getLogger(__name__)

def parse_arguments():
    parser = argparse.ArgumentParser(description="Generate sequential consumption profiles (Step 2)")
    
    # Kept for compatibility, though really only 'generate' is valid now
    parser.add_argument("--step", type=str, choices=["generate"], default="generate", help="Execution step (default: generate)")
    parser.add_argument("--family-id", type=str, help="Specific family ID to process")
    parser.add_argument("--family-index", type=int, help="Specific family index (0-based) to process (Prioritized)")
    parser.add_argument("--families-file", type=str, help="Path to specific stage1 families JSON file")
    
    # Output dir matches extraction script
    parser.add_argument("--output-dir", type=str, default="outputs/sequential_consumption", help="Output directory")
    
    return parser.parse_args()

def generate_consumption_sequence(args, country, family_data, weather_sequence_file, out_path, components):
    print_section("Step 2: Generating Consumption Sequence")
    
    if not weather_sequence_file.exists():
        print_error(f"Weather sequence file not found: {weather_sequence_file}")
        print_info("Run 'scripts/extract_weather_sequence.py' first to generate Step 1 data.")
        return
        
    with open(weather_sequence_file, "r") as f:
        data = json.load(f)
        if isinstance(data, dict) and "sequence" in data:
            weather_sequence = data["sequence"]
        else:
            weather_sequence = data
        
    family_id = family_data.get("household_id", "Unknown")
    
    # Ensure work regime exists (or use empty if not)
    work_regime = {}
    if "household_work_regime" in family_data:
        work_regime = {
            "household_id": family_id,
            "household_work_regime": family_data.get("household_work_regime"),
            "weekday_daytime_occupancy_level": family_data.get("weekday_daytime_occupancy_level"),
            "members": family_data.get("members")
        }
    else:
        print_warning("No work regime found in family data. Using empty regime.")

    generated_files = []
    
    for day_idx, day_weather in enumerate(weather_sequence):
        date_str = day_weather["date"]
        season = day_weather["season"]
        day_type = day_weather["day_type"]
        day_name = day_weather["day_name"]
        
        print_info(f"[{day_idx+1}/{len(weather_sequence)}] {date_str} ({day_name}) - {season} - {day_type}")
        
        # Prepare Pipeline Inputs
        # Enhanced Context: Inject Day Name into Day Type for LLM visibility
        enhanced_day_type = f"{day_name} ({day_type})"
        
        variables = {
            "country": country,
            "season": season,
            "day_type": enhanced_day_type,
            "family_data": json.dumps(family_data, separators=(',', ':')),
            "work_regime": json.dumps(work_regime, separators=(',', ':')),
            "weather_hourly": json.dumps(day_weather, separators=(',', ':'))
        }
        
        context = {
            "country": country,
            "season": season,
            "family_id": family_id,
            "date": date_str
        }
        
        # Success Handler
        seed = random.randint(1, 999999)
        filename = f"{date_str}_{day_name}_{season}_{day_type}.json"
        filepath = out_path / filename
        
        def success_handler(output_str, data, attempt):
            with open(filepath, "w") as f:
                f.write(output_str)
            print_info(f"Saved: {colored(filename, Colors.CYAN)}", 1)
            
        # Run Pipeline
        print_info(f"   Generating consumption...", 1)
        output = run_council_pipeline(
            stage_name="stage5_consumption",
            variables=variables,
            context=context,
            components=components,
            custom_success_handler=success_handler,
            seed=seed
        )
        
        if output:
            generated_files.append(filepath)
            
    # Summary
    print_section("Summary")
    print_success(f"Generated {len(generated_files)} daily profiles")
    print_info(f"Output directory: {out_path}")

def main():
    args = parse_arguments()
    
    print_header("SEQUENTIAL CONSUMPTION GENERATOR (STEP 2)")
    
    # 1. Initialize Configuration
    config = Config(project_root / "config")
    runtime_config = config.runtime
    
    # Determine country for context
    country = runtime_config.get("pipeline", {}).get("country", "Ireland")
    
    # 2. Load Family Data (Crucial for ID and Path)
    print_section("Loading Family Data")
    
    candidates = []
    if args.families_file:
        p = Path(args.families_file)
        if p.exists():
            candidates.append(p)
        else:
            print_error(f"Specified families file not found: {p}")
            return
            
    if not candidates:
        candidates = [
            Path("stage1_families.json"),
            project_root / "outputs" / "stage1_families.json",
            project_root / "stage1_families.json"
        ]
        candidates.extend(project_root.glob("outputs/**/stage1_families.json"))
        
    family_data = None
    
    # Validation: Must have either ID or Index
    if not args.family_id and args.family_index is None:
        print_error("You must provide either --family-id OR --family-index")
        return

    for p in candidates:
        if p.exists():
            try:
                with open(p) as f:
                    data = json.load(f)
                    if isinstance(data, list) and data:
                        if args.family_index is not None:
                            if 0 <= args.family_index < len(data):
                                family_data = data[args.family_index]
                                print_success(f"Loaded family at index {args.family_index} (ID: {family_data.get('household_id')}) from {p}")
                                break
                            else:
                                print_warning(f"Index {args.family_index} out of range for file {p} (size {len(data)})")
                        elif args.family_id:
                            family_data = next((f for f in data if f.get("household_id") == args.family_id), None)
                            if family_data:
                                print_success(f"Loaded family {args.family_id} from {p}")
                                break
            except Exception as e:
                print_warning(f"Error reading {p}: {e}")
                pass
                
    if not family_data:
        print_error("Could not find specified family in any candidate files.")
        return

    family_id = family_data.get("household_id", "Unknown")
    
    # 3. Setup Paths
    out_path = Path(args.output_dir) / family_id
    out_path.mkdir(parents=True, exist_ok=True)
    
    # Override config logic for consolidated logging
    if "paths" not in runtime_config:
        runtime_config["paths"] = {}
    runtime_config["paths"]["output_dir"] = str(out_path)
    
    setup_logging(out_path)
    
    # Initialize components with updated config (logs will go to out_path/logs)
    components = initialize_council(config)
    
    # 4. Execute Generation
    weather_sequence_file = out_path / "weather_sequence.json"
    
    generate_consumption_sequence(args, country, family_data, weather_sequence_file, out_path, components)

if __name__ == "__main__":
    main()
