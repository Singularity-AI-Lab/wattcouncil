"""
3-Stage Pipeline Execution Script

Generates household energy consumption profiles using a 3-stage pipeline:
- Stage 1: Family structures (generated if enabled and not provided)
- Stage 2: Weather profiles (generated if enabled and not provided)
- Stage 3: Consumption patterns (always generated)

Usage:
    # Generate all stages from scratch:
    python main.py --config config/runtime.yaml
    
    # Use existing files:
    python main.py --families stage1_families.json --weather stage2_weather_profiles.json
    
    # Resume from previous run:
    python main.py --config config/runtime.yaml --resume run_20260129_150000
"""
import sys
import json
import yaml
import re
import argparse
from pathlib import Path
from datetime import datetime
import random
import itertools
from utils.constants import CONSTANTS

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

from council.config import Config
from council.pipeline import initialize_council, run_council_pipeline
from utils import (
    Icons, Colors, colored,
    print_header, print_section, print_success, print_warning, print_error,
    print_info,
    setup_logging, save_combined_output,
    scan_stage1_checkpoints, scan_stage2_checkpoints, scan_stage3_checkpoints, load_checkpoint_data,
    snapshot_configs, display_log_analysis
)
from utils.api_logger import get_logger


def main():
    parser = argparse.ArgumentParser(description="Run 3-Stage LLM Council Energy Pipeline")
    parser.add_argument("--config", type=str, default="config/runtime.yaml",
                       help="Path to runtime configuration file")
    parser.add_argument("--families", type=str, default=None,
                       help="Path to stage1_families.json input file")
    parser.add_argument("--weather", type=str, default=None,
                       help="Path to stage2_weather_profiles.json input file")
    parser.add_argument("--weather-config", type=str, default=None,
                       help="Optional path to weather configuration file (for TMY generation)")
    parser.add_argument("--resume", type=str, default=None,
                       help="Resume from specific run directory (e.g., run_20260129_150000)")
    parser.add_argument("--output-dir", type=str, default=None,
                       help="Explicit output directory path (overrides timestamped creation)")
    args = parser.parse_args()
    
    # 1. Initialization
    print_header(f"{Icons.ROCKET} 3-STAGE PIPELINE: CONSUMPTION GENERATION")
    
    # Load Runtime Config
    try:
        with open(args.config, 'r') as f:
            runtime_config = yaml.safe_load(f) or {}
        print_info(f"Loaded runtime config: {colored(args.config, Colors.CYAN)}")
    except Exception as e:
        print_error(f"Failed to load runtime config: {e}")
        return
    
    # Load Weather Config (optional)
    weather_config = None
    if args.weather_config:
        try:
            with open(args.weather_config, 'r') as f:
                weather_config = yaml.safe_load(f) or {}
            print_info(f"Loaded weather config: {colored(args.weather_config, Colors.CYAN)}")
        except Exception as e:
            print_warning(f"Failed to load weather config: {e}")
            weather_config = None
    
    # Setup Output Directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = None
    resumed = False
    
    # Handle resume
    if args.resume:
        resume_path = Path(args.resume)
        if not resume_path.exists():
            resume_path = Path(f"outputs/{args.resume}")
        
        if resume_path.exists():
            output_dir = resume_path
            resumed = True
            print_success(f"Resuming from: {colored(str(output_dir), Colors.GREEN)}")
        else:
            print_warning(f"Resume directory not found: {resume_path}")
    
    if not output_dir and args.output_dir:
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        print_info(f"Using explicit output directory: {colored(str(output_dir), Colors.BLUE)}")

    if not output_dir:
        output_dir = Path(f"outputs/run_{timestamp}")
        output_dir.mkdir(parents=True, exist_ok=True)
    print_info(f"Output Directory: {colored(str(output_dir), Colors.BLUE)}")
    
    # Snapshot config
    configs_to_snapshot = [args.config]
    if Path("config/models.yaml").exists():
        configs_to_snapshot.append("config/models.yaml")
    if Path("config/constants.yaml").exists():
        configs_to_snapshot.append("config/constants.yaml")
    if Path("config/model_costs.yaml").exists():
        configs_to_snapshot.append("config/model_costs.yaml")
    if weather_config:
        configs_to_snapshot.append(args.weather_config)
    snapshot_configs(output_dir, config_files=configs_to_snapshot)
    
    log_dir = setup_logging(output_dir)
    
    # Get active provider from config
    provider = runtime_config.get("active_provider", "deepinfra")
    api_logger = get_logger(log_dir=str(log_dir), enabled=True, provider=provider)
    
    try:
        # Initialize Council Components
        try:
            council_config = Config(config_dir="config")
            components = initialize_council(council_config)
        except Exception as e:
            print_error(f"Failed to initialize council: {e}")
            return
        
        # Pipeline Context
        pipeline_context = {
            "output_dir": output_dir,
            "country": runtime_config.get("pipeline", {}).get("country", "Ireland"),
            "year": runtime_config.get("pipeline", {}).get("year", 2009),
            "weather_config": weather_config
        }
        
        stages_config = runtime_config.get("stages", {})
        
        # =========================================================================
        # STAGE 1: FAMILY GENERATION
        # =========================================================================
        families = []
        stage1_file = args.families or (output_dir / "stage1_families.json")
        
        if stages_config.get("stage1_families", {}).get("enabled", False):
            if not args.families and not Path(stage1_file).exists():
                print_header(f"{Icons.FAMILY} STAGE 1: FAMILY GENERATION")
                
                num_families = stages_config["stage1_families"].get("num_families", 1)
                num_variants = stages_config["stage1_families"].get("num_variants", 1)
                
                stage1_outputs = []
                stage1_raw_dir = output_dir / "raw" / "stage1_families"
                stage1_raw_dir.mkdir(parents=True, exist_ok=True)
                
                # Checkpoints
                existing_data = load_checkpoint_data(stage1_raw_dir, "*.json")
                stage1_outputs.extend(existing_data)
                
                completed_items = scan_stage1_checkpoints(stage1_raw_dir)
                completed_variants = set()
                
                # Determine fully completed variants (where count >= num_families)
                variant_counts = {}
                for household_id, var in completed_items:
                    variant_counts[var] = variant_counts.get(var, 0) + 1
                    
                for var, count in variant_counts.items():
                    if count >= num_families:
                        completed_variants.add(var)
                
                if completed_variants:
                     print_info(f"Found {len(existing_data)} families from previous run (variants: {sorted(list(completed_variants))})")

                # Prepare loop items (Targeted vs Standard)
                targeted_generation = stages_config["stage1_families"].get("targeted_generation", False)
                
                if targeted_generation:
                    print_info(f"{Icons.FAMILY} Running TARGETED generation based on demographic maps")

                    # Create combinations from CONSTANTS maps
                    combinations = list(itertools.product(
                        CONSTANTS.house_type_map.items(),
                        CONSTANTS.n_people_map.items(),
                        CONSTANTS.household_composition_map.items(),
                    ))
                    
                    # REPRESENTATIVE_COMBOS_ALT provided by config
                    # Format: (num_people, house_type, composition)
                    # NOTE: itertools.product produces (house_type, num_people, composition)
                    representative_combos_config = stages_config["stage1_families"].get("representative_combos", [])
                    REPRESENTATIVE_COMBOS_ALT = [tuple(c) for c in representative_combos_config]

                    # Filter combinations
                    target_limit = stages_config["stage1_families"].get("num_families", len(combinations))
                    target_variants = stages_config["stage1_families"].get("num_variants", 1)

                    loop_items = []
                    global_counter = 1
                    
                    # Get employment statuses to cycle through
                    emp_statuses = list(CONSTANTS.employment_status_map.items())
                    
                    for combo_idx, (h, p, c) in enumerate(combinations, 1):
                        # check against representative combos (p, h, c)
                        # h is (id, name), p is (id, name), c is (id, name)
                        
                        current_combo_ids = (p[0], h[0], c[0])
                        
                        # Only filter if the config list is provided and not empty
                        if REPRESENTATIVE_COMBOS_ALT:
                            if current_combo_ids in REPRESENTATIVE_COMBOS_ALT:
                                 print_info(f"MATCHED REPRESENTATIVE GROUP: H={h[1]}, P={p[1]}, C={c[1]}")
                                 # Use the configured num_variants
                            else:
                                 # Skip if not in the representative list
                                 continue
                        
                        # If list is empty, we fall through here (logic for normal targeted gen if needed, 
                        # but request implies strict filtering)
                        
                        for v_idx in range(1, target_variants + 1):
                            # Cycle through employment statuses based on variant
                            e = emp_statuses[(v_idx - 1) % len(emp_statuses)]
                            
                            loop_items.append({
                                "type": "targeted",
                                "idx": global_counter,
                                "variant_num": v_idx,
                                "desc": f"H{h[0]}-P{p[0]}-C{c[0]}-E{e[0]}", 
                                "vars": {
                                    "house_type": h[1],
                                    "num_people": p[1],
                                    "composition": c[1],
                                    "employment_status": e[1],
                                    "chief_income_status": "Any" 
                                }
                            })
                            global_counter += 1
                    
                    if REPRESENTATIVE_COMBOS_ALT:
                        print_info(f"Generated {len(loop_items)} tasks for {len(REPRESENTATIVE_COMBOS_ALT)} representative groups.")
                    else:
                        print_info(f"Generated {len(loop_items)} tasks (no representative filter applied).")
                    num_families_per_call = 1
                else:
                    # Standard random generation
                    loop_items = [{"type": "standard", "idx": i+1, "desc": f"Variant {i+1}", "vars": {}} for i in range(num_variants)]
                    num_families_per_call = num_families

                # Main Generation Loop
                for item in loop_items:
                    v = item["idx"] - 1 # 0-indexed for internal logic if needed, but we use item['idx'] (1-based) mostly
                    idx = item["idx"]
                    variant_num = item.get("variant_num", idx) # Use specific variant number if available
                    desc = item["desc"]
                    extra_vars = item["vars"]

                    # Check completion status
                    is_completed = False
                    if targeted_generation:
                         # For targeted, checking if specfic var for this combo exists
                         # Filename pattern: ..._CODE_varXX_...
                         combo_code = desc.replace("-", "_")
                         # We need to check if ANY file matches this pattern in raw_dir
                         # pattern: *_{combo_code}_var{variant_num:02d}_*.json
                         pattern = f"*_{combo_code}_var{variant_num:02d}_*.json"
                         if list(stage1_raw_dir.glob(pattern)):
                             is_completed = True
                    else:
                         # Standard mode uses the aggregated count (num_families per variant)
                         if idx in completed_variants:
                             is_completed = True

                    if is_completed:
                        # print_info(f"Skipping {desc} var{variant_num} - already completed", 1) # Optional verbose
                        continue

                    variables = {
                        "country": pipeline_context["country"],
                        "year": str(pipeline_context["year"]),
                        "num_families": num_families_per_call,
                        # Default constraint values (can be overridden by extra_vars)
                        "house_type": "Any",
                        "num_people": "Any",
                        "composition": "Any",
                        "chief_income_status": "Any"
                    }
                    variables.update(extra_vars) # Apply targeted constraints
                    
                    variation_seed = random.randint(1, 999999)
                    print_info(f"Generating {desc} (Variant {variant_num}, seed: {variation_seed})")
                    
                    # Track families generated in this call
                    generated_families = []
                    
                    # Success handler to save individual families
                    def stage1_success_handler(output_str, data, attempt):
                        try:
                            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                            country_code = pipeline_context["country"].replace(" ", "")[:3].upper()
                            type_counter = {}
                            
                            if isinstance(data, list):
                                for family in data:
                                    family_type = family.get("family_type", "Unknown")
                                    type_abbr = ''.join([word[0].upper() for word in family_type.split()[:3]])
                                    
                                    if type_abbr not in type_counter:
                                        type_counter[type_abbr] = 0
                                    type_counter[type_abbr] += 1
                                    
                                    household_id = f"{country_code}-{type_abbr}-{type_counter[type_abbr]:03d}"
                                    family["household_id"] = household_id
                                    
                                    # Construct filename
                                    if item["type"] == "targeted":
                                         combo_code = desc.replace("-", "_")
                                         filename = f"{household_id}_{combo_code}_var{variant_num:02d}_{ts}.json"
                                    else:
                                         filename = f"{household_id}_var{idx:02d}_{ts}.json"

                                    filepath = stage1_raw_dir / filename
                                    with open(filepath, "w") as f:
                                        json.dump(family, f, indent=2)
                                    print_info(f"Saved: {colored(filename, Colors.CYAN)}", 1)
                                    
                                    # Add to generated list (preserves household_id)
                                    generated_families.append(family)
                        except Exception as e:
                            print_error(f"Error in success handler: {e}")
                    
                    output = run_council_pipeline(
                        stage_name="stage1_family",
                        variables=variables,
                        context={"country": pipeline_context["country"]},
                        components=components,
                        custom_success_handler=stage1_success_handler,
                        seed=variation_seed
                    )
                    
                    # Use the families from success handler (they have household_ids assigned)
                    if generated_families:
                        stage1_outputs.extend(generated_families)
                
                if stage1_outputs:
                    with open(stage1_file, 'w') as f:
                        json.dump(stage1_outputs, f, indent=2)
                    print_success(f"Generated {len(stage1_outputs)} families → {colored(stage1_file.name, Colors.GREEN)}")
                    families = stage1_outputs
                else:
                    print_error("Stage 1 failed. Stopping.")
                    return
            else:
                # Load existing file
                if Path(stage1_file).exists():
                    print_info(f"Loading families from: {colored(Path(stage1_file).name, Colors.CYAN)}")
                    with open(stage1_file, 'r') as f:
                        families = json.load(f)
                    if isinstance(families, dict):
                        families = [families]
                    print_success(f"Loaded {len(families)} families")
                else:
                    print_error(f"Stage 1 families file not found: {stage1_file}")
                    return
        else:
            # Stage 1 disabled, must provide file
            if not Path(stage1_file).exists():
                print_error(f"Stage 1 disabled but families file not found: {stage1_file}")
                return
            with open(stage1_file, 'r') as f:
                families = json.load(f)
            if isinstance(families, dict):
                families = [families]
            print_success(f"Loaded {len(families)} families")

        # =========================================================================
        # STAGE 2: WEATHER GENERATION
        # =========================================================================
        weather_profiles = []
        stage2_file = args.weather or (output_dir / "stage2_weather_profiles.json")
        
        # Check if we should run TMY generation (overrides enabled flag if config is present)
        run_tmy = weather_config is not None
        
        # Check if we should run LLM generation
        run_llm_weather = stages_config.get("stage2_weather", {}).get("enabled", False)
        
        # We run stage 2 if either TMY is configured OR LLM is enabled
        # AND we don't have an input file already
        should_run_stage2 = (run_tmy or run_llm_weather) and not args.weather
        
        if should_run_stage2:
            if not Path(stage2_file).exists():
                print_header(f"{Icons.WEATHER} STAGE 2: WEATHER GENERATION")
                
                stage2_outputs = []
                stage2_raw_dir = output_dir / "raw" / "stage2_weather"
                stage2_raw_dir.mkdir(parents=True, exist_ok=True)
                
                # BRANCH 1: TMY GENERATION
                if run_tmy:
                    print_info(f"Using TMY Weather Generation (Config: {args.weather_config})")
                    from utils.tmy_weather import TMYWeatherGenerator
                    
                    try:
                        tmy_gen = TMYWeatherGenerator(
                            year=pipeline_context["year"],
                            config=weather_config.get("tmy", {})
                        )
                        
                        # Use num_variants from config or default to 1
                        num_variants = stages_config.get("stage2_weather", {}).get("num_variants", 1)
                        # Get seasons from config (default to all if not specified)
                        seasons = stages_config.get("stage2_weather", {}).get("seasons", None)
                        
                        # Generate for the pipeline country
                        # Note: TMY generator works per country. Pipeline context has 'country'.
                        country = pipeline_context["country"]
                        print_info(f"Fetching TMY data for {country}...")
                        
                        # Generate Stage 3 compatible ranges directly
                        # TMY generator handles seasonality internally
                        tmy_outputs = tmy_gen.generate_stage3_ranges(country, num_variants=num_variants, seasons=seasons)
                        stage2_outputs.extend(tmy_outputs)
                        
                        # Also save raw CSVs (handled by TMY class if needed, or we can call generate_weather_for_country)
                        # The TMY class has generate_weather_for_country which saves CSVs
                        if weather_config.get("output", {}).get("save_csv", True):
                            saved_csvs = tmy_gen.generate_weather_for_country(
                                country, 
                                stage2_raw_dir
                            )
                            print_info(f"Saved TMY CSV profiles to {stage2_raw_dir}")

                    except Exception as e:
                        print_error(f"TMY Generation Failed: {e}")
                        # If TMY fails and LLM is not enabled, we should probably stop
                        if not run_llm_weather:
                            return
                        print_warning("Falling back to LLM generation default logic...")
                        run_tmy = False # Fallback

                # BRANCH 2: LLM GENERATION (Only if TMY not run or failed/skipped)
                if not run_tmy and run_llm_weather:
                    num_variants = stages_config["stage2_weather"].get("num_variants", 1)
                    seasons = stages_config["stage2_weather"].get("seasons", ["summer", "winter", "spring", "autumn"])
                    
                    # Checkpoints
                    existing_data = load_checkpoint_data(stage2_raw_dir, "*.json")
                    stage2_outputs.extend(existing_data)
                    
                    completed_items = scan_stage2_checkpoints(stage2_raw_dir)
                    if completed_items:
                         print_info(f"Found {len(existing_data)} weather profiles from previous run")

                    # Generate weather for each season and variant
                    for v_idx in range(num_variants):
                        for season in seasons:
                            if (v_idx + 1, season) in completed_items:
                                 print_info(f"Skipping {season} variant {v_idx+1}/{num_variants} - already completed", 1)
                                 continue

                            variation_seed = random.randint(1, 999999)
                            print_info(f"Generating {season} weather (variant {v_idx+1}/{num_variants}, seed: {variation_seed})")
                            
                            variables = {
                                "season": season,
                                "country": pipeline_context["country"],
                                "year": str(pipeline_context["year"])
                            }
                            
                            # Success handler
                            def stage2_success_handler(output_str, data, attempt):
                                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                                filename = f"variation{v_idx+1:02d}_{season}_{ts}.json"
                                filepath = stage2_raw_dir / filename
                                with open(filepath, "w") as f:
                                    f.write(output_str)
                                print_info(f"Saved: {colored(filename, Colors.CYAN)}", 1)
                            
                            output = run_council_pipeline(
                                stage_name="stage2_weather",
                                variables=variables,
                                context={"country": pipeline_context["country"], "season": season},
                                components=components,
                                custom_success_handler=stage2_success_handler,
                                seed=variation_seed
                            )
                            
                            if output:
                                data = json.loads(output)
                                stage2_outputs.append(data)
                
                if stage2_outputs:
                    with open(stage2_file, 'w') as f:
                        json.dump(stage2_outputs, f, indent=2)
                    print_success(f"Generated {len(stage2_outputs)} weather profiles → {colored(stage2_file.name, Colors.GREEN)}")
                    weather_profiles = stage2_outputs
                else:
                    print_error("Stage 2 generation produced no outputs. Stopping.")
                    return # Should return here if failed
            else:
                 # Should just fall through to load
                 pass

        # Load existing file (if we didn't just generate it, or if skipped above)
        # Check if file exists now (either from generation or previous runs)
        if Path(stage2_file).exists():
             if not weather_profiles: # Only load if not already loaded/generated
                print_info(f"Loading weather profiles from: {colored(Path(stage2_file).name, Colors.CYAN)}")
                with open(stage2_file, 'r') as f:
                    weather_profiles = json.load(f)
                    if not isinstance(weather_profiles, list):
                        weather_profiles = [weather_profiles]
                print_success(f"Loaded {len(weather_profiles)} weather profiles")
        else:
            # Only error if we really needed it and couldn't generate/load
            print_error(f"Stage 2 weather file not found: {stage2_file}")
            return
        
        # =========================================================================
        # STAGE 3: CONSUMPTION PATTERNS
        # =========================================================================
        if stages_config.get("stage3_consumption", {}).get("enabled", True):
            print_header(f"{Icons.ENERGY} STAGE 3: CONSUMPTION PATTERNS")
            
            num_variants = stages_config.get("stage3_consumption", {}).get("num_variants", 1)
            stage3_outputs = []
            stage3_raw_dir = output_dir / "raw" / "stage3_consumption"
            stage3_raw_dir.mkdir(parents=True, exist_ok=True)
            
            # Checkpoints
            completed_items = scan_stage3_checkpoints(stage3_raw_dir)
            if completed_items:
                print_info(f"Found {len(completed_items)} completed items from previous run")
                existing_data = load_checkpoint_data(stage3_raw_dir, "*.json")
                stage3_outputs.extend(existing_data)
            
            # Group weather by season
            weather_by_season = {}
            for w in weather_profiles:
                s = w.get("season", "Unknown")
                if s not in weather_by_season:
                    weather_by_season[s] = []
                weather_by_season[s].append(w)
            
            # Calculate total tasks
            total_tasks = "dynamic" # Depends on day types
            print_info(f"Processing {len(families)} families × {len(weather_profiles)} weather profiles × {num_variants} variants")
            if completed_items:
                print_success(f"Resuming: {len(completed_items)}/{total_tasks} already completed")
            
            # Iterate Families
            # Iterate Families
            for fam_idx, family in enumerate(families, start=1):
                family_id = family.get("household_id", "Unknown")
                family_file_id = f"f{fam_idx:02d}"  # Use short index f01, f02 etc.
                print_section(f"Family {fam_idx}/{len(families)}: {family_id}")
                
                # Extract embedded work regime (from Stage 1)
                work_regime = {
                    "household_work_regime": family.get("household_work_regime", {}),
                    "weekday_daytime_occupancy_level": family.get("weekday_daytime_occupancy_level", "Unknown"),
                    "members": family.get("members", [])
                }
                
                if not work_regime.get("household_work_regime"):
                    print_warning(f"No work regime found in family {family_id}. Using empty.")
                
                # Iterate Weather Profiles
                for season, weathers in weather_by_season.items():
                    for w_idx, weather in enumerate(weathers, start=1):
                        # Determine day types to process
                        weather_day_type = weather.get("day_type") or weather.get("date_type")
                        
                        # Infer day type from day name if available (common in sequential runs)
                        if not weather_day_type and "day_name" in weather:
                            day_name = weather["day_name"]
                            if day_name in ["Saturday", "Sunday"]:
                                weather_day_type = "weekend"
                            else:
                                weather_day_type = "weekday"
                                
                        if weather_day_type:
                            target_day_types = [weather_day_type]
                        else:
                            # Default to standard weekday/weekend split if not specified in weather
                            # Check if config overrides this
                            config_day_types = stages_config.get("stage3_consumption", {}).get("day_types")
                            target_day_types = config_day_types if config_day_types else ["weekday", "weekend"]
                            
                        for day_type in target_day_types:
                            for v in range(num_variants):
                                # Check checkpoint (using family_file_id for filename matching)
                                # Key format matches scan_stage5_checkpoints: (family_id, work_var, season, s3_var, day_type, variant)
                                checkpoint_key = (family_file_id, 1, season, w_idx, day_type, v+1)
                                if checkpoint_key in completed_items:
                                    print_info(f"   ⏭️  Skipping {season} s3var{w_idx:02d} {day_type} var{v+1:02d} - already completed", 1)
                                    continue
                                
                                variables = {
                                    "country": pipeline_context["country"],
                                    "season": season,
                                    "year": str(pipeline_context["year"]),
                                    "family_id": f"{family_id} ({day_type})",
                                    "day_type": day_type,
                                    "weather": json.dumps(weather, separators=(',', ':')),
                                    "family_profile": json.dumps(family, separators=(',', ':')),
                                    "household_id": family_id, # passed for prompt substitution
                                    # Prompt templates might expect these:
                                    "household_composition": json.dumps(family.get("members", []), separators=(',', ':')),
                                    "work_regime": json.dumps(work_regime, separators=(',', ':'))
                                }
                                
                                context = {
                                    "country": pipeline_context["country"],
                                    "season": season,
                                    "family_id": f"{family_id} ({day_type})"
                                }
                                
                                # Success Handler
                                def stage3_success_handler(output_str, data, attempt):
                                    # Inject source metadata if available
                                    try:
                                        if isinstance(data, dict):
                                            if args.families:
                                                 data["stage1_source_file"] = Path(args.families).name
                                            if args.weather:
                                                 data["stage2_source_file"] = Path(args.weather).name
                                            # Update output string with new metadata
                                            output_str = json.dumps(data, indent=2)
                                    except Exception as e:
                                        print_warning(f"Failed to inject source metadata: {e}")

                                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                                    # Filename format compatible with scanner:
                                    # FamilyID_workXX_s3varXX_Season_DayType_varXX_timestamp.json
                                    filename = f"{family_file_id}_work01_s3var{w_idx:02d}_{season}_{day_type}_var{v+1:02d}_{ts}.json"
                                    filepath = stage3_raw_dir / filename
                                    with open(filepath, "w") as f:
                                        f.write(output_str)
                                    print_info(f"Saved: {colored(filename, Colors.CYAN)}", 1)
                                
                                output = run_council_pipeline(
                                    stage_name="stage3_consumption",  # Using existing consumption prompt
                                    variables=variables,
                                    context=context,
                                    components=components,
                                    custom_success_handler=stage3_success_handler,
                                    seed=random.randint(1, 999999)
                                )
                                
                                if output:
                                    data = json.loads(output)
                                    stage3_outputs.append(data)
                                    print_success(f"{family_id} - {season} - {day_type} var{v+1:02d} generated")
            
            # Save combined output
            if stage3_outputs:
                filepath = output_dir / "stage3_consumption.json"
                save_combined_output(filepath, stage3_outputs)
                print_success(f"Saved combined output: {colored(filepath.name, Colors.GREEN)}")
            
            print_header(f"{Icons.SUCCESS} PIPELINE COMPLETED")
            print_info(f"Outputs saved to: {colored(str(output_dir), Colors.BLUE)}")
            display_log_analysis(output_dir, log_dir)
    
    finally:
        # Always close API logger
        api_logger.close()


if __name__ == "__main__":
    main()
