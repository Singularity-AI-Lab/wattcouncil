import os
import json
import yaml
import shutil
import argparse
from pathlib import Path


def merge_json_files(source_files, target_file):
    """
    Merges multiple JSON files containing lists into a single JSON file.
    """
    combined_data = []
    for fpath in source_files:
        if not os.path.exists(fpath):
            print(f"Warning: {fpath} does not exist, skipping json merge.")
            continue
            
        try:
            with open(fpath, 'r') as f:
                data = json.load(f)
                if isinstance(data, list):
                    combined_data.extend(data)
                elif isinstance(data, dict):
                    combined_data.append(data)
                else:
                    print(f"Warning: Unexpected data type in {fpath}")
        except json.JSONDecodeError:
             print(f"Error decoding {fpath}")

    # Write combined data
    with open(target_file, 'w') as f:
        json.dump(combined_data, f, indent=2)
    print(f"Merged {len(source_files)} JSON files into {target_file} ({len(combined_data)} records).")


def merge_text_lines_files(source_files, target_file):
    """
    Merges multiple text files (like knowledge_base.txt), deduplicating lines.
    """
    unique_lines = set()
    # If target already exists (e.g. from base copy), read it first
    if os.path.exists(target_file):
        with open(target_file, 'r') as f:
            unique_lines.update([line.strip() for line in f if line.strip()])

    for fpath in source_files:
        if not os.path.exists(fpath):
            continue
        try:
            with open(fpath, 'r') as f:
                # Add non-empty lines
                unique_lines.update([line.strip() for line in f if line.strip()])
        except Exception as e:
            print(f"Error reading {fpath}: {e}")

    # Write sorted unique lines
    try:
        with open(target_file, 'w') as f:
            for line in sorted(unique_lines):
                f.write(line + "\n")
        print(f"Merged knowledge base from {len(source_files)} files into {target_file}")
    except Exception as e:
         print(f"Error writing to {target_file}: {e}")

def copy_directory_contents(src, dst, rename_conflicting_yamls=False):
    """
    Recursively copies contents of src to dst.
    """
    if not os.path.exists(dst):
        os.makedirs(dst)
        
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        
        if os.path.isdir(s):
            copy_directory_contents(s, d, rename_conflicting_yamls=rename_conflicting_yamls)
        else:
            # Handling special files
            if item == "stage4_hourly_weather.json":
                continue 
            
            # Skip knowledge_base.txt for now, we merge it at the end
            if item == "knowledge_base.txt":
                continue

            # Rename strategy for Configs AND Logs
            if item.endswith(".yaml") or item.endswith(".yml") or item.endswith(".log"):
                # If destination exists, check strategy
                if os.path.exists(d):
                    if rename_conflicting_yamls:
                        # Rename strategy: filename_copy1.ext
                        base, ext = os.path.splitext(item)
                        counter = 1
                        while True:
                            new_name = f"{base}_copy{counter}{ext}"
                            new_d = os.path.join(dst, new_name)
                            if not os.path.exists(new_d):
                                break
                            counter += 1
                        
                        print(f"File conflict: Renaming {item} -> {new_name}")
                        shutil.copy2(s, new_d)
                    else:
                        # Overwrite
                        print(f"Overwriting file: {item}")
                        shutil.copy2(s, d)
                else:
                    shutil.copy2(s, d)
                continue

            # Simple copy for other files, overwriting if exists
            shutil.copy2(s, d)

def main():
    parser = argparse.ArgumentParser(description="Merge experiment folders for Stage 5.")
    parser.add_argument("--family-dir", required=True, help="Directory containing Stage 1/2 outputs (Families)")
    parser.add_argument("--weather-dirs", nargs='+', required=True, help="List of directories containing Stage 4 outputs (Weather)")
    parser.add_argument("--output-dir", required=True, help="Target directory for the combined experiment")
    
    args = parser.parse_args()
    
    output_path = Path(args.output_dir)
    if output_path.exists():
        print(f"Warning: Output directory {output_path} already exists.")
    else:
        os.makedirs(output_path)

    # Track knowledge base files
    kb_paths = []

    # 1. Copy Family Directory Contents (Base) - No renaming, plain copy
    print(f"Copying base family data from {args.family_dir}...")
    copy_directory_contents(args.family_dir, args.output_dir, rename_conflicting_yamls=False)
    
    # Check for knowledge base in family dir
    fam_kb = os.path.join(args.family_dir, "knowledge_base.txt")
    if os.path.exists(fam_kb):
        kb_paths.append(fam_kb)

    # 2. Copy Weather Directory Contents (excluding the main JSON for now)
    weather_json_paths = []
    
    # Check if family dir has a weather file? usually no, but if so, include it
    fam_weather = os.path.join(args.family_dir, "stage4_hourly_weather.json")
    if os.path.exists(fam_weather):
        weather_json_paths.append(fam_weather)

    print("Copying weather resources...")
    for w_dir in args.weather_dirs:
        # Enable renaming for conflicts from weather dirs
        copy_directory_contents(w_dir, args.output_dir, rename_conflicting_yamls=True)
        
        w_json = os.path.join(w_dir, "stage4_hourly_weather.json")
        if os.path.exists(w_json):
            weather_json_paths.append(w_json)
            
        w_kb = os.path.join(w_dir, "knowledge_base.txt")
        if os.path.exists(w_kb):
            kb_paths.append(w_kb)

    # 3. Merge Weather JSONs
    if weather_json_paths:
        target_json = os.path.join(args.output_dir, "stage4_hourly_weather.json")
        print("Merging weather JSON files...")
        merge_json_files(weather_json_paths, target_json)
    else:
        print("No stage4_hourly_weather.json files found to merge.")

    # 4. Merge Knowledge Base
    if kb_paths:
        target_kb = os.path.join(args.output_dir, "knowledge_base.txt")
        print("Merging knowledge base files...")
        merge_text_lines_files(kb_paths, target_kb)

    print(f"\nSuccess! Experiment folder ready at: {args.output_dir}")

if __name__ == "__main__":
    main()
