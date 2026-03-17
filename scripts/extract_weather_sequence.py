#!/usr/bin/env python3
"""
Weather Sequence Extractor

Extracts a chronological sequence of weather data (TMY) for a specific family/location.
This is Step 1 of the sequential consumption generation process.

Usage:
    python scripts/extract_weather_sequence.py --start-date 06-01 --days 7 --family-index 0
"""

import sys
import argparse
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from council.config import Config
from utils.tmy_weather import TMYWeatherGenerator
from utils.constants import CONSTANTS
from utils import (
    Icons, Colors, colored,
    print_header, print_section, print_success, print_warning, print_error,
    print_info, setup_logging
)

# Setup logging
logger = logging.getLogger(__name__)

def parse_arguments():
    parser = argparse.ArgumentParser(description="Extract weather sequence")
    
    parser.add_argument("--start-date", type=str, required=True, help="Start date in MM-DD format (e.g. 06-01)")
    parser.add_argument("--days", type=int, default=7, help="Number of days to extract (default: 7)")
    parser.add_argument("--year", type=int, default=None, help="Year to use (override config)")
    parser.add_argument("--country", type=str, default=None, help="Country to use (override config)")
    parser.add_argument("--output-dir", type=str, default="outputs/sequential_weather", help="Output directory")
    
    return parser.parse_args()

def extract_weather_sequence(args, country, year, tmy, out_path):
    print_section("Step 1: Extracting Weather Sequence")
    
    try:
        start_md = args.start_date
        start_month, start_day = map(int, start_md.split('-'))
        days = args.days
    except ValueError:
        print_error("Start date must be MM-DD")
        return None
        
    # --- Logic ---
    # 1. Fetch TMY data (cached)
    print_info(f"Fetching TMY data for {country}...")
    
    coordinates = tmy.coordinates.get(country)
    if not coordinates:
        print_error(f"No coordinates found for {country}")
        return None
        
    lat, lon = coordinates
    tmy_data = tmy.fetch_tmy_data(country, lat, lon)
    
    if tmy_data is None:
        print_error("Failed to fetch TMY data")
        return None
        
    # Ensure DatetimeIndex
    if not isinstance(tmy_data.index, pd.DatetimeIndex):
        tmy_data = tmy_data.reset_index()
        # Look for time column
        time_col = next((c for c in tmy_data.columns if 'time' in c.lower()), None)
        if time_col:
            tmy_data[time_col] = pd.to_datetime(tmy_data[time_col])
            tmy_data.set_index(time_col, inplace=True)
        
    # 2. Extract and Save Sequence
    generated_files = []
    
    # Create date objects for iteration (using a dummy non-leap year)
    current_date = datetime(year if year else 2023, start_month, start_day)
    
    print_info(f"Date Range: {current_date.strftime('%Y-%m-%d')} to {(current_date + timedelta(days=days)).strftime('%Y-%m-%d')}")
    
    for i in range(days):
        date_obj = current_date + timedelta(days=i)
        
        # Format for lookup MM-DD
        lookup_key = date_obj.strftime("%m-%d")
        day_name = date_obj.strftime("%A")
        date_str = date_obj.strftime("%Y-%m-%d")
        
        # Filter dataframe for this day
        day_data = tmy_data[
            (tmy_data.index.month == date_obj.month) & 
            (tmy_data.index.day == date_obj.day)
        ]
        
        if day_data.empty:
            print_warning(f"No data found for {lookup_key}")
            continue
            
        # Determine metadata
        season = tmy.get_season(date_obj.day, date_obj.month)
        
        # Determine day type (specific to country)
        weekends = CONSTANTS.weekends_by_country.get(country, ["Saturday", "Sunday"])
        day_type = "weekend" if day_name in weekends else "weekday"
        
        # Extract hourly lists
        # We assume 0-23 hours are present in sorted order of day_data
        # Handle missing hours if necessary (though TMY is usually complete)
        # Ensure we clamp to 24 hours
        
        temps = []
        humids = []
        diffuses = []
        directs = []
        
        for hour in range(24):
            if hour < len(day_data):
                row = day_data.iloc[hour]
                temps.append(round(row['temp_air'], 2))
                humids.append(round(row['relative_humidity'], 2))
                diffuses.append(round(row['dhi'], 2))
                directs.append(round(row['dni'], 2))
            else:
                # Fallback for missing data (should not happen with standard TMY)
                temps.append(temps[-1] if temps else 0)
                humids.append(humids[-1] if humids else 0)
                diffuses.append(0)
                directs.append(0)
                
        # Construct weather object with requested structure
        weather_obj = {
            "country": country,
            "year": year,
            "season": season, # Title Case from TMY
            "date": date_str,
            "day_type": day_type,
            "day_name": day_name,
            "weather_data": {
                "temperature": {
                    "min": min(temps),
                    "max": max(temps),
                    "hourly_profile": temps
                },
                "humidity": {
                    "min": min(humids),
                    "max": max(humids),
                    "hourly_profile": humids
                },
                "solar_radiation_diffuse": {
                    "min": min(diffuses),
                    "max": max(diffuses),
                    "hourly_profile": diffuses
                },
                "solar_radiation_direct": {
                    "min": min(directs),
                    "max": max(directs),
                    "hourly_profile": directs
                }
            }
        }
        
        # Save individual file for this day
        # Filename: weather_{YYYY-MM-DD}_{DayName}.json
        filename = f"weather_{date_str}_{day_name}.json"
        filepath = out_path / filename
        
        # Wrap in list as main.py expects a list of profiles
        output_data = [weather_obj]
        
        with open(filepath, "w") as f:
            json.dump(output_data, f, indent=2)
            
        generated_files.append(filepath)
        print_info(f"Saved: {colored(filename, Colors.CYAN)} ({season}, {day_type})")

    if generated_files:
        print_success(f"Extracted {len(generated_files)} daily weather files to {out_path}")
        # Return list of files for potential external use
        return generated_files
    else:
        print_warning("No weather files were generated.")
        return []

def main():
    args = parse_arguments()
    
    print_header("WEATHER SEQUENCE EXTRACTOR")
    
    # 1. Initialize Configuration
    config = Config(project_root / "config")
    runtime_config = config.runtime
    
    # Load weather config manually
    weather_config_path = project_root / "config" / "weather.yaml"
    weather_config = {}
    if weather_config_path.exists():
        import yaml
        with open(weather_config_path, "r") as f:
            weather_config = yaml.safe_load(f)
    
    # Determine base parameters
    country = args.country if args.country else runtime_config.get("pipeline", {}).get("country", "Ireland")
    year = args.year if args.year else runtime_config.get("pipeline", {}).get("year", 2024)
    
    print_info(f"Target: {country} | Year: {year}")
    print_info(f"Sequence: {args.start_date} for {args.days} days")
    
    # 2. Setup Output Environment
    out_path = Path(args.output_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    
    # Override config logic for logging (though less critical here than in pipeline)
    if "paths" not in runtime_config:
        runtime_config["paths"] = {}
    runtime_config["paths"]["output_dir"] = str(out_path)
    
    setup_logging(out_path)
    
    # 3. Get Weather Data
    print_section("Preparing Weather Data")
    tmy_config = weather_config.get("tmy", {})
    tmy = TMYWeatherGenerator(year=year, config=tmy_config)
    
    # 4. Extract
    extract_weather_sequence(args, country, year, tmy, out_path)

if __name__ == "__main__":
    main()
