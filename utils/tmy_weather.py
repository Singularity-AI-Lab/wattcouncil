"""
TMY Weather Generator

Alternative to LLM-based weather generation using pvlib's Typical Meteorological Year data.
This can replace Stages 3 & 4 when USE_TMY is enabled in configuration.
"""
import logging
from pathlib import Path
from typing import Dict, Tuple
import pandas as pd

logger = logging.getLogger(__name__)


class TMYWeatherGenerator:
    """
    Generate weather profiles using pvlib's Typical Meteorological Year data.
    
    This class provides an alternative to LLM-based weather generation by using
    actual meteorological data from PVGIS (Photovoltaic Geographical Information System).
    """
    
    def __init__(self, year: int = 2024, timeout: int = 30, config: Dict = None):
        """
        Initialize TMY weather generator.
        
        Args:
            year: Target year for coercing the TMY data
            timeout: Timeout in seconds for PVGIS API calls
            config: Full TMY configuration dict (corresponding to 'tmy' key in weather.yaml)
        """
        try:
            import pvlib
            self.pvlib = pvlib
        except ImportError:
            raise ImportError(
                "pvlib is required for TMY weather generation. "
                "Install with: pip install pvlib"
            )
        
        # Configuration setup
        self.config = config or {}
        api_config = self.config.get('api', {})
        cache_config = self.config.get('cache', {})
        
        self.year = self.config.get('target_year', year)
        self.timeout = timeout
        
        # API settings
        self.start_year = api_config.get('start_year', 2013)
        self.end_year = api_config.get('end_year', 2023)
        self.use_horizon = api_config.get('use_horizon', True)
        
        # Coordinates setup (merge defaults with config)
        self.coordinates = {}
        if 'coordinates' in self.config:
            # Update with configured coordinates (handles lists from YAML by converting to tuples)
            for country, coords in self.config['coordinates'].items():
                if isinstance(coords, list):
                    self.coordinates[country] = tuple(coords)
                else:
                    self.coordinates[country] = coords
        
        # Southern hemisphere countries (for season determination)
        self.southern_hemisphere = self.config.get('southern_hemisphere', [])
        
        # Cache settings
        self.cache_enabled = cache_config.get('enabled', False)
        self.cache_dir = Path(cache_config.get('directory', 'outputs/cache/tmy'))
        
        logger.info(f"Initialized TMYWeatherGenerator for year {year}")
        logger.info(f"Using TMY data range: {self.start_year}-{self.end_year}")
        if self.cache_enabled:
            logger.info(f"Cache enabled: {self.cache_dir}")
    
    def get_season(self, day: int, month: int, hemisphere: str = 'northern') -> str:
        """
        Determine season based on day, month, and hemisphere.
        Matches logic from create_base_dfs.py
        """
        if (month == 12 and day >= 21) or (month in [1, 2]) or (month == 3 and day < 20):
            return 'Winter' if hemisphere == 'northern' else 'Summer'
        elif (month == 3 and day >= 20) or (month in [4, 5]) or (month == 6 and day < 21):
            return 'Spring' if hemisphere == 'northern' else 'Autumn'
        elif (month == 6 and day >= 21) or (month in [7, 8]) or (month == 9 and day < 23):
            return 'Summer' if hemisphere == 'northern' else 'Winter'
        elif (month == 9 and day >= 23) or (month in [10, 11]) or (month == 12 and day < 21):
            return 'Autumn' if hemisphere == 'northern' else 'Spring'
        else:
            raise ValueError("Invalid date or hemisphere. Ensure 'day' and 'month' are valid.")
    
    def _get_cache_path(self, country: str) -> Path:
        """
        Get the cache file path for a given country.
        Creates directory structure: {cache_dir}/{country}/{year}/tmy_data.csv
        
        Args:
            country: Country name
            
        Returns:
            Path to cache file
        """
        cache_path = self.cache_dir / country / str(self.year) / "tmy_data.csv"
        return cache_path
    
    def _load_from_cache(self, country: str) -> pd.DataFrame:
        """
        Load TMY data from cache if it exists.
        
        Args:
            country: Country name
            
        Returns:
            Cached DataFrame or None if cache doesn't exist
        """
        cache_path = self._get_cache_path(country)
        
        if cache_path.exists():
            try:
                logger.info(f"Loading TMY data from cache: {cache_path}")
                data = pd.read_csv(cache_path, index_col=0, parse_dates=True)
                logger.info(f"Successfully loaded {len(data)} records from cache for {country}")
                return data
            except Exception as e:
                logger.warning(f"Failed to load cache for {country}: {e}")
                return None
        
        return None
    
    def _save_to_cache(self, country: str, data: pd.DataFrame) -> None:
        """
        Save TMY data to cache.
        
        Args:
            country: Country name
            data: TMY data to cache
        """
        cache_path = self._get_cache_path(country)
        
        try:
            # Create directory structure
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save to CSV
            data.to_csv(cache_path)
            logger.info(f"Saved TMY data to cache: {cache_path}")
        except Exception as e:
            logger.warning(f"Failed to save cache for {country}: {e}")
    
    def fetch_tmy_data(self, country: str, lat: float, lon: float) -> pd.DataFrame:
        """
        Fetch TMY data from PVGIS for given coordinates.
        Uses cache if enabled to avoid redundant API calls.
        
        Args:
            country: Country name
            lat: Latitude
            lon: Longitude
            
        Returns:
            DataFrame with hourly TMY data
        """
        # Check cache first if enabled
        if self.cache_enabled:
            cached_data = self._load_from_cache(country)
            if cached_data is not None:
                return cached_data
        
        # Fetch from PVGIS API
        logger.info(f"Fetching TMY data from PVGIS for {country} (lat={lat}, lon={lon})")
        
        try:
            # pvlib returns (data, months_selected, inputs, metadata) in older versions
            # but only (data, months_selected) in newer versions
            # We only need the data, so unpack flexibly
            result = self.pvlib.iotools.get_pvgis_tmy(
                lat, lon,
                outputformat='json',
                usehorizon=self.use_horizon,
                userhorizon=None,
                startyear=self.start_year,
                endyear=self.end_year,
                map_variables=True,
                timeout=self.timeout,
                coerce_year=self.year
            )
            
            # Extract just the data (first element)
            if isinstance(result, tuple):
                data = result[0]
            else:
                data = result
            
            data['country'] = country
            logger.info(f"Successfully fetched {len(data)} records for {country}")
            
            # Save to cache if enabled
            if self.cache_enabled:
                self._save_to_cache(country, data)
            
            return data
            
        except Exception as e:
            logger.error(f"Failed to fetch TMY data for {country}: {e}")
            raise
    
    def process_tmy_to_seasonal_profiles(
        self,
        data: pd.DataFrame,
        country: str
    ) -> pd.DataFrame:
        """
        Process raw TMY data into seasonal hourly profiles.
        
        Args:
            data: Raw TMY DataFrame
            country: Country name
            
        Returns:
            DataFrame with seasonal averages per hour
        """
        # Reset index and extract time components
        data.reset_index(inplace=True)
        data['hour'] = data['time(UTC)'].dt.hour
        data['day'] = data['time(UTC)'].dt.day
        data['month'] = data['time(UTC)'].dt.month
        
        # Determine hemisphere
        hemisphere = 'southern' if country in self.southern_hemisphere else 'northern'
        
        # Apply season determination
        data['season'] = data.apply(
            lambda row: self.get_season(row['day'], row['month'], hemisphere),
            axis=1
        )
        
        # Convert season to lowercase to match pipeline convention
        data['season'] = data['season'].str.lower()
        
        # Group by season and hour, calculate means
        seasonal_data = data.groupby(['season', 'hour', 'country']).mean(numeric_only=True).reset_index()
        seasonal_data.drop(columns=['day', 'month'], inplace=True)
        
        # Rename columns to match pipeline format
        seasonal_data.rename(columns={
            'country': 'Country',
            'season': 'Season',
            'hour': 'Hour',
            'temp_air': 'Temperature_Value',
            'relative_humidity': 'Humidity_Value',
            'dhi': 'SolRad-Diffuse_Value',
            'dni': 'SolRad-Direct_Value',
            'wind_speed': 'Wind-Speed_Value'
        }, inplace=True)
        
        # Add description columns (empty for TMY data)
        seasonal_data['Temperature_Description'] = ''
        seasonal_data['Humidity_Description'] = ''
        seasonal_data['SolRad-Diffuse_Description'] = ''
        seasonal_data['SolRad-Direct_Description'] = ''
        seasonal_data['Wind-Speed_Description'] = ''
        
        # Reorder columns for consistency
        seasonal_data = seasonal_data[[
            'Country', 'Season', 'Hour',
            'Temperature_Description', 'Temperature_Value',
            'Humidity_Description', 'Humidity_Value',
            'SolRad-Diffuse_Description', 'SolRad-Diffuse_Value',
            'SolRad-Direct_Description', 'SolRad-Direct_Value',
            'Wind-Speed_Description', 'Wind-Speed_Value'
        ]]
        
        # Round values to 2 decimal places
        seasonal_data = seasonal_data.round(2)
        
        return seasonal_data
    
    def generate_weather_for_country(
        self,
        country: str,
        output_dir: Path,
        coordinates: Tuple[float, float] = None
    ) -> Dict[str, Path]:
        """
        Generate TMY weather profiles for a country and save to files.
        
        Args:
            country: Country name
            output_dir: Directory to save output files
            coordinates: Optional (lat, lon) tuple. If None, uses coordinates from config
            
        Returns:
            Dictionary mapping season names to file paths
        """
        # Get coordinates
        if coordinates is None:
            if country not in self.coordinates:
                raise ValueError(
                    f"No coordinates defined for {country}. "
                    f"Available countries: {list(self.coordinates.keys())}"
                )
            coordinates = self.coordinates[country]
        
        lat, lon = coordinates
        
        # Fetch and process data
        raw_data = self.fetch_tmy_data(country, lat, lon)
        seasonal_data = self.process_tmy_to_seasonal_profiles(raw_data, country)
        
        # Ensure output directory exists
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save each season to separate files
        saved_files = {}
        for season in seasonal_data['Season'].unique():
            season_data = seasonal_data[seasonal_data['Season'] == season]
            
            # Create filename
            country_clean = country.replace(' ', '-')
            filename = f"{country_clean}_{season}.csv"
            filepath = output_dir / filename
            
            # Save to CSV
            season_data.to_csv(filepath, index=False)
            saved_files[season] = filepath
            logger.info(f"Saved {season} weather data: {filepath}")
        
        return saved_files
    
    def generate_stage3_ranges(self, country: str, num_variants: int = 1, seasons: list = None) -> list:
        """
        Generate complete Stage 3 weather data from TMY.
        Returns list of season objects in Stage 3 format.
        
        Args:
            country: Country name
            num_variants: Number of variations per season (samples different days)
            seasons: Optional list of season names to filter by (e.g. ['summer', 'winter'])
            
        Returns:
            List of dictionaries in Stage 3 format, one per season per variant
        """
        import random
        
        coordinates = self.coordinates.get(country)
        if not coordinates:
            raise ValueError(f"No coordinates for {country}")
        
        lat, lon = coordinates
        raw_data = self.fetch_tmy_data(country, lat, lon)
        
        # Prepare data with season info
        data = raw_data.copy()
        data.reset_index(inplace=True)
        data['hour'] = data['time(UTC)'].dt.hour
        data['day'] = data['time(UTC)'].dt.day
        data['month'] = data['time(UTC)'].dt.month
        data['date_str'] = data['time(UTC)'].dt.strftime('%m-%d')
        
        hemisphere = 'southern' if country in self.southern_hemisphere else 'northern'
        data['season'] = data.apply(
            lambda row: self.get_season(row['day'], row['month'], hemisphere),
            axis=1
        )
        
        # Normalize to lowercase to match config/pipeline conventions
        data['season'] = data['season'].str.lower()
        
        # Normalize input seasons filter if provided
        target_seasons = None
        if seasons:
            target_seasons = [s.lower() for s in seasons]
            logger.info(f"Filtering TMY generation for seasons: {target_seasons}")
        
        # Generate Stage 3 objects for each season and variant
        stage3_outputs = []
        
        # Explicitly iterate over unique seasons found in data
        found_seasons = sorted(data['season'].unique())
        
        for season in found_seasons:
            # Apply filter if set
            if target_seasons and season not in target_seasons:
                continue
                
            season_data = data[data['season'] == season]
            
            # Get available days for this season
            available_days = sorted(season_data['date_str'].unique())
            
            for var_idx in range(1, num_variants + 1):
                # Sample a specific day for variation
                rng = random.Random(var_idx * 42)  # Deterministic seed
                selected_date = rng.choice(available_days)
                
                # Get hourly data for selected day
                day_data = season_data[season_data['date_str'] == selected_date].sort_values('hour')
                
                # Build hourly profiles (24 hours)
                temp_profile = day_data['temp_air'].round(2).tolist()
                hum_profile = day_data['relative_humidity'].round(2).tolist()
                diffuse_profile = day_data['dhi'].round(2).tolist()
                direct_profile = day_data['dni'].round(2).tolist()
                
                # Calculate min/max from seasonal data
                temp_min = round(season_data['temp_air'].min(), 2)
                temp_max = round(season_data['temp_air'].max(), 2)
                hum_min = round(season_data['relative_humidity'].min(), 2)
                hum_max = round(season_data['relative_humidity'].max(), 2)
                diffuse_min = round(season_data['dhi'].min(), 2)
                diffuse_max = round(season_data['dhi'].max(), 2)
                direct_min = round(season_data['dni'].min(), 2)
                direct_max = round(season_data['dni'].max(), 2)
                
                # Create Stage 3 format object
                # Note: We capitalize the season in output for Stage 3 consistency (e.g. "Winter")
                # if main.py expects Title Case. But earlier check showed "Winter" in output.
                # Let's emit Title Case for the "season" field to be safe/consistent with Schema?
                # Schema enum says "Winter", "Spring"...
                # So we title case it back.
                
                stage3_obj = {
                    "country": country,
                    "year": self.year,
                    "season": season.title(),
                    "sampled_date": selected_date,
                    "weather_data": {
                        "temperature": {
                            "min": temp_min,
                            "max": temp_max,
                            "hourly_profile": temp_profile
                        },
                        "humidity": {
                            "min": hum_min,
                            "max": hum_max,
                            "hourly_profile": hum_profile
                        },
                        "solar_radiation_diffuse": {
                            "min": diffuse_min,
                            "max": diffuse_max,
                            "hourly_profile": diffuse_profile
                        },
                        "solar_radiation_direct": {
                            "min": direct_min,
                            "max": direct_max,
                            "hourly_profile": direct_profile
                        }
                    }
                }
                
                stage3_outputs.append(stage3_obj)
        
        return stage3_outputs

    
    def generate_stage4_hourly(
        self,
        country: str,
        season: str,
        seed: int = None
    ) -> Dict[str, any]:
        """
        Generate Stage 4-like hourly weather profile for a specific season.
        Uses random day sampling for variations if seed is provided.
        
        Args:
            country: Country name
            season: Season name ('summer', 'winter', 'spring', 'autumn')
            seed: Optional seed for deterministic random sampling of a day
            
        Returns:
            Dictionary with hourly weather values
        """
        import random
        
        coordinates = self.coordinates.get(country)
        if not coordinates:
            raise ValueError(f"No coordinates for {country}")
        
        lat, lon = coordinates
        raw_data = self.fetch_tmy_data(country, lat, lon)
        
        # We need raw data with season info, but NOT averaged yet
        # Re-using logic from process_tmy_to_seasonal_profiles without aggregation
        
        # Reset index and extract time components
        # Note: raw_data might be a view or copy depending on fetch_tmy_data caching
        # So we process a fresh copy for sampling
        data = raw_data.copy()
        data.reset_index(inplace=True)
        data['hour'] = data['time(UTC)'].dt.hour
        data['day'] = data['time(UTC)'].dt.day
        data['month'] = data['time(UTC)'].dt.month
        data['date_str'] = data['time(UTC)'].dt.strftime('%m-%d')
        
        hemisphere = 'southern' if country in self.southern_hemisphere else 'northern'
        
        data['season'] = data.apply(
            lambda row: self.get_season(row['day'], row['month'], hemisphere),
            axis=1
        )
        
        # Filter for specific season (compare case-insensitive)
        # get_season now returns Title Case (Winter), pipeline uses lowercase (winter)
        season_data = data[data['season'].str.lower() == season.lower()]
        
        if season_data.empty:
            raise ValueError(f"No data for season '{season}' in {country}")
        
        # Get unique days available for this season
        available_days = sorted(season_data['date_str'].unique())
        
        if not available_days:
            raise ValueError(f"No days found for season '{season}'")
            
        # Select a day
        if seed is not None:
            # Deterministic selection based on seed
            rng = random.Random(seed)
            selected_date_str = rng.choice(available_days)
            logger.info(f"TMY Sampling: Selected {selected_date_str} for {season} (Seed: {seed})")
        else:
            # Default behavior: Average (Legacy/Fallback)
            # Revert to process_tmy_to_seasonal_profiles logic
            logger.info(f"TMY Sampling: No seed provided, using seasonal average for {season}")
            seasonal_data = self.process_tmy_to_seasonal_profiles(raw_data, country)
            season_profile = seasonal_data[seasonal_data['Season'] == season.lower()]
            
            hourly_data = []
            for _, row in season_profile.iterrows():
                hourly_data.append({
                    "hour": int(row['Hour']),
                    "temperature": row['Temperature_Value'],
                    "humidity": row['Humidity_Value'],
                    "solar_diffuse": row['SolRad-Diffuse_Value'],
                    "solar_direct": row['SolRad-Direct_Value'],
                    "wind_speed": row['Wind-Speed_Value']
                })
            
            return {
                "country": country,
                "season": season,
                "hourly_profile": hourly_data
            }

        # Filter data for the selected day
        day_profile = season_data[season_data['date_str'] == selected_date_str].copy()
        
        # Ensure we have 24 hours (fill missing if necessary, though TMY should be complete)
        # Sort by hour
        day_profile.sort_values('hour', inplace=True)
        
        # Renaming map for raw PVGIS columns to output format
        # PVGIS TMY cols: temp_air, relative_humidity, dhi, dni, ghi, wind_speed
        
        hourly_data = []
        for _, row in day_profile.iterrows():
            hourly_data.append({
                "hour": int(row['hour']),
                "temperature": round(row['temp_air'], 2),
                "humidity": round(row['relative_humidity'], 2),
                "solar_diffuse": round(row['dhi'], 2),
                "solar_direct": round(row['dni'], 2),
                "wind_speed": round(row['wind_speed'], 2)
            })
        
        return {
            "country": country,
            "season": season,
            "variable_date": selected_date_str, 
            "hourly_profile": hourly_data
        }
