"""
Centralized constants loader for the LLM Council Energy project.

Loads constants from config/constants.yaml and provides typed access.
All scripts should use this module instead of hardcoding values.
"""
import yaml
from pathlib import Path
from typing import Dict, List, Any


class Constants:
    """Singleton class for loading and accessing shared constants."""
    
    _instance = None
    _config = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._load_config()
        return cls._instance
    
    @classmethod
    def _load_config(cls):
        """Load constants from config/constants.yaml"""
        config_path = Path(__file__).parent.parent / "config" / "constants.yaml"
        
        if not config_path.exists():
            raise FileNotFoundError(
                f"Constants config not found at {config_path}. "
                "This file is required for pipeline execution."
            )
        
        with open(config_path, 'r') as f:
            cls._config = yaml.safe_load(f)
    
    # Country Mappings
    
    @property
    def country_code_to_name(self) -> Dict[str, str]:
        """ISO country code to full country name mapping."""
        return self._config['countries']['code_to_name']
    
    @property
    def country_name_to_code(self) -> Dict[str, str]:
        """Country name to ISO code mapping (includes reverse mapping + aliases)."""
        mapping = {v: k for k, v in self.country_code_to_name.items()}
        mapping.update(self._config['countries']['aliases'])
        return mapping
    
    @property
    def weekends_by_country(self) -> Dict[str, List[Dict[str, Any]]]:
        """Weekend definitions by country code with historical date ranges."""
        return self._config['countries']['weekends']
    
    # Time Data Settings
    
    @property
    def time_step(self) -> str:
        """Pandas frequency string for time series generation."""
        return self._config['time_data']['time_step']
    
    @property
    def start_time_template(self) -> str:
        """Start time template (use .format(year=YYYY) to fill)."""
        return self._config['time_data']['start_template']
    
    @property
    def end_time_template(self) -> str:
        """End time template (use .format(year=YYYY) to fill)."""
        return self._config['time_data']['end_template']
    
    @property
    def yearly_file_template(self) -> str:
        """Base filename template for yearly time data files."""
        return self._config['time_data']['yearly_file_template']
    
    # Data Expansion Settings
    
    @property
    def family_noise_factor(self) -> float:
        """Noise factor for family consumption data expansion."""
        return self._config['data_expansion']['noise_factors']['family']
    
    @property
    def weather_noise_factor(self) -> float:
        """Noise factor for weather data expansion."""
        return self._config['data_expansion']['noise_factors']['weather']
    
    # Plotting Settings
    
    @property
    def season_colors(self) -> Dict[str, str]:
        """Color mapping for seasons in plots."""
        return self._config['plotting']['season_colors']
    
    @property
    def energy_signature_aggregate_by(self) -> str:
        """Aggregation level for energy signature plots."""
        return self._config['plotting']['energy_signatures']['aggregate_by']
    
    @property
    def energy_signature_max_families(self) -> int:
        """Maximum number of families to plot per country."""
        return self._config['plotting']['energy_signatures']['max_families_per_country']
    
    @property
    def energy_signature_save_plots(self) -> bool:
        """Whether to save energy signature plots."""
        return self._config['plotting']['energy_signatures']['save_plots']
    
    @property
    def plot_labels(self) -> Dict[str, str]:
        """Display labels for plot axes and legends."""
        return self._config['plotting']['labels']

    # Targeted Generation Mappings
    
    @property
    def house_type_map(self) -> Dict[int, str]:
        """Map of house type IDs to descriptions."""
        return {
            1: "Apartment",
            2: "Semi-detached house",
            3: "Detached house",
            4: "Terraced house",
            5: "Bungalow",
        }

    @property
    def n_people_map(self) -> Dict[int, str]:
        """Map of number of people IDs to descriptions."""
        return {
            1: "1",
            2: "2",
            3: "3",
            4: "4",
            5: "5 or more",
        }

    @property
    def household_composition_map(self) -> Dict[int, str]:
        """Map of household composition IDs to descriptions."""
        return {
            1: "Lives alone",
            2: "All people in home are over 15 years of age",
            3: "Both adults and children under 15 years of age live in home",
        }

    @property
    def employment_status_map(self) -> Dict[int, str]:
        """Map of employment status IDs to descriptions."""
        return {
            1: "employee",
            2: "self-employed",
            3: "unemployed",
            4: "retired",
        }


# Global singleton instance - import this in scripts
CONSTANTS = Constants()
