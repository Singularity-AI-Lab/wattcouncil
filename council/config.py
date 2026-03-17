"""
Configuration management for LLM Council Energy Pipeline.
"""
import yaml
from pathlib import Path
from typing import Dict, Any, Optional


class Config:
    """Configuration manager for loading and accessing config values."""
    
    def __init__(self, config_dir: str = "config"):
        """
        Initialize configuration manager.
        
        Args:
            config_dir: Directory containing config files
        """
        self.config_dir = Path(config_dir)
        
        # Load settings config
        settings_path = self.config_dir / "models.yaml"
        with open(settings_path, "r") as f:
            self.default = yaml.safe_load(f)
        
        # Load runtime config if it exists
        runtime_path = self.config_dir / "runtime.yaml"
        if runtime_path.exists():
            with open(runtime_path, "r") as f:
                self.runtime = yaml.safe_load(f)
        else:
            self.runtime = {}
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get a config value using dot notation.
        
        Args:
            key_path: Dot-separated path (e.g., "roles.generator.temperature")
            default: Default value if key not found
        
        Returns:
            Config value
        """
        keys = key_path.split(".")
        
        # Try runtime config first
        value = self._get_nested(self.runtime, keys)
        if value is not None:
            return value
        
        # Fall back to default config
        value = self._get_nested(self.default, keys)
        return value if value is not None else default
    
    def _get_nested(self, config: Dict, keys: list) -> Any:
        """Get nested value from config dict."""
        current = config
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        return current
    
    def get_provider_config(self, provider: Optional[str] = None) -> Dict[str, Any]:
        """
        Get provider configuration.
        
        Args:
            provider: Provider name (uses active_provider if None)
        
        Returns:
            Provider config dict
        """
        if provider is None:
            provider = self.get("active_provider", "deepinfra")
        
        return self.get(f"providers.{provider}", {})
    
    def get_role_config(self, role: str) -> Dict[str, Any]:
        """
        Get role-specific configuration.
        
        Args:
            role: Role name (e.g., "generator", "cultural_auditor")
        
        Returns:
            Role config dict with merged defaults
        """
        role_config = self.get(f"roles.{role}", {})
        defaults = self.get("model_defaults", {})
        
        # Merge with defaults (role config takes precedence)
        return {**defaults, **role_config}
    
    def get_stage_config(self, stage: str) -> Dict[str, Any]:
        """
        Get stage-specific configuration from runtime.
        
        Args:
            stage: Stage name (e.g., "stage1_family")
        
        Returns:
            Stage config dict
        """
        return self.get(f"stages.{stage}", {})
    
    def get_pipeline_config(self) -> Dict[str, Any]:
        """
        Get pipeline execution configuration from runtime.
        
        Returns:
            Pipeline config dict
        """
        return self.get("pipeline", {})
    
    def get_orchestrator_config(self) -> Dict[str, Any]:
        """
        Get orchestrator configuration.
        
        Returns:
            Orchestrator config dict
        """
        return self.get("orchestrator", {})
    
    def get_paths_config(self) -> Dict[str, Any]:
        """
        Get file paths configuration.
        
        Returns:
            Paths config dict
        """
        return self.get("paths", {})
