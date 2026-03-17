import json
import time
import os
import uuid
import yaml
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

# Global logger instance
_logger_instance = None

def get_logger(log_dir: str = "logs", enabled: bool = True, provider: str = "deepinfra"):
    """Get or create global API logger instance"""
    global _logger_instance
    if _logger_instance is None:
        _logger_instance = APILogger(log_dir, enabled, provider)
    return _logger_instance

class SafeJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime,)):
            return obj.isoformat()
        try:
            return str(obj)
        except Exception:
            return repr(obj)

class APILogger:
    """Logs LLM API calls to a JSON file for analysis and auditing."""
    
    def __init__(self, log_dir: str = "logs", enabled: bool = True, provider: str = "deepinfra"):
        self.enabled = enabled
        self.log_dir = Path(log_dir)
        self.log_file = None
        self.first_entry = True
        self.is_closed = False
        self.provider = provider.lower()
        self.model_costs = self._load_model_costs()
        
        # Track prompt counts per role for naming
        self.prompt_counters = {}
        
        if self.enabled:
            self.log_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.log_file = self.log_dir / f"api_calls_{timestamp}.json"
            
            # Create prompts subdirectory
            self.prompts_dir = self.log_dir / "prompts"
            self.prompts_dir.mkdir(parents=True, exist_ok=True)
            
            # Initialize file with start of array
            with open(self.log_file, "w", encoding="utf-8") as f:
                f.write("[\n")
            
            print(f"✅ API logger initialized: {self.log_file}")
            print(f"✅ Prompts will be saved to: {self.prompts_dir}")
    
    def _load_model_costs(self) -> Dict[str, Dict[str, float]]:
        """Load model cost configuration from YAML file for the current provider."""
        cost_file = Path("config/model_costs.yaml")
        
        if not cost_file.exists():
            print(f"⚠️ Warning: {cost_file} not found, using default costs")
            return {
                "default": {
                    "input_cost_per_million": 1.0,
                    "output_cost_per_million": 3.0
                }
            }
        
        try:
            with open(cost_file, 'r') as f:
                config = yaml.safe_load(f)
                
            # Get provider-specific costs, fall back to deepinfra, then default
            provider_costs = config.get(self.provider, config.get('deepinfra', {}))
            
            # Add default costs from config
            if 'default' in config:
                provider_costs['default'] = config['default']
            
            return provider_costs
            
        except Exception as e:
            print(f"⚠️ Error loading model costs: {e}, using defaults")
            return {
                "default": {
                    "input_cost_per_million": 1.0,
                    "output_cost_per_million": 3.0
                }
            }

    def log_api_call(self, prompt: str, response: str, model: str, duration: float, 
                     system_prompt: Optional[str] = None, 
                     input_tokens: Optional[int] = None, 
                     output_tokens: Optional[int] = None,
                     metadata: Optional[Dict[str, Any]] = None):
        """Log a single API interaction."""
        if not self.enabled or self.is_closed:
            return
        
        # Save full composed prompt to text file
        self._save_prompt_to_file(prompt, system_prompt, metadata)
            
        # Calculate cost using model_costs configuration
        cost = 0.0
        if input_tokens and output_tokens:
            # Try exact model match first
            model_config = self.model_costs.get(model)
            
            # If not found, try fallback to default
            if not model_config:
                model_config = self.model_costs.get('default', {
                    'input_cost_per_million': 1.0,
                    'output_cost_per_million': 3.0
                })
            
            input_rate = model_config.get('input_cost_per_million', 1.0)
            output_rate = model_config.get('output_cost_per_million', 3.0)
            
            cost = (input_tokens * input_rate + output_tokens * output_rate) / 1_000_000

        entry = {
            "timestamp": datetime.now().isoformat(),
            "id": str(uuid.uuid4()),
            "model": model,
            "duration_seconds": duration,
            "prompt": prompt,
            "system_prompt": system_prompt,
            "response": response,
            "tokens": {
                "input": input_tokens,
                "output": output_tokens,
                "total": (input_tokens or 0) + (output_tokens or 0)
            },
            "cost_usd": {
                "input": (input_tokens or 0) * input_rate / 1_000_000 if input_tokens else 0.0,
                "output": (output_tokens or 0) * output_rate / 1_000_000 if output_tokens else 0.0,
                "total": cost
            },
            "metadata": metadata or {}
        }
        
        try:
            json_entry = json.dumps(entry, cls=SafeJSONEncoder, indent=2)
            
            # Append comma if not first entry
            prefix = ",\n" if not self.first_entry else ""
            
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(prefix + json_entry)
            
            self.first_entry = False
            
        except Exception as e:
            print(f"⚠️ Failed to log API call: {e}")
    
    def _save_prompt_to_file(self, prompt: str, system_prompt: Optional[str] = None, 
                             metadata: Optional[Dict[str, Any]] = None):
        """Save the full composed prompt (system + user) to a text file."""
        if not self.enabled or self.is_closed:
            return
        
        try:
            # Extract role from metadata, default to 'unknown'
            role = "unknown"
            if metadata and "role" in metadata:
                role = metadata["role"].lower()
            
            # Increment counter for this role
            if role not in self.prompt_counters:
                self.prompt_counters[role] = 0
            self.prompt_counters[role] += 1
            
            # Format filename with timestamp prefix: YYYYMMDD_HHMMSS_role##.txt
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{timestamp}_{role}{self.prompt_counters[role]:02d}.txt"
            filepath = self.prompts_dir / filename
            
            # Compose full prompt (system + user)
            full_prompt = ""
            if system_prompt:
                full_prompt += "=" * 80 + "\n"
                full_prompt += "SYSTEM PROMPT\n"
                full_prompt += "=" * 80 + "\n"
                full_prompt += system_prompt + "\n\n"
            
            full_prompt += "=" * 80 + "\n"
            full_prompt += "USER PROMPT\n"
            full_prompt += "=" * 80 + "\n"
            full_prompt += prompt + "\n"
            
            # Write to file
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(full_prompt)
            
        except Exception as e:
            print(f"⚠️ Failed to save prompt to file: {e}")

    def close(self):
        """Finalize the JSON log file."""
        if self.enabled and not self.is_closed and self.log_file:
            try:
                with open(self.log_file, "a", encoding="utf-8") as f:
                    f.write("\n]")
                self.is_closed = True
            except Exception as e:
                print(f"⚠️ Failed to close API log file: {e}")
