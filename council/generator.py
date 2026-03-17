"""
Primary Generator - produces candidate structured outputs.
"""
import re
import json
import logging
from typing import Dict, Any
from string import Template
from council.client import LLMClient
from council.utils import extract_json

logger = logging.getLogger(__name__)


class PrimaryGenerator:
    """Generates structured output for each pipeline stage."""
    
    def __init__(self, client: LLMClient, model: str, prompts_dir: str, knowledge_base_path: str = None):
        """
        Initialize generator.
        
        Args:
            client: LLM client instance
            model: Model identifier
        model: Model identifier
            prompts_dir: Path to prompts directory
            knowledge_base_path: Optional path to persistent knowledge file
        """
        self.client = client
        self.model = model
        self.prompts_dir = prompts_dir
        self.knowledge_base_path = knowledge_base_path
    
    def load_schema(self, stage: str) -> str:
        """
        Load JSON schema for a stage if it exists.
        
        Returns:
            JSON schema as formatted string, or empty string if not found
        """
        import os
        import json
        
        schema_map = {
            "stage1_family": "stage1_families",
            "stage2_weather": "stage2_weather_profiles", 
            "stage3_consumption": "stage3_consumption"
        }
        
        schema_name = schema_map.get(stage, stage)
        schema_file = f"{self.prompts_dir}/../schemas/{schema_name}.json"
        
        if os.path.exists(schema_file):
            with open(schema_file, "r") as f:
                schema = json.load(f)
            # Return pretty-printed JSON for readability in prompts
            return json.dumps(schema, indent=2)
        
        return ""
    
    def load_prompt(self, stage: str) -> tuple:
        """
        Load prompt template(s) from stage folder.
        
        Returns:
            Tuple of (system_prompt, user_prompt)
        """
        import os
        
        # Load from folder structure
        stage_folder = f"{self.prompts_dir}/{stage}"
        system_file = f"{stage_folder}/system.txt"
        user_file = f"{stage_folder}/user.txt"
        
        if os.path.exists(system_file) and os.path.exists(user_file):
            with open(system_file, "r") as f:
                system_prompt = f.read()
            with open(user_file, "r") as f:
                user_prompt = f.read()
            return system_prompt, user_prompt
        else:
            raise FileNotFoundError(f"No prompt files found for stage: {stage}")
    
    
    def generate(self, stage: str, variables: Dict[str, Any], seed: int = None) -> str:
        """
        Generate output for a given stage.
        
        Args:
            stage: Stage name (e.g., "stage1_family")
            variables: Dictionary of variables to inject into prompt
            seed: Optional seed for deterministic generation
        
        Returns:
            Generated JSON string
        """
        logger.info(f"Generating output for {stage}")
        
        # Load prompt templates
        system_prompt, user_prompt = self.load_prompt(stage)
        
        # Load schema if exists and inject into variables
        schema_str = self.load_schema(stage)
        if schema_str:
            variables["SCHEMA"] = schema_str
            logger.info(f"Loaded schema for {stage}")
        
        # Inject variables into user prompt
        user_template = Template(user_prompt)
        user_prompt_filled = user_template.safe_substitute(**variables)
        
        # Inject regeneration guidance if present
        if "regeneration_guidance" in variables:
            user_prompt_filled += f"\n\nIMPORTANT GUIDANCE FOR REGENERATION:\n{variables['regeneration_guidance']}"
            logger.info("Injected regeneration guidance into prompt")
            
        # Inject accumulated knowledge if file exists
        if self.knowledge_base_path:
            import os
            if os.path.exists(self.knowledge_base_path):
                try:
                    with open(self.knowledge_base_path, "r") as f:
                        knowledge = f.read().strip()
                    if knowledge:
                        user_prompt_filled += f"\n\nLESSONS LEARNED FROM PREVIOUS ATTEMPTS (MUST FOLLOW):\n{knowledge}"
                        logger.info(f"Injected accumulated knowledge from {self.knowledge_base_path}")
                except Exception as e:
                    logger.warning(f"Failed to read knowledge base: {e}")
        
        # Generate
        raw_output = self.client.generate(user_prompt_filled, self.model, system_prompt, role="generator", seed=seed)
        
        # Extract JSON from output
        output = extract_json(raw_output)
        
        # Validate JSON
        try:
            json.loads(output)
            logger.info(f"Valid JSON generated for {stage}")
        except json.JSONDecodeError as e:
            logger.warning(f"Generated output is not valid JSON: {e}")
            logger.warning(f"Raw output: {raw_output[:500]}...")
        
        return output