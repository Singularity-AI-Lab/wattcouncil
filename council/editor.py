"""
JSON Editor - Edits existing JSON to fix specific issues.
"""
import json
import logging
from typing import Any
from string import Template
from council.client import LLMClient
from council.utils import extract_json

logger = logging.getLogger(__name__)


class Editor:
    """Edits existing JSON to fix specific issues instead of regenerating from scratch."""
    
    def __init__(self, client: LLMClient, model: str, prompts_dir: str):
        """
        Initialize editor.
        
        Args:
            client: LLM client instance
            model: Model identifier
            prompts_dir: Path to prompts directory
        """
        self.client = client
        self.model = model
        self.prompts_dir = prompts_dir
    
    def edit(self, original_json: str, fix_guidance: str) -> str:
        """
        Edit existing JSON to fix specific issues.
        
        Args:
            original_json: The original JSON with issues
            fix_guidance: Specific guidance on what to fix
        
        Returns:
            Edited JSON string
        """
        logger.info("Editing JSON with targeted fixes")
        
        # Load system prompt
        with open(f"{self.prompts_dir}/editor_json/system.txt", "r") as f:
            system_prompt = f.read()
        
        # Load user prompt template
        with open(f"{self.prompts_dir}/editor_json/user.txt", "r") as f:
            user_template_str = f.read()
        
        # Inject variables
        user_template = Template(user_template_str)
        user_prompt = user_template.safe_substitute(
            original_json=original_json,
            fix_guidance=fix_guidance
        )
        
        # Call LLM
        output = self.client.generate(user_prompt, self.model, system_prompt, role="editor")
        
        # Extract JSON from response
        output_json = extract_json(output)
        
        # Validate JSON
        try:
            json.loads(output_json)
            logger.info("Valid edited JSON generated")
        except json.JSONDecodeError as e:
            logger.warning(f"Edited output is not valid JSON: {e}")
            logger.warning(f"Raw output: {output[:500]}...")
        
        return output_json
