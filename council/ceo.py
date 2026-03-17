"""
CEO - Final decision maker for accept/regenerate.
"""
import json
import logging
from typing import Dict, Any
from council.client import LLMClient
from council.utils import extract_json, get_stage_folder

logger = logging.getLogger(__name__)


class CEO:
    """Makes final decisions on generated content."""
    
    def __init__(self, client: LLMClient, model: str, prompts_dir: str):
        """
        Initialize CEO.
        
        Args:
            client: LLM client instance
            model: Model identifier
            prompts_dir: Path to prompts directory
        """
        self.client = client
        self.model = model
        self.prompts_dir = prompts_dir
    
    def decide(
        self,
        stage: str,
        generated_output: str,
        cultural_audit: Dict[str, Any],
        physical_audit: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Make final decision on generated output.
        
        Args:
            stage: Stage name
            generated_output: Generated output
            cultural_audit: Cultural audit report
            physical_audit: Physical audit report
        
        Returns:
            Decision dictionary with decision, reason, and regeneration_guidance
        """
        logger.info(f"CEO decision for {stage}")
        
        # Load system prompt
        with open(f"{self.prompts_dir}/ceo/system.txt", "r") as f:
            system_prompt = f.read()
        
        # Load user prompt template
        with open(f"{self.prompts_dir}/ceo/user.txt", "r") as f:
            user_template_str = f.read()
        
        # Load stage-specific considerations
        stage_folder = get_stage_folder(stage)
        with open(f"{self.prompts_dir}/{stage_folder}/ceo.txt", "r") as f:
            stage_specific_considerations = f.read()
        
        from string import Template
        user_template = Template(user_template_str)
        
        # Inject variables using safe_substitute
        user_prompt = user_template.safe_substitute(
            stage=stage,
            generated_output=json.dumps(generated_output, separators=(',', ':')),  # Matches $generated_output in template
            cultural_audit=json.dumps(cultural_audit, separators=(',', ':')),
            physical_audit=json.dumps(physical_audit, separators=(',', ':')),
            stage_specific_considerations=stage_specific_considerations
        )
        
        # Get decision (pass system and user prompts separately for proper logging)
        decision_output = self.client.generate(user_prompt, self.model, system_prompt=system_prompt, role="ceo")
        
        # Extract and parse JSON
        try:
            extracted_json = extract_json(decision_output)
            decision = json.loads(extracted_json)
            logger.info(f"CEO decision: {decision.get('decision', 'UNKNOWN')}")
            return decision
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse CEO decision: {e}")
            return {
                "decision": "REGENERATE_FULL",
                "reason": "Failed to parse CEO decision",
                "regeneration_guidance": "Please regenerate with valid JSON format"
            }
