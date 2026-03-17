"""
Approver - Verifies editor changes without re-running full audits.
"""
import json
import logging
from typing import Dict, Any
from council.client import LLMClient
from council.utils import extract_json

logger = logging.getLogger(__name__)


class Approver:
    """Verifies that editor successfully applied CEO's guidance."""
    
    def __init__(self, client: LLMClient, model: str, prompts_dir: str):
        """
        Initialize Approver.
        
        Args:
            client: LLM client instance
            model: Model identifier
            prompts_dir: Path to prompts directory
        """
        self.client = client
        self.model = model
        self.prompts_dir = prompts_dir
    
    def verify_edit(
        self,
        stage: str,
        old_output: str,
        new_output: str,
        guidance: str
    ) -> Dict[str, Any]:
        """
        Verify that editor successfully applied the guidance.
        
        This is more cost-effective than re-running auditors after REGENERATE_PARTIAL.
        Approver reviews old JSON, new JSON, and guidance to ensure changes are correct.
        
        Args:
            stage: Stage name
            old_output: Original output before editor changes
            new_output: Output after editor modifications
            guidance: The regeneration guidance given to the editor
        
        Returns:
            Decision dictionary with decision (ACCEPT or REGENERATE_FULL) and reason
        """
        logger.info(f"Approver verifying editor changes for {stage}")
        
        # Load system prompt for verification
        with open(f"{self.prompts_dir}/approver/system.txt", "r") as f:
            system_prompt = f.read()
        
        # Load user prompt template for verification
        with open(f"{self.prompts_dir}/approver/user.txt", "r") as f:
            user_template_str = f.read()
        
        from string import Template
        user_template = Template(user_template_str)
        
        # Inject variables
        user_prompt = user_template.safe_substitute(
            stage=stage,
            old_output=old_output,
            new_output=new_output,
            guidance=guidance
        )
        
        # Get decision (pass system and user prompts separately for proper logging)
        decision_output = self.client.generate(user_prompt, self.model, system_prompt=system_prompt, role="approver")
        
        # Extract and parse JSON
        try:
            extracted_json = extract_json(decision_output)
            decision = json.loads(extracted_json)
            logger.info(f"Approver verification: {decision.get('decision', 'UNKNOWN')}")
            return decision
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Approver verification: {e}")
            return {
                "decision": "REGENERATE_FULL",
                "reason": "Failed to parse Approver verification decision",
                "regeneration_guidance": "Please regenerate with valid JSON format"
            }
