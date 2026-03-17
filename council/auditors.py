"""
Auditors - Cultural and Physical constraint validation.
"""
import json
import logging
from typing import Dict, Any
from abc import ABC, abstractmethod
from string import Template
from council.client import LLMClient
from council.utils import extract_json, get_stage_folder

logger = logging.getLogger(__name__)


class BaseAuditor(ABC):
    """Base class for all auditors."""
    
    def __init__(self, client: LLMClient, model: str, prompts_dir: str):
        """
        Initialize auditor.
        
        Args:
            client: LLM client instance
            model: Model identifier
            prompts_dir: Path to prompts directory
        """
        self.client = client
        self.model = model
        self.prompts_dir = prompts_dir
    
    @property
    @abstractmethod
    def auditor_type(self) -> str:
        """Return the auditor type (e.g., 'cultural', 'physical')."""
        pass
    
    @property
    @abstractmethod
    def stage_content_key(self) -> str:
        """Return the template variable name for stage-specific content."""
        pass
    
    @abstractmethod
    def get_error_response(self) -> Dict[str, Any]:
        """Return the error response structure for this auditor."""
        pass
    
    def audit(self, stage: str, generated_output: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Audit generated output.
        
        Args:
            stage: Stage name
            generated_output: Output to audit
            context: Context variables (country, season, etc.)
        
        Returns:
            Audit report as dictionary
        """
        logger.info(f"{self.auditor_type.capitalize()} audit for {stage}")
        
        # Load system prompt
        with open(f"{self.prompts_dir}/auditor_{self.auditor_type}/system.txt", "r") as f:
            system_prompt = f.read()
        
        # Load user prompt template
        with open(f"{self.prompts_dir}/auditor_{self.auditor_type}/user.txt", "r") as f:
            user_template_str = f.read()
        
        # Load stage-specific content
        stage_folder = get_stage_folder(stage)
        with open(f"{self.prompts_dir}/{stage_folder}/auditor_{self.auditor_type}.txt", "r") as f:
            stage_specific_content = f.read().strip()
        
        # Skip auditing if content is a placeholder (starts with #)
        if not stage_specific_content or stage_specific_content.startswith("#"):
            logger.info(f"{self.auditor_type.capitalize()} audit skipped for {stage} (no criteria defined)")
            # Return a passing audit with no issues
            if self.auditor_type == "cultural":
                return {"issues_found": False, "severity": "NONE"}
            else:  # physical
                return {"violations_found": False, "severity": "NONE"}
        
        user_template = Template(user_template_str)
        
        # Build formatted context string from all context variables
        context_lines = []
        for key, value in context.items():
            if value:  # Only include non-empty values
                # Format key: replace underscores with spaces and title case
                formatted_key = key.replace('_', ' ').title()
                context_lines.append(f"- {formatted_key}: {value}")
        
        formatted_context = "\n".join(context_lines) if context_lines else "- No additional context"
        
        # Inject variables using safe_substitute
        template_vars = {
            "stage": stage,
            "generated_output": generated_output,
            "context": formatted_context,
            self.stage_content_key: stage_specific_content
        }
        user_prompt = user_template.safe_substitute(**template_vars)

        
        # Get audit report (pass system and user prompts separately for proper logging)
        audit_output = self.client.generate(user_prompt, self.model, system_prompt=system_prompt, role=f"auditor_{self.auditor_type}")
        
        # Extract and parse JSON
        try:
            extracted_json = extract_json(audit_output)
            audit_report = json.loads(extracted_json)
            logger.info(f"{self.auditor_type.capitalize()} audit complete: {audit_report.get('severity', 'UNKNOWN')}")
            return audit_report
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse audit report: {e}")
            return self.get_error_response()


class CulturalAuditor(BaseAuditor):
    """Reviews content for cultural appropriateness."""
    
    @property
    def auditor_type(self) -> str:
        return "cultural"
    
    @property
    def stage_content_key(self) -> str:
        return "stage_specific_criteria"
    
    def get_error_response(self) -> Dict[str, Any]:
        return {
            "issues_found": True,
            "issue_descriptions": ["Failed to parse audit report"],
            "severity": "HIGH"
        }


class PhysicalAuditor(BaseAuditor):
    """Reviews content for physical plausibility."""
    
    @property
    def auditor_type(self) -> str:
        return "physical"
    
    @property
    def stage_content_key(self) -> str:
        return "stage_specific_constraints"
    
    def get_error_response(self) -> Dict[str, Any]:
        return {
            "violations_found": True,
            "violated_constraints": ["Failed to parse audit report"],
            "severity": "HIGH"
        }
