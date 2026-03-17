"""
Pipeline utilities for council test execution.

Provides reusable initialization and execution logic shared across all stage tests.
"""
import json
# import logging
from typing import Optional, Dict, Any
from pathlib import Path

# Import core council components
from council.config import Config
from council.client import LLMClient
from council.generator import PrimaryGenerator
from council.auditors import CulturalAuditor, PhysicalAuditor
from council.ceo import CEO
from council.editor import Editor
from council.approver import Approver

# logger = logging.getLogger(__name__)

# Import formatting utilities
from utils.output_formatting import (
    Icons, Colors, colored,
    print_section, print_success, print_warning, print_error,
    print_info, print_progress, print_result, format_severity, format_decision,
    save_test_output, print_test_summary
)

# Import API logger
from utils.api_logger import APILogger, get_logger


def initialize_council(config: Config) -> Dict[str, Any]:
    """
    Initialize all council components and print configuration.
    
    Args:
        config: Config object
        
    Returns:
        Dictionary containing all initialized components:
        - client: LLMClient instance
        - generator: PrimaryGenerator instance
        - cultural_auditor: CulturalAuditor instance
        - physical_auditor: PhysicalAuditor instance
        - editor: Editor instance
        - approver: Approver instance
        - ceo: CEO instance
        - api_logger: APILogger instance
    """
    # Get model configurations
    generator_config = config.get_role_config("generator")
    auditor_config = config.get_role_config("cultural_auditor")
    
    # Print model information
    print_info(f"\nModels:")
    print_info(f"  Generator:        {colored(generator_config['model'], Colors.CYAN)}", 1)
    print_info(f"  Cultural Auditor: {colored(auditor_config['model'], Colors.CYAN)}", 1)
    print_info(f"  Physical Auditor: {colored(config.get_role_config('physical_auditor')['model'], Colors.CYAN)}", 1)
    print_info(f"  Editor:           {colored(config.get_role_config('editor')['model'], Colors.CYAN)}", 1)
    print_info(f"  Approver:         {colored(config.get_role_config('approver')['model'], Colors.CYAN)}", 1)
    print_info(f"  CEO:              {colored(config.get_role_config('ceo')['model'], Colors.CYAN)}\n", 1)
    
    # Initialize components
    print_section("Initializing Components", Icons.GEAR)
    
    prompts_dir = config.get("paths.prompts_dir", "prompts")
    
    # Initialize API logger - get output dir and provider from config or use defaults
    output_dir = config.get("paths.output_dir", "outputs")
    log_dir = Path(output_dir) / "logs"
    provider = config.get("active_provider", "deepinfra")
    api_logger = get_logger(log_dir=str(log_dir), enabled=True, provider=provider)
    print_success(f"API logger initialized: {api_logger.log_file}")
    
    client = LLMClient(config=config, api_logger=api_logger)
    
    generator = PrimaryGenerator(
        client=client,
        model=generator_config["model"],
        prompts_dir=prompts_dir,
        knowledge_base_path=str(log_dir.parent / "knowledge_base.txt")
    )
    
    cultural_auditor = CulturalAuditor(
        client=client,
        model=auditor_config["model"],
        prompts_dir=prompts_dir
    )
    
    physical_auditor = PhysicalAuditor(
        client=client,
        model=config.get_role_config("physical_auditor")["model"],
        prompts_dir=prompts_dir
    )
    
    ceo = CEO(
        client=client,
        model=config.get_role_config("ceo")["model"],
        prompts_dir=prompts_dir
    )
    
    editor = Editor(
        client=client,
        model=config.get_role_config("editor")["model"],
        prompts_dir=prompts_dir
    )
    
    approver = Approver(
        client=client,
        model=config.get_role_config("approver")["model"],
        prompts_dir=prompts_dir
    )
    
    print_success("All components initialized")
    
    return {
        "client": client,
        "generator": generator,
        "cultural_auditor": cultural_auditor,
        "physical_auditor": physical_auditor,
        "ceo": ceo,
        "editor": editor,
        "approver": approver,
        "api_logger": api_logger
    }


def run_council_pipeline(
    stage_name: str,
    variables: Dict[str, Any],
    context: Dict[str, Any],
    components: Dict[str, Any],
    max_attempts: int = 3,
    custom_parser: Optional[callable] = None,
    custom_success_handler: Optional[callable] = None,
    seed: int = None
) -> Optional[str]:
    """
    Execute the standard generate → audit → decide pipeline with retries.
    
    Args:
        stage_name: Stage identifier
        variables: Variables to pass to generator
        context: Context to pass to auditors
        components: Dictionary of initialized council components
        max_attempts: Maximum number of retry attempts (default: 3)
        custom_parser: Optional custom parser function(output) -> dict
        custom_success_handler: Optional custom function(output, data, attempt) -> None
            If not provided, uses default save and summary
        seed: Optional seed for deterministic generation
            
    Returns:
        Accepted output string or None if all attempts fail
    """
    generator = components["generator"]
    cultural_auditor = components.get("cultural_auditor")  # Optional
    physical_auditor = components["physical_auditor"]
    ceo = components["ceo"]
    editor = components["editor"]
    approver = components["approver"]
    
    for attempt in range(max_attempts):
        print_section(f"Attempt {attempt + 1}/{max_attempts}", Icons.INFO)
        
        # Generate
        try:
            print_progress("Generating output", Icons.GENERATOR)
            output = generator.generate(stage_name, variables.copy(), seed=seed)
            print_result("DONE")
            
            # Parse JSON
            if custom_parser:
                data = custom_parser(output)
            else:
                from council.utils import extract_json
                cleaned_output = extract_json(output)
                data = json.loads(cleaned_output)
            
            print_success(f"Generated output successfully")
            
            # Handle different data types for display
            if isinstance(data, list):
                print_info(f"Generated {len(data)} items", 1)
            elif isinstance(data, dict):
                # Show first few keys as sample
                sample_keys = list(data.keys())[:3]
                print_info(f"Keys: {', '.join(sample_keys)}", 1)
                
        except json.JSONDecodeError as e:
            print_error(f"Invalid JSON: {e}")
            continue
        except Exception as e:
            print_error(f"Generation failed: {e}")
            continue
        
        # Cultural Audit
        cultural_result = {}
        if cultural_auditor:
            print_progress("Cultural audit", Icons.AUDIT)
            cultural_result = cultural_auditor.audit(stage_name, output, context)
            severity = cultural_result.get('severity', 'UNKNOWN')
            
            # Skip display if auditing was not applicable
            if severity == 'NONE':
                print_result("SKIPPED")
            else:
                print_result(format_severity(severity))
                if cultural_result.get('issues_found'):
                    for issue in cultural_result.get('issue_descriptions', []):
                        print_info(f"└─ {issue}", 2)
        
        # Physical Audit
        print_progress("Physical audit", Icons.AUDIT)
        physical_result = physical_auditor.audit(stage_name, output, context)
        print_result(format_severity(physical_result.get('severity', 'UNKNOWN')))
        
        if physical_result.get('violations_found'):
            for violation in physical_result.get('violated_constraints', []):
                print_info(f"└─ {violation}", 2)
        
        # CEO Decision
        print_progress("CEO decision", Icons.CEO)
        decision = ceo.decide(stage_name, output, cultural_result, physical_result)
        decision_type = decision.get('decision', 'UNKNOWN')
        print_result(format_decision(decision_type))
        
        if decision_type == 'ACCEPT':
            # Handle success
            if custom_success_handler:
                custom_success_handler(output, data, attempt + 1)
            else:
                # Default success handling
                output_file = save_test_output(stage_name.split('_')[0], output)
                print_test_summary(
                    stage_name.split('_')[0],
                    attempt + 1,
                    {"output_type": type(data).__name__},
                    output_file
                )
            return output
            
        elif decision_type == 'REGENERATE_PARTIAL':
            # Use Editor for targeted fixes, then CEO verifies the changes
            print_warning(f"Reason: {decision.get('reason')}")
            guidance = decision.get('regeneration_guidance', '')
            if guidance:
                print_info(f"Guidance: {guidance}", 1)
            
            try:
                # Save old output for comparison
                old_output = output
                
                # Editor applies fixes
                print_progress("Editor making targeted fixes", Icons.GENERATOR)
                output = editor.edit(output, guidance)
                print_result("DONE")
                
                # Approver verifies the edit (cheaper than re-running auditors)
                print_progress("Approver verifying editor changes", Icons.CEO)
                verification = approver.verify_edit(stage_name, old_output, output, guidance)
                verification_decision = verification.get('decision', 'UNKNOWN')
                print_result(format_decision(verification_decision))
                
                if verification_decision == 'ACCEPT':
                    # Editor succeeded, treat as accepted
                    print_success("Editor changes approved by Approver")
                    
                    # Re-parse the edited output for success handler
                    try:
                        if custom_parser:
                            data = custom_parser(output)
                        else:
                            from council.utils import extract_json
                            cleaned_output = extract_json(output)
                            data = json.loads(cleaned_output)
                    except Exception as parse_error:
                        print_error(f"Failed to parse edited output: {parse_error}")
                        continue
                    
                    # Handle success
                    if custom_success_handler:
                        custom_success_handler(output, data, attempt + 1)
                    else:
                        output_file = save_test_output(stage_name.split('_')[0], output)
                        print_test_summary(
                            stage_name.split('_')[0],
                            attempt + 1,
                            {"output_type": type(data).__name__},
                            output_file
                        )
                    return output
                else:
                    # CEO rejected editor changes, fall back to full regeneration
                    print_warning(f"CEO verification failed: {verification.get('reason')}")
                    # Continue to next attempt with full regeneration
                    
            except Exception as e:
                print_error(f"Editor failed: {e}")
                print_warning("Falling back to full regeneration")
                # Continue to next attempt
                
        elif decision_type == 'REGENERATE_FULL':
            print_warning(f"Reason: {decision.get('reason')}")
            if decision.get('regeneration_guidance'):
                guidance = decision.get('regeneration_guidance')
                print_info(f"Guidance: {guidance}", 1)
                # Pass guidance to the next attempt
                variables['regeneration_guidance'] = guidance
                
                # Accumulate knowledge
                try:
                    # Infer knowledge base path from api_logger if available in components
                    if "api_logger" in components:
                        log_dir = Path(components["api_logger"].log_dir)
                        kb_path = log_dir.parent / "knowledge_base.txt"
                        
                        # Use the knowledge_base_rule from CEO decision (if provided)
                        kb_rule = decision.get('knowledge_base_rule', '').strip()
                        if kb_rule:
                            # Format the lesson as a general rule
                            lesson = f"\n- [RULE: {stage_name}] {kb_rule}"
                            
                            with open(kb_path, "a") as f:
                                f.write(lesson)
                            print_info(f"Added rule to knowledge base: {kb_path}", 2)
                except Exception as e:
                    print_warning(f"Failed to save to knowledge base: {e}")
    
    # Failed after max attempts
    print_error(f"Failed after {max_attempts} attempts")
    return None
