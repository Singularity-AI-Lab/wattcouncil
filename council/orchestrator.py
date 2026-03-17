"""
Orchestrator - Simplified 3-Stage Pipeline

Main features:
- Stage 1: Families (with embedded work regimes)
- Stage 2: Weather Profiles (ranges + hourly combined)
- Stage 3: Consumption (per family)
- Country/Season folder structure for checkpoints
- Partial success tracking
- Uses dedicated Editor for targeted JSON fixes
"""
import json
import logging
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Tuple, List
from council.generator import PrimaryGenerator
from council.auditors import CulturalAuditor, PhysicalAuditor
from council.ceo import CEO
from council.editor import Editor

logger = logging.getLogger(__name__)


class Orchestrator:
    """
    Orchestrates the 3-stage LLM council pipeline.
    
    Pipeline Flow:
    1. Stage 1: Families (with embedded work regimes)
    2. Stage 2: Weather Profiles (ranges + hourly combined, shared)
    3. Stage 3: Consumption (per family)
    """
    
    def __init__(
        self,
        generator: PrimaryGenerator,
        cultural_auditor: CulturalAuditor,
        physical_auditor: PhysicalAuditor,
        ceo: CEO,
        editor: Editor,
        max_retries: int = 3,
        enable_checkpoints: bool = True,
        checkpoint_dir: str = "outputs/checkpoints"
    ):
        """
        Initialize orchestrator.
        
        Args:
            generator: Primary content generator
            cultural_auditor: Cultural appropriateness auditor
            physical_auditor: Physical plausibility auditor
            ceo: Final decision maker
            editor: JSON editor for targeted fixes
            max_retries: Maximum regeneration attempts per stage
            enable_checkpoints: Whether to save/load checkpoints
            checkpoint_dir: Base directory for checkpoints
        """
        self.generator = generator
        self.cultural_auditor = cultural_auditor
        self.physical_auditor = physical_auditor
        self.ceo = ceo
        self.editor = editor
        self.max_retries = max_retries
        self.enable_checkpoints = enable_checkpoints
        self.checkpoint_dir = Path(checkpoint_dir)
        
        # Create run-level timestamp for debug outputs
        self.run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.debug_base_dir = Path(f"outputs/debug/run_{self.run_timestamp}")
        
        if self.enable_checkpoints:
            self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        
        # Create debug directory for this run
        self.debug_base_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"🔍 Debug outputs will be saved to: {self.debug_base_dir}")
    
    def _get_checkpoint_path(self, stage: str, country: str, season: str, family_id: str = None) -> Path:
        """
        Get checkpoint file path for a stage.
        
        Args:
            stage: Stage name (e.g., 'stage1_families', 'stage2', 'stage5')
            country: Country name
            season: Season name
            family_id: Optional family ID (e.g., 'family_001')
        
        Returns:
            Path to checkpoint file
        """
        # Sanitize names for filesystem
        country_clean = country.replace(" ", "_").replace("/", "-").upper()
        season_clean = season.replace(" ", "_").upper()
        
        # Create country/season directory structure
        checkpoint_dir = self.checkpoint_dir / country_clean / season_clean
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename
        if family_id:
            # Per-family checkpoint: stage2_family_001.json
            filename = f"{stage}_{family_id}.json"
        else:
            # Shared checkpoint: stage1_families.json, stage2_weather.json
            filename = f"{stage}.json"
        
        return checkpoint_dir / filename
    
    def _save_checkpoint(self, stage: str, country: str, season: str, 
                        output: str, metadata: Dict[str, Any], family_id: str = None) -> None:
        """Save stage output to checkpoint file."""
        if not self.enable_checkpoints:
            return
        
        checkpoint_path = self._get_checkpoint_path(stage, country, season, family_id)
        checkpoint_data = {
            "stage": stage,
            "country": country,
            "season": season,
            "family_id": family_id,
            "output": output,
            "metadata": metadata,
            "timestamp": datetime.now().isoformat()
        }
        
        with open(checkpoint_path, "w") as f:
            json.dump(checkpoint_data, f, indent=2)
        
        logger.info(f"💾 Checkpoint saved: {checkpoint_path}")
    
    def _load_checkpoint(self, stage: str, country: str, season: str, family_id: str = None) -> Tuple[str, Dict[str, Any]]:
        """Load stage output from checkpoint file if it exists."""
        if not self.enable_checkpoints:
            return None, None
        
        checkpoint_path = self._get_checkpoint_path(stage, country, season, family_id)
        
        if not checkpoint_path.exists():
            return None, None
        
        try:
            with open(checkpoint_path, "r") as f:
                checkpoint_data = json.load(f)
            
            logger.info(f"📂 Checkpoint loaded: {checkpoint_path}")
            return checkpoint_data["output"], checkpoint_data["metadata"]
        except Exception as e:
            logger.warning(f"Failed to load checkpoint {checkpoint_path}: {e}")
            return None, None
    
    def run_stage(self, stage: str, variables: Dict[str, Any], context: Dict[str, Any],
                  use_checkpoint: bool = True) -> Tuple[str, Dict[str, Any]]:
        """
        Run a single stage with council validation.
        
        Args:
            stage: Stage name
            variables: Variables to inject into prompts
            context: Context dict (country, season, etc.)
            use_checkpoint: Whether to use checkpointing for this stage
        
        Returns:
            Tuple of (generated_output, metadata)
        """
        logger.info(f"{'=' * 60}")
        logger.info(f"Running {stage}")
        logger.info(f"{'=' * 60}")
        
        # Try to load from checkpoint if enabled
        if use_checkpoint and self.enable_checkpoints:
            family_id = variables.get("family_id")
            checkpoint_output, checkpoint_meta = self._load_checkpoint(
                stage, context["country"], context["season"], family_id
            )
            if checkpoint_output:
                logger.info(f"✅ Using checkpoint for {stage}")
                return checkpoint_output, checkpoint_meta
        
        # Run generation with council validation
        for attempt in range(self.max_retries):
            logger.info(f"Attempt {attempt + 1}/{self.max_retries}")
            
            # Create debug output directory for this stage
            debug_dir = self.debug_base_dir / stage
            debug_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Step 1: Generate
            generated_output = self.generator.generate(stage, variables)
            
            # Save generated output
            gen_file = debug_dir / f"attempt_{attempt+1}_{timestamp}_generated.json"
            with open(gen_file, "w") as f:
                f.write(generated_output)
            logger.info(f"💾 Saved generated output: {gen_file}")
            
            # Step 2: Cultural audit
            cultural_audit = self.cultural_auditor.audit(stage, generated_output, context)
            
            # Save cultural audit
            cult_file = debug_dir / f"attempt_{attempt+1}_{timestamp}_cultural_audit.json"
            with open(cult_file, "w") as f:
                json.dump(cultural_audit, f, indent=2)
            logger.info(f"💾 Saved cultural audit: {cult_file}")
            
            # Step 3: Physical audit
            physical_audit = self.physical_auditor.audit(stage, generated_output, context)
            
            # Save physical audit
            phys_file = debug_dir / f"attempt_{attempt+1}_{timestamp}_physical_audit.json"
            with open(phys_file, "w") as f:
                json.dump(physical_audit, f, indent=2)
            logger.info(f"💾 Saved physical audit: {phys_file}")
            
            # Step 4: CEO decision
            decision = self.ceo.decide(stage, generated_output, cultural_audit, physical_audit)
            
            # Save CEO decision
            ceo_file = debug_dir / f"attempt_{attempt+1}_{timestamp}_ceo_decision.json"
            with open(ceo_file, "w") as f:
                json.dump(decision, f, indent=2)
            logger.info(f"💾 Saved CEO decision: {ceo_file}")
            
            # Step 5: Process decision
            if decision.get("decision") == "ACCEPT":
                logger.info(f"Output ACCEPTED for {stage}")
                
                # Save checkpoint
                if use_checkpoint and self.enable_checkpoints:
                    family_id = variables.get("family_id")
                    self._save_checkpoint(
                        stage, context["country"], context["season"],
                        generated_output,
                        {
                            "attempts": attempt + 1,
                            "cultural_audit": cultural_audit,
                            "physical_audit": physical_audit,
                            "decision": decision
                        },
                        family_id
                    )
                
                return generated_output, {
                    "attempts": attempt + 1,
                    "cultural_audit": cultural_audit,
                    "physical_audit": physical_audit,
                    "decision": decision
                }
            
            elif decision.get("decision") == "REGENERATE_PARTIAL":
                logger.warning(f"REGENERATE_PARTIAL for {stage}: {decision.get('reason')}")
                
                # Use targeted JSON editing instead of full regeneration
                logger.info("Using targeted JSON editing - passing to editor")
                fix_guidance = decision.get("regeneration_guidance", "Fix the issues mentioned in the audits")
                
                try:
                    generated_output = self.editor.edit(
                        original_json=generated_output,
                        fix_guidance=fix_guidance
                    )
                    logger.info("JSON edited successfully, re-validating...")
                    # Continue to next iteration to re-audit the edited output
                    continue
                except Exception as e:
                    logger.error(f"JSON editing failed: {e}, falling back to full regeneration")
                    # Fall back to full regeneration if editing fails
                    variables["regeneration_guidance"] = fix_guidance
            
            elif decision.get("decision") == "REGENERATE_FULL":
                logger.warning(f"REGENERATE_FULL for {stage}: {decision.get('reason')}")
                # For full regeneration, add guidance but don't pass previous output
                variables["regeneration_guidance"] = decision.get("regeneration_guidance", "")
            
            else:
                logger.error(f"Unknown CEO decision: {decision.get('decision')}")
                break
        
        # Max retries reached
        logger.error(f"Max retries reached for {stage}")
        raise RuntimeError(f"Failed to generate acceptable output for {stage} after {self.max_retries} attempts")
    
    def run_pipeline(self, country: str, season: str, num_families: int = 5, 
                     day_type: str = "weekday", resume_from_checkpoint: bool = False) -> Dict[str, Any]:
        """
        Run the complete 3-stage pipeline.
        
        Pipeline Architecture:
        - Stage 1: Families (with embedded work regimes)
        - Stage 2: Weather Profiles (ranges + hourly combined, shared)
        - Stage 3: Consumption (per family)
        
        Args:
            country: Country name
            season: Season name
            num_families: Number of families to generate (default: 5)
            day_type: Day type for energy consumption - "weekday" or "weekend"
            resume_from_checkpoint: Resume from checkpoints if available
        
        Returns:
            Dictionary with results for all families
        """
        logger.info(f"\n{'=' * 60}")
        logger.info(f"3-STAGE PIPELINE: {country} in {season}")
        logger.info(f"Generating {num_families} families, Day type: {day_type}")
        logger.info(f"Resume from checkpoint: {resume_from_checkpoint}")
        logger.info(f"{'=' * 60}\n")
        
        context = {"country": country, "season": season}
        results = {
            "families": [],
            "shared_weather": None
        }
        
        # ==================== STAGE 1: Families with Work Regimes ====================
        logger.info(f"\n🏠 STAGE 1: Generating {num_families} families (with work regimes)")
        
        stage1_vars = {"country": country, "num_families": num_families}
        stage1_output, stage1_meta = self.run_stage(
            "stage1_family",
            stage1_vars,
            context,
            use_checkpoint=resume_from_checkpoint
        )
        
        # Parse families (should already include work regime data)
        families_data = json.loads(stage1_output)
        if not isinstance(families_data, list):
            raise ValueError("Stage 1 must return an array of families")
        
        logger.info(f"✅ Stage 1 complete: {len(families_data)} families generated with embedded work regimes")
        
        # ==================== STAGE 2: Weather Profiles (Shared) ====================
        logger.info(f"\n🌤️  STAGE 2: Generating weather profiles (shared by all families)")
        
        # Generate complete weather profile (ranges + hourly) in a single call
        weather_vars = {
            "country": country, 
            "season": season,
            "year": context.get("year", 2024)
        }
        weather_output, weather_meta = self.run_stage(
            "stage2_weather",
            weather_vars,
            context,
            use_checkpoint=resume_from_checkpoint
        )
        
        # Parse and store weather profile
        weather_data = json.loads(weather_output)
        results["shared_weather"] = weather_data
        logger.info(f"✅ Stage 2 complete: Weather profile generated")

        
        # ==================== STAGE 3: Consumption (Per Family) ====================
        logger.info(f"\n⚡ STAGE 3: Generating consumption for each family")
        successful_families = 0
        failed_families = []
        
        for i, family in enumerate(families_data):
            family_id = family.get("household_id", f"family_{i+1:03d}")
            logger.info(f"\n{'#' * 60}")
            logger.info(f"Processing {family_id} ({i+1}/{len(families_data)})")
            logger.info(f"{'#' * 60}")
            
            family_result = {
                "family_id": family_id,
                "family_data": family,
                "consumption": None
            }
            
            try:
                # Extract embedded work regime
                work_regime = {
                    "household_work_regime": family.get("household_work_regime", {}),
                    "weekday_daytime_occupancy_level": family.get("weekday_daytime_occupancy_level", "Unknown"),
                    "members": family.get("members", [])
                }
                
                # Generate consumption
                consumption_vars = {
                    "country": country,
                    "season": season,
                    "day_type": day_type,
                    "family_id": family_id,
                    "family_data": json.dumps(family, separators=(',', ':')),
                    "work_regime": json.dumps(work_regime, separators=(',', ':')),
                    "weather_hourly": json.dumps(weather_data, separators=(',', ':'))
                }
                
                consumption_output, consumption_meta = self.run_stage(
                    "stage3_consumption",
                    consumption_vars,
                    context,
                    use_checkpoint=resume_from_checkpoint
                )
                
                family_result["consumption"] = consumption_output
                results["families"].append(family_result)
                successful_families += 1
                logger.info(f"✅ {family_id} completed successfully")
                
            except Exception as e:
                logger.error(f"❌ {family_id} failed: {e}")
                failed_families.append({"family_id": family_id, "error": str(e)})
                # Continue with next family instead of failing entire pipeline
        
        # ==================== Summary ====================
        logger.info(f"\n{'=' * 60}")
        logger.info(f"PIPELINE COMPLETE")
        logger.info(f"{'=' * 60}")
        logger.info(f"✅ Successful families: {successful_families}/{len(families_data)}")
        if failed_families:
            logger.warning(f"❌ Failed families: {len(failed_families)}")
            for failed in failed_families:
                logger.warning(f"  - {failed['family_id']}: {failed['error']}")
        logger.info(f"{'=' * 60}\n")
        
        results["summary"] = {
            "total_families": len(families_data),
            "successful": successful_families,
            "failed": len(failed_families),
            "failed_details": failed_families
        }
        
        return results
