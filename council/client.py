"""
LLM Client with provider abstraction for DeepInfra and OpenRouter.
"""
import os
import time
import logging
from typing import Dict, Any, Optional
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)


class LLMClient:
    """Provider-agnostic LLM client using OpenAI SDK with config support."""
    
    def __init__(self, config=None, provider: str = None, api_logger=None):
        """
        Initialize LLM client.
        
        Args:
            config: Config object (optional, for loading provider settings and defaults)
            provider: Provider name (optional, overrides config)
            api_logger: Optional APILogger instance for tracking API calls
        """
        self.config = config
        self.api_logger = api_logger
        
        # Determine provider
        if provider:
            self.provider = provider.lower()
        elif config:
            self.provider = config.get("active_provider", "deepinfra")
        else:
            self.provider = "deepinfra"
        
        # Get provider configuration
        if config:
            provider_config = config.get_provider_config(self.provider)
            self.base_url = provider_config.get("base_url")
            api_key_env = provider_config.get("api_key_env")
            self.api_key = os.getenv(api_key_env)
        else:
            # Fallback to hardcoded values if no config
            if self.provider == "deepinfra":
                self.api_key = os.getenv("DEEPINFRA_API_KEY")
                self.base_url = "https://api.deepinfra.com/v1/openai"
            elif self.provider == "openrouter":
                self.api_key = os.getenv("OPENROUTER_API_KEY")
                self.base_url = "https://openrouter.ai/api/v1"
            else:
                raise ValueError(f"Unsupported provider: {self.provider}")
        
        if not self.api_key:
            api_key_env = f"{self.provider.upper()}_API_KEY"
            raise ValueError(f"API key not found for provider: {self.provider}. Please set {api_key_env} in .env file")
        
        # Initialize OpenAI client
        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url
        )
        
        logger.info(f"Initialized {self.provider} client")
    
    def generate(self, prompt: str, model: str, system_prompt: str = None, 
                 temperature: float = None, max_tokens: int = None,
                 top_p: float = None, frequency_penalty: float = None, 
                 presence_penalty: float = None, role: str = None, seed: int = None) -> str:
        """
        Generate text using specified model.
        
        Args:
            prompt: User input prompt
            model: Model identifier
            system_prompt: Optional system prompt to guide model behavior
            temperature: Optional temperature override (uses config default if not specified)
            max_tokens: Optional max_tokens override (uses config default if not specified)
            top_p: Optional top_p override (uses config default if not specified)
            frequency_penalty: Optional frequency_penalty override (uses config default if not specified)
            presence_penalty: Optional presence_penalty override (uses config default if not specified)
            role: Optional role identifier for logging purposes (e.g., 'ceo', 'generator')
            seed: Optional seed for deterministic generation
        
        Returns:
            Generated text
        """
        logger.info(f"Calling {self.provider} with model {model}")
        
        start_time = time.time()
        
        # Get defaults from config or use fallback values
        if self.config:
            defaults = self.config.get("model_defaults", {})
        else:
            defaults = {}
        
        # Retry logic parameters
        max_retries = 5
        base_delay = 10
        
        for attempt in range(max_retries):
            try:
                # Build messages array
                messages = []
                if system_prompt:
                    messages.append({"role": "system", "content": system_prompt})
                messages.append({"role": "user", "content": prompt})
                
                # Use provided values or defaults
                response = self.client.chat.completions.create(
                    model=model,
                    messages=messages,
                    temperature=temperature if temperature is not None else defaults.get("temperature", 0.7),
                    max_tokens=max_tokens if max_tokens is not None else defaults.get("max_tokens", 16000),
                    top_p=top_p if top_p is not None else defaults.get("top_p", 1.0),
                    frequency_penalty=frequency_penalty if frequency_penalty is not None else defaults.get("frequency_penalty", 0),
                    presence_penalty=presence_penalty if presence_penalty is not None else defaults.get("presence_penalty", 0),
                    seed=seed
                )
                
                generated_text = response.choices[0].message.content
                
                # Calculate duration
                duration = time.time() - start_time
                
                # Extract token usage if available
                input_tokens = None
                output_tokens = None
                if hasattr(response, 'usage') and response.usage:
                    input_tokens = response.usage.prompt_tokens
                    output_tokens = response.usage.completion_tokens
                
                # Log API call if logger is enabled
                if self.api_logger:
                    metadata = {"provider": self.provider}
                    if role:
                        metadata["role"] = role
                    
                    self.api_logger.log_api_call(
                        prompt=prompt,
                        response=generated_text,
                        model=model,
                        duration=duration,
                        system_prompt=system_prompt,
                        input_tokens=input_tokens,
                        output_tokens=output_tokens,
                        metadata=metadata
                    )
                
                logger.info(f"Successfully generated {len(generated_text)} characters in {duration:.2f}s")
                return generated_text
                
            except Exception as e:
                import random
                error_msg = str(e)
                # Check for rate limits or overloaded servers
                if attempt < max_retries - 1:
                    wait_time = (base_delay * (2 ** attempt)) + (random.random() * 2)
                    logger.warning(f"API Attempt {attempt+1}/{max_retries} failed: {error_msg}. Retrying in {wait_time:.1f}s...")
                    print(f"⚠️  Rate limit or error. Retrying in {wait_time:.1f}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"API request failed after {max_retries} attempts: {e}")
                    raise
