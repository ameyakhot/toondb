"""
LLM client for Groq API integration.
Handles chat completions and token counting.
"""
from typing import Optional, Dict, Any
import time
from groq import Groq
from toonpy.core.token_counter import count_tokens

from tests.accuracy_benchmarks.config import BenchmarkConfig


class GroqLLMClient:
    """Client for interacting with Groq API"""
    
    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        """
        Initialize Groq client.
        
        Args:
            api_key: Groq API key (defaults to GROQ_API_KEY from config)
            model: Model name (defaults to GROQ_MODEL from config)
        """
        self.api_key = api_key or BenchmarkConfig.GROQ_API_KEY
        if not self.api_key:
            raise ValueError("Groq API key is required. Set GROQ_API_KEY in .env file.")
        
        self.model = model or BenchmarkConfig.GROQ_MODEL
        self.client = Groq(api_key=self.api_key)
    
    def chat(
        self, 
        prompt: str, 
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Send chat completion request to Groq.
        
        Args:
            prompt: The prompt to send
            model: Model to use (defaults to instance model)
            temperature: Temperature setting (defaults to config)
            max_tokens: Max tokens (defaults to config)
        
        Returns:
            Dict with keys: 'response', 'tokens_used', 'latency_seconds'
        """
        model = model or self.model
        temperature = temperature if temperature is not None else BenchmarkConfig.TEMPERATURE
        max_tokens = max_tokens or BenchmarkConfig.MAX_TOKENS
        
        start_time = time.time()
        
        try:
            response = self.client.chat.completions.create(
                model=model,
                messages=[
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=temperature,
                max_tokens=max_tokens,
            )
            
            latency = time.time() - start_time
            
            # Extract response content
            content = response.choices[0].message.content
            
            # Count tokens (approximate using tiktoken)
            # Groq API doesn't always return token counts, so we estimate
            tokens_used = count_tokens(prompt + content, model="gpt-4")  # Approximate
            
            return {
                'response': content,
                'tokens_used': tokens_used,
                'latency_seconds': latency,
                'model': model,
                'raw_response': response
            }
        except Exception as e:
            raise RuntimeError(f"Groq API error: {e}") from e
    
    def count_tokens(self, text: str) -> int:
        """
        Count tokens in text.
        
        Args:
            text: Text to count tokens for
        
        Returns:
            Number of tokens
        """
        return count_tokens(text, model="gpt-4")  # Approximate for Groq models

