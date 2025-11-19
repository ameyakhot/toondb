"""
Configuration management for accuracy benchmarks.
Reads settings from .env file and provides configuration.
"""
import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Load .env file from project root
PROJECT_ROOT = Path(__file__).parent.parent.parent
ENV_FILE = PROJECT_ROOT / ".env"
load_dotenv(ENV_FILE)


class BenchmarkConfig:
    """Configuration for accuracy benchmarks"""
    
    # Groq API Configuration
    GROQ_API_KEY: Optional[str] = os.getenv("GROQ_API_KEY")
    # Default to llama-3.1-8b-instant (fast and reliable)
    # Alternatives: llama-3.3-70b-versatile, mixtral-8x7b-32768, gemma-7b-it
    GROQ_MODEL: str = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
    
    # Test Parameters
    TEMPERATURE: float = float(os.getenv("BENCHMARK_TEMPERATURE", "0.0"))
    MAX_TOKENS: Optional[int] = int(os.getenv("BENCHMARK_MAX_TOKENS", "1000")) if os.getenv("BENCHMARK_MAX_TOKENS") else None
    NUM_RUNS: int = int(os.getenv("BENCHMARK_NUM_RUNS", "1"))
    
    # Database Configuration (use existing test database connections)
    POSTGRES_CONN_STRING: str = os.getenv(
        "POSTGRES_CONN_STRING", 
        "postgresql://testuser:testpass@localhost:5433/testdb"
    )
    MYSQL_CONN_STRING: str = os.getenv(
        "MYSQL_CONN_STRING",
        "mysql://testuser:testpass@localhost:3307/testdb"
    )
    MONGO_CONN_STRING: str = os.getenv(
        "MONGO_CONN_STRING",
        "mongodb://localhost:27017"
    )
    MONGO_DATABASE: str = os.getenv("MONGO_DATABASE", "testdb")
    
    # Output Configuration
    OUTPUT_DIR: Path = PROJECT_ROOT / "benchmark_results"
    JSON_REPORT: str = "benchmark_results.json"
    MARKDOWN_REPORT: str = "benchmark_results.md"
    CSV_REPORT: str = "benchmark_results.csv"
    
    @classmethod
    def validate(cls) -> bool:
        """Validate that required configuration is present"""
        if not cls.GROQ_API_KEY:
            raise ValueError(
                "GROQ_API_KEY not found in .env file. "
                f"Please add it to {ENV_FILE}"
            )
        return True
    
    @classmethod
    def ensure_output_dir(cls):
        """Ensure output directory exists"""
        cls.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

