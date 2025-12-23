"""
Configuration management for Cognix V2.
Loads environment variables and provides typed configuration.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from pydantic import BaseModel

# Load environment variables
load_dotenv()


class Settings(BaseModel):
    """Application settings with validation."""
    
    # OpenAI
    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")
    openai_model: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    
    # Server
    host: str = os.getenv("HOST", "0.0.0.0")
    port: int = int(os.getenv("PORT", "8000"))
    debug: bool = os.getenv("DEBUG", "false").lower() == "true"
    
    # Data
    data_path: Path = Path(os.getenv("DATA_PATH", "data/raw.parquet"))
    artifacts_dir: Path = Path(os.getenv("ARTIFACTS_DIR", "artifacts"))
    viz_specs_dir: Path = Path(os.getenv("VIZ_SPECS_DIR", "viz"))
    
    # Schema definition for the analytics engine
    available_metrics: list[str] = ["sales", "profit", "quantity"]
    available_dimensions: list[str] = ["region", "category", "sub_category"]
    available_time_grains: list[str] = ["day", "week", "month", "quarter", "year"]
    
    def validate_openai_key(self) -> bool:
        """Check if OpenAI API key is configured."""
        return bool(self.openai_api_key and self.openai_api_key.startswith("sk-"))


# Global settings instance
settings = Settings()

