"""
Configuration management for Cognix V2.
Loads environment variables and provides typed configuration.
Supports both local .env files and Streamlit Cloud secrets.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from pydantic import BaseModel

# Load environment variables
load_dotenv()


def get_secret(key: str, default: str = "") -> str:
    """Get secret from Streamlit Cloud or environment variable."""
    # Try Streamlit secrets first (for cloud deployment)
    try:
        import streamlit as st
        if hasattr(st, 'secrets') and key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    # Fall back to environment variable
    return os.getenv(key, default)


class Settings(BaseModel):
    """Application settings with validation."""
    
    # OpenAI
    openai_api_key: str = get_secret("OPENAI_API_KEY", "")
    openai_model: str = get_secret("OPENAI_MODEL", "gpt-4o-mini")
    
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

