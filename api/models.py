from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from typing import Optional, Dict, Any

class Settings(BaseSettings):
    """Loads settings from environment variables / .env file."""
    DBT_PROJECT_DIR: str
    DBT_PROFILES_DIR: str
    API_KEY: str

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'

class DbtRunRequest(BaseModel):
    models: Optional[list[str]] = Field(
        None, description="Optional list of models/selectors for dbt run/test."
    )

class DbtCommandResponse(BaseModel):
    status: str = "success"
    command: str
    stdout: str
    stderr: str

class HealthResponse(BaseModel):
    status: str = "ok"


if __name__ == "__main__":
    None