import os
import subprocess
from typing import Optional, Dict, Any

from fastapi import FastAPI, HTTPException, status, Depends, APIRouter, Security
from fastapi.security import APIKeyHeader
from loguru import logger # Use Loguru logger

from api.models import DbtRunRequest, DbtCommandResponse, HealthResponse, Settings

# Instantiate settings globally
try:
    settings = Settings()
    logger.info("Settings loaded successfully.")
    logger.info(f"DBT_PROJECT_DIR: {settings.DBT_PROJECT_DIR}")
    logger.info(f"DBT_PROFILES_DIR: {settings.DBT_PROFILES_DIR}")
    logger.info(f"API_KEY: {'*' * (len(settings.API_KEY) - 4) + settings.API_KEY[-4:] if len(settings.API_KEY) > 4 else '****'}")
except Exception as e:
    logger.critical(
        f"FATAL ERROR: Could not load settings. Check .env file and environment variables. Error: {e}"
    )
    raise

# --- API Key Security Definition ---
API_KEY_NAME = "X-API-Key" # Standard header name
api_key_header_auth = APIKeyHeader(
    name=API_KEY_NAME, auto_error=True
)

async def verify_api_key(api_key_header: str = Security(api_key_header_auth)):
    """Dependency function to verify the API key provided in the header."""
    if api_key_header == settings.API_KEY:
        return api_key_header # Or return True, value not typically used
    else:
        logger.warning(f"Unauthorized API key received: {api_key_header[:5]}...") # Log partial key
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API Key",
        )

app = FastAPI(
    title="DBT Runner API",
    description="API to trigger dbt commands remotely, with API key auth and versioning.",
    version="1.0.0",
)

# --- API Router for v1 ---
router_v1 = APIRouter(
    prefix="/v1",
    tags=["DBT Execution v1"],
    dependencies=[Depends(verify_api_key)]
)

# --- Helper Function to Run DBT Commands (Uses Loguru & Settings) ---
def run_dbt_command(command_args: list[str]) -> Dict[str, Any]:
    """Runs dbt command, uses Loguru, reads paths from settings."""
    # Settings are already loaded and checked at startup
    cmd = ['dbt'] + command_args + ['--project-dir', settings.DBT_PROJECT_DIR, '--profiles-dir', settings.DBT_PROFILES_DIR]
    command_str = ' '.join(cmd)
    logger.info(f"Running dbt command: {command_str}")

    try:
        process = subprocess.run(
            cmd, capture_output=True, text=True, check=True, cwd=settings.DBT_PROJECT_DIR
        )
        stdout = process.stdout
        stderr = process.stderr
        logger.info(f"dbt command successful. stdout snippet: {(stdout or '')[:200]}...") # Log snippet
        if stderr:
            logger.warning(f"dbt command stderr snippet: {(stderr or '')[:200]}...")
        return {"status": "success", "command": command_str, "stdout": stdout, "stderr": stderr}
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt command failed with exit code {e.returncode}. stderr:\n{e.stderr}\nstdout:\n{e.stdout}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"message": "dbt command failed.", "command": command_str, "stdout": e.stdout, "stderr": e.stderr, "returncode": e.returncode}
        )
    except FileNotFoundError:
        logger.error("Error: 'dbt' command not found. Is dbt installed in the image PATH?")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="'dbt' command not found in container.")
    except Exception as e:
        logger.exception("An unexpected error occurred during dbt execution.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected server error occurred: {str(e)}")

# API Endpoints
@app.get(
    "/", # Define path for the root URL
    tags=["Root"],
    summary="API Root / Welcome Message",
    response_model=Dict[str, Any] # Define a basic response structure
)
async def read_root():
    """Provides a welcome message and links to key API sections."""
    logger.info("Root endpoint '/' accessed.")
    return {
        "message": "Welcome to the DBT Runner API!",
        "api_version": "v1", # Indicate the current version path
        "documentation": "/docs", # Link to FastAPI auto-docs
        "health_check": "/health" # Link to health endpoint
    }

# Health check - Placed on root app, no authentication needed
@app.get("/health", response_model=HealthResponse, tags=["Health"], summary="Check API health")
async def health_check():
    """Basic health check endpoint. Does not require API key."""
    return {"status": "ok"}

# DBT commands - Placed on the v1 router, requires authentication via dependency
@router_v1.post("/run", response_model=DbtCommandResponse, summary="Trigger dbt run")
async def dbt_run(request_body: Optional[DbtRunRequest] = None) -> Dict[str, Any]:
    """Triggers 'dbt run'. Requires valid X-API-Key header."""
    command_args = ['run']
    if request_body and request_body.models:
        command_args.extend(['--select'] + request_body.models)
        logger.info(f"Running dbt run with models/selectors: {request_body.models}")
    result = run_dbt_command(command_args)
    return result

@router_v1.post("/test", response_model=DbtCommandResponse, summary="Trigger dbt test")
async def dbt_test(request_body: Optional[DbtRunRequest] = None) -> Dict[str, Any]:
    """Triggers 'dbt test'. Requires valid X-API-Key header."""
    command_args = ['test']
    if request_body and request_body.models:
        command_args.extend(['--select'] + request_body.models)
        logger.info(f"Running dbt test with models/selectors: {request_body.models}")
    result = run_dbt_command(command_args)
    return result

# Include the versioned router in the main application
if __name__ == "__main__":
	app.include_router(router_v1)

# --- Loguru Configuration (Optional: Add custom formatting, sinks etc.) ---
# Example: Configure logger to output JSON format if needed for log aggregation
# logger.add("file.log", rotation="500 MB") # Log to a file
# logger.add(sys.stderr, format="{time} {level} {message}", level="INFO") # Customize console output
# logger.configure(handlers=[{"sink": sys.stderr, "serialize": True}]) # JSON logs to stderr

# --- (Uvicorn runs the 'app' instance via docker-compose command) ---