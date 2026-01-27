"""FastAPI service for Spark to BigQuery SQL conversion."""

import asyncio
import json
import logging
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from src.agent.graph import run_conversion
from src.schemas.models import ConvertRequest, ConvertResponse, ConversionHistory
from src.services.log_stream import setup_log_streaming, subscribe_logs, get_recent_logs, init_log_loop
from src.services.usage_logger import UsageLogger


load_dotenv()

# Configure logging
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

setup_log_streaming()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    # Initialize thread-safe logging
    init_log_loop()
    from src.services.log_stream import _loop
    logger.info(f"Log loop initialized: {_loop}")
    
    # Startup: Validate required environment variables
    # We strictly use Vertex AI (gemini) and Dry Run mode.
    
    required_vars = [
        "GOOGLE_CLOUD_PROJECT",
        "GOOGLE_CLOUD_LOCATION"
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.warning(f"Missing environment variables: {', '.join(missing_vars)}")
        logger.warning("The service may not function correctly without these variables.")
    
    # Initialize Usage Logger (will check/create table)
    try:
        UsageLogger()
    except Exception as e:
        logger.warning(f"Failed to initialize UsageLogger: {e}")

    logger.info("=" * 60)
    logger.info("Spark to BigQuery SQL Converter - Starting up")
    logger.info(f"Log Level: {log_level}")
    logger.info("=" * 60)
    
    yield
    
    # Shutdown: Cleanup if needed
    logger.info("Shutting down...")


app = FastAPI(
    title="Spark to BigQuery SQL Converter",
    description="Convert Spark SQL to BigQuery SQL using LangGraph with configurable LLM (Gemini/OpenAI)",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UI_DIR = Path(__file__).parent.parent / "ui"
app.mount("/assets", StaticFiles(directory=UI_DIR), name="assets")


@app.get("/ui")
async def serve_ui():
    """Serve the frontend UI."""
    html_path = UI_DIR / "index.html"
    if html_path.exists():
        return FileResponse(html_path)
    raise HTTPException(status_code=404, detail="UI not found")


@app.get("/")
async def root():
    """Root endpoint returning service information."""
    return {
        "service": "Spark to BigQuery SQL Converter",
        "version": "1.0.0",
        "endpoints": {
            "/ui": "GET - Web UI for SQL conversion",
            "/convert": "POST - Convert Spark SQL to BigQuery SQL",
            "/health": "GET - Health check",
            "/config": "GET - Current feature configuration",
            "/logs/stream": "GET - SSE log stream",
            "/logs/recent": "GET - Recent logs",
        }
    }


@app.get("/config")
async def get_config():
    """Get current configuration."""
    return {
        "execute_enabled": os.getenv("EXECUTE_ENABLED", "true").lower() == "true",
        "data_verification_enabled": os.getenv("DATA_VERIFICATION_ENABLED", "true").lower() == "true"
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


@app.get("/logs/stream")
async def stream_logs():
    """Stream logs via Server-Sent Events (SSE).
    
    Frontend can connect to this endpoint to receive real-time log updates.
    """
    async def event_generator():
        async for log_entry in subscribe_logs():
            data = json.dumps(log_entry, ensure_ascii=False)
            yield f"data: {data}\n\n"
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
    )


@app.get("/logs/recent")
async def get_logs(count: int = 50):
    """Get recent logs from buffer.
    
    Args:
        count: Maximum number of logs to return (default 50).
    """
    return {"logs": get_recent_logs(count)}


@app.post("/convert", response_model=ConvertResponse)
async def convert_sql(request: ConvertRequest):
    """Convert Spark SQL to BigQuery SQL.
    
    This endpoint:
    1. Validates the input Spark SQL syntax
    2. Converts Spark SQL to BigQuery SQL
    3. Validates the BigQuery SQL (using dry_run)
    4. Iteratively fixes any errors (up to 3 retries)
    
    Args:
        request: ConvertRequest containing the Spark SQL to convert.
        
    Returns:
        ConvertResponse with conversion results and validation status.
    """
    logger.info("=" * 60)
    logger.info("[API] Received conversion request")
    logger.debug(f"[API] Input Spark SQL:\n{request.spark_sql}")
    
    try:
        # Run in thread to allow log streaming (SSE) to function during execution
        result = await asyncio.to_thread(run_conversion, request.spark_sql)
        
        # Build conversion history
        history = [
            ConversionHistory(
                attempt=entry.attempt,
                bigquery_sql=entry.bigquery_sql,
                error=entry.error,
            )
            for entry in result.get("conversion_history", [])
        ]
        
        # Determine success and warning
        success = result["spark_valid"] and result["validation_success"]
        warning = None
        
        if result["spark_valid"] and not result["validation_success"]:
            if result["retry_count"] >= result.get("max_retries", 3):
                warning = (
                    f"Maximum retries ({result.get('max_retries', 3)}) exceeded. "
                    "The converted SQL may still contain errors."
                )
        
        logger.info("=" * 60)
        logger.info("[API] Conversion completed")
        logger.info(f"[API] Success: {success}")
        logger.info(f"[API] Spark Valid: {result['spark_valid']}")
        logger.info(f"[API] Validation Success: {result['validation_success']}")
        logger.info(f"[API] Retry Count: {result['retry_count']}")
        if result.get("bigquery_sql"):
            logger.debug(f"[API] Final BigQuery SQL:\n{result['bigquery_sql']}")
        if result.get("validation_error"):
            logger.error(f"[API] Validation Error: {result['validation_error']}")
        if result.get("spark_error"):
            logger.error(f"[API] Spark Error: {result['spark_error']}")
        if warning:
            logger.warning(f"[API] Warning: {warning}")

        # Signal completion to frontend
        logger.info("[API] process finished", extra={"type": "status", "status": "completed"})
        
        return ConvertResponse(
            success=success,
            spark_sql=result["spark_sql"],
            spark_valid=result["spark_valid"],
            spark_error=result.get("spark_error"),
            bigquery_sql=result.get("bigquery_sql"),
            validation_success=result["validation_success"],
            validation_error=result.get("validation_error"),
            validation_mode=result.get("validation_mode"),
            llm_check_success=result.get("llm_check_success"),
            llm_check_error=result.get("llm_check_error"),
            retry_count=result["retry_count"],
            conversion_history=history,
            warning=warning,
            execution_success=result.get("execution_success"),
            execution_target_table=result.get("execution_target_table"),
            execution_error=result.get("execution_error"),
            data_verification_success=result.get("data_verification_success"),
            data_verification_result=result.get("data_verification_result"),
            data_verification_error=result.get("data_verification_error"),
        )
        
    except ValueError as e:
        logger.error(f"[API] ValueError: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"[API] Internal error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


if __name__ == "__main__":
    import signal
    import uvicorn
    
    # Handle SIGINT for graceful shutdown
    def handle_sigint(sig, frame):
        logger.info("Received SIGINT, shutting down...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, handle_sigint)
    
    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        timeout_graceful_shutdown=5,  # Force shutdown after 5 seconds
    )
