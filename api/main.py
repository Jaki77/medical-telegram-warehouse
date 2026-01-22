#!/usr/bin/env python3
"""
Main FastAPI application for Medical Telegram Analytics API
"""
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi

from api.dependencies import get_db, get_redis
from api.routes import channels, messages, reports, search
from api.utils.validators import validate_api_key
from api.services.data_service import initialize_cache

import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/api.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan manager for startup/shutdown events
    """
    # Startup
    logger.info("Starting Medical Telegram Analytics API")
    
    # Initialize cache
    try:
        redis_client = get_redis()
        if redis_client:
            await initialize_cache(redis_client)
            logger.info("Cache initialized successfully")
    except Exception as e:
        logger.warning(f"Cache initialization failed: {e}")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Medical Telegram Analytics API")


# Create FastAPI app
app = FastAPI(
    title="Medical Telegram Analytics API",
    description="API for analyzing Ethiopian medical businesses from Telegram data",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict to specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Custom OpenAPI schema
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    
    # Add security scheme
    openapi_schema["components"]["securitySchemes"] = {
        "APIKeyHeader": {
            "type": "apiKey",
            "in": "header",
            "name": "X-API-Key",
            "description": "API key for authentication"
        }
    }
    
    # Add security requirements
    openapi_schema["security"] = [{"APIKeyHeader": []}]
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

# Include routers
app.include_router(
    reports.router,
    prefix="/api/reports",
    tags=["reports"],
    dependencies=[Depends(validate_api_key)]
)

app.include_router(
    channels.router,
    prefix="/api/channels",
    tags=["channels"],
    dependencies=[Depends(validate_api_key)]
)

app.include_router(
    messages.router,
    prefix="/api/search",
    tags=["search"],
    dependencies=[Depends(validate_api_key)]
)

app.include_router(
    search.router,
    prefix="/api",
    tags=["search"],
    dependencies=[Depends(validate_api_key)]
)


# Health check endpoint
@app.get("/health", tags=["health"])
async def health_check(db=Depends(get_db)):
    """
    Health check endpoint to verify API and database status
    """
    try:
        # Check database connection
        db.execute("SELECT 1")
        
        # Check Redis if available
        redis_client = get_redis()
        if redis_client:
            await redis_client.ping()
        
        return {
            "status": "healthy",
            "database": "connected",
            "cache": "available" if redis_client else "not_configured",
            "timestamp": "2026-01-19T10:30:00Z"  # In production, use datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unavailable")


# Root endpoint
@app.get("/", tags=["root"])
async def root():
    """
    Root endpoint with API information
    """
    return {
        "name": "Medical Telegram Analytics API",
        "version": "1.0.0",
        "description": "API for analyzing Ethiopian medical businesses from Telegram data",
        "documentation": "/docs",
        "health_check": "/health",
        "endpoints": {
            "reports": "/api/reports",
            "channels": "/api/channels",
            "search": "/api/search",
            "analytics": "/api/analytics"
        }
    }


# Custom error handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "path": request.url.path
        }
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": str(exc),
            "path": request.url.path
        }
    )


if __name__ == "__main__":
    import uvicorn
    
    # Get port from environment or default to 8000
    port = int(os.getenv("API_PORT", 8000))
    host = os.getenv("API_HOST", "0.0.0.0")
    
    logger.info(f"Starting server on {host}:{port}")
    uvicorn.run(
        "api.main:app",
        host=host,
        port=port,
        reload=True,  # Auto-reload in development
        log_level="info"
    )