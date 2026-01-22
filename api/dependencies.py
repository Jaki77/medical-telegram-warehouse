"""
Dependencies for FastAPI application
"""
import os
from typing import Generator, Optional
from redis.asyncio import Redis

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from fastapi import Depends, HTTPException, Header

from dotenv import load_dotenv

load_dotenv('config/.env')

# Database configuration
DATABASE_URL = (
    f"postgresql://{os.getenv('DB_USER', 'postgres')}:"
    f"{os.getenv('DB_PASSWORD', 'postgres123')}@"
    f"{os.getenv('DB_HOST', 'localhost')}:"
    f"{os.getenv('DB_PORT', '5432')}/"
    f"{os.getenv('DB_NAME', 'medical_warehouse')}"
)

# Redis configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Create database engine
engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=300)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Redis client (optional)
redis_client: Optional[Redis] = None


def get_db() -> Generator[Session, None, None]:
    """
    Dependency to get database session
    """
    db = SessionLocal()
    try:
        yield db
    except SQLAlchemyError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        db.close()


def get_redis() -> Optional[Redis]:
    """
    Dependency to get Redis client
    """
    global redis_client
    
    if not REDIS_URL:
        return None
    
    if redis_client is None:
        try:
            redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
        except Exception as e:
            print(f"Redis connection failed: {e}")
            return None
    
    return redis_client


async def validate_api_key(x_api_key: Optional[str] = Header(None)):
    """
    Validate API key from header
    In production, use proper authentication
    """
    # For development, accept any key or none
    if os.getenv("ENVIRONMENT") == "production":
        valid_keys = os.getenv("API_KEYS", "").split(",")
        if not x_api_key or x_api_key not in valid_keys:
            raise HTTPException(
                status_code=401,
                detail="Invalid or missing API key"
            )
    # In development, allow all requests
    return True