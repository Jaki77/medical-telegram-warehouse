"""
API routes for reports and analytics
"""
from typing import Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session

from api.dependencies import get_db, get_redis
from api.services.data_service import DataService
from api.schemas import (
    TopProductsResponse, VisualContentStats, 
    EngagementMetrics, PaginationParams
)

router = APIRouter()


@router.get("/top-products", response_model=TopProductsResponse)
async def get_top_products(
    limit: int = Query(10, ge=1, le=100, description="Number of top products to return"),
    days: int = Query(30, ge=1, le=365, description="Number of days to analyze"),
    db: Session = Depends(get_db),
    redis_client = Depends(get_redis)
):
    """
    Get top mentioned medical products across all channels
    
    Returns the most frequently mentioned products with their mention counts,
    channels where they appear, and average engagement metrics.
    """
    try:
        data_service = DataService(db, redis_client)
        products = await data_service.get_top_products(limit=limit, days=days)
        
        total_mentions = sum(p.mention_count for p in products)
        
        return TopProductsResponse(
            products=products,
            total_mentions=total_mentions,
            time_period={
                "days_analyzed": days,
                "description": f"Last {days} days"
            }
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/visual-content", response_model=VisualContentStats)
async def get_visual_content_stats(
    db: Session = Depends(get_db),
    redis_client = Depends(get_redis)
):
    """
    Get statistics about image usage across channels
    
    Provides insights into:
    - Total number of images analyzed
    - Distribution of images across channels
    - Image categories (promotional, product_display, etc.)
    - Most commonly detected objects
    - Average detections per image
    """
    try:
        data_service = DataService(db, redis_client)
        stats = await data_service.get_visual_content_stats()
        return stats
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/engagement", response_model=EngagementMetrics)
async def get_engagement_metrics(
    days: int = Query(7, ge=1, le=90, description="Number of days to analyze"),
    db: Session = Depends(get_db),
    redis_client = Depends(get_redis)
):
    """
    Get engagement metrics for messages
    
    Provides:
    - Total messages, views, and forwards
    - Average views and forwards per message
    - Top performing messages by views
    """
    try:
        data_service = DataService(db, redis_client)
        metrics = await data_service.get_engagement_metrics(days=days)
        return metrics
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trends/daily")
async def get_daily_trends(
    metric: str = Query("messages", description="Metric to analyze: messages, views, forwards, images"),
    days: int = Query(30, ge=1, le=365, description="Number of days to analyze"),
    db: Session = Depends(get_db),
    redis_client = Depends(get_redis)
):
    """
    Get daily trends for specified metric
    
    Available metrics:
    - messages: Daily message count
    - views: Daily total views
    - forwards: Daily total forwards
    - images: Daily image count
    """
    try:
        data_service = DataService(db, redis_client)
        
        # Build query based on metric
        if metric == "messages":
            query = """
                SELECT 
                    dd.full_date,
                    COUNT(fm.message_key) as value
                FROM marts.fct_messages fm
                JOIN marts.dim_dates dd ON fm.date_key = dd.date_key
                WHERE dd.full_date >= CURRENT_DATE - INTERVAL ':days days'
                GROUP BY dd.full_date
                ORDER BY dd.full_date
            """
        elif metric == "views":
            query = """
                SELECT 
                    dd.full_date,
                    SUM(fm.view_count) as value
                FROM marts.fct_messages fm
                JOIN marts.dim_dates dd ON fm.date_key = dd.date_key
                WHERE dd.full_date >= CURRENT_DATE - INTERVAL ':days days'
                GROUP BY dd.full_date
                ORDER BY dd.full_date
            """
        elif metric == "forwards":
            query = """
                SELECT 
                    dd.full_date,
                    SUM(fm.forward_count) as value
                FROM marts.fct_messages fm
                JOIN marts.dim_dates dd ON fm.date_key = dd.date_key
                WHERE dd.full_date >= CURRENT_DATE - INTERVAL ':days days'
                GROUP BY dd.full_date
                ORDER BY dd.full_date
            """
        elif metric == "images":
            query = """
                SELECT 
                    dd.full_date,
                    COUNT(fid.detection_key) as value
                FROM marts.fct_image_detections fid
                JOIN marts.dim_dates dd ON fid.date_key = dd.date_key
                WHERE dd.full_date >= CURRENT_DATE - INTERVAL ':days days'
                GROUP BY dd.full_date
                ORDER BY dd.full_date
            """
        else:
            raise HTTPException(status_code=400, detail="Invalid metric")
        
        result = db.execute(query, {"days": days}).fetchall()
        
        return {
            "metric": metric,
            "days": days,
            "data": [
                {"date": str(row[0]), "value": row[1] or 0}
                for row in result
            ]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))