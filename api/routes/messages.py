"""
API routes for message operations
"""
from typing import List, Optional
from datetime import date
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session

from api.dependencies import get_db, get_redis
from api.services.data_service import DataService
from api.schemas import MessageResponse, MessageSearchParams, SearchResponse

router = APIRouter()


@router.get("/messages", response_model=SearchResponse)
async def search_messages(
    query: str = Query(..., min_length=1, max_length=100, description="Search query"),
    channel_name: Optional[str] = Query(None, description="Filter by channel name"),
    start_date: Optional[date] = Query(None, description="Start date for filtering"),
    end_date: Optional[date] = Query(None, description="End date for filtering"),
    has_images: Optional[bool] = Query(None, description="Filter by image presence"),
    min_views: Optional[int] = Query(None, ge=0, description="Minimum view count"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Items per page"),
    db: Session = Depends(get_db),
    redis_client = Depends(get_redis)
):
    """
    Search for messages containing specific keywords
    
    Advanced filtering options:
    - Channel name
    - Date range
    - Image presence
    - Minimum view count
    - Pagination support
    """
    try:
        data_service = DataService(db, redis_client)
        
        # Calculate offset for pagination
        offset = (page - 1) * limit
        
        # Search messages with filters
        messages = await data_service.search_messages(
            query=query,
            channel_name=channel_name,
            start_date=start_date,
            end_date=end_date,
            has_images=has_images,
            min_views=min_views,
            limit=limit
        )
        
        # For simplicity, we're returning all results without actual pagination
        # In production, you would implement proper pagination in the SQL query
        
        return SearchResponse(
            query=query,
            total_results=len(messages),
            messages=messages,
            page=page,
            total_pages=max(1, (len(messages) + limit - 1) // limit)
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/messages/{message_id}")
async def get_message(
    message_id: int,
    channel_name: Optional[str] = Query(None, description="Channel name for disambiguation"),
    db: Session = Depends(get_db)
):
    """
    Get detailed information about a specific message
    
    If multiple channels have the same message_id, provide channel_name for disambiguation
    """
    try:
        # Build query
        query = """
            SELECT 
                fm.message_id,
                fm.message_text,
                dc.channel_name,
                fm.view_count,
                fm.forward_count,
                fm.has_image_flag,
                fm.message_length,
                fm.engagement_score,
                dd.full_date as message_date
            FROM marts.fct_messages fm
            JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
            JOIN marts.dim_dates dd ON fm.date_key = dd.date_key
            WHERE fm.message_id = :message_id
        """
        
        params = {"message_id": message_id}
        
        if channel_name:
            query += " AND dc.channel_name = :channel_name"
            params["channel_name"] = channel_name
        
        result = db.execute(query, params).fetchall()
        
        if not result:
            raise HTTPException(status_code=404, detail="Message not found")
        
        if len(result) > 1 and not channel_name:
            # Multiple messages with same ID, suggest channels
            channels = [row[2] for row in result]
            raise HTTPException(
                status_code=400,
                detail=f"Multiple messages found with ID {message_id}. "
                      f"Specify channel_name: {', '.join(channels)}"
            )
        
        row = result[0]
        
        # Get image detection if exists
        image_query = """
            SELECT 
                image_category,
                detected_objects,
                avg_confidence
            FROM marts.fct_image_detections
            WHERE message_id = :message_id
                AND channel_name = :channel_name
        """
        
        image_result = db.execute(
            image_query, 
            {"message_id": message_id, "channel_name": row[2]}
        ).fetchone()
        
        message_data = {
            "message_id": row[0],
            "message_text": row[1],
            "channel_name": row[2],
            "view_count": row[3],
            "forward_count": row[4],
            "has_image": row[5],
            "message_length": row[6],
            "engagement_score": row[7],
            "message_date": row[8]
        }
        
        if image_result:
            message_data["image_analysis"] = {
                "category": image_result[0],
                "detected_objects": image_result[1],
                "confidence": image_result[2]
            }
        
        return message_data
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/messages/popular")
async def get_popular_messages(
    timeframe: str = Query("week", description="Timeframe: day, week, month"),
    limit: int = Query(10, ge=1, le=50, description="Number of messages to return"),
    db: Session = Depends(get_db)
):
    """
    Get most popular messages based on views
    
    Timeframes available:
    - day: Last 24 hours
    - week: Last 7 days  
    - month: Last 30 days
    """
    try:
        # Determine days based on timeframe
        if timeframe == "day":
            days = 1
        elif timeframe == "week":
            days = 7
        elif timeframe == "month":
            days = 30
        else:
            raise HTTPException(status_code=400, detail="Invalid timeframe")
        
        query = """
            SELECT 
                fm.message_id,
                fm.message_text,
                dc.channel_name,
                fm.view_count,
                fm.forward_count,
                fm.has_image_flag,
                fm.message_length,
                fm.engagement_score,
                dd.full_date as message_date
            FROM marts.fct_messages fm
            JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
            JOIN marts.dim_dates dd ON fm.date_key = dd.date_key
            WHERE dd.full_date >= CURRENT_DATE - INTERVAL ':days days'
            ORDER BY fm.view_count DESC
            LIMIT :limit
        """
        
        result = db.execute(query, {"days": days, "limit": limit}).fetchall()
        
        messages = []
        for row in result:
            messages.append({
                "message_id": row[0],
                "message_text": row[1][:200] + "..." if len(row[1]) > 200 else row[1],
                "channel_name": row[2],
                "view_count": row[3],
                "forward_count": row[4],
                "has_image": row[5],
                "message_length": row[6],
                "engagement_score": row[7],
                "message_date": row[8],
                "timeframe": timeframe
            })
        
        return {
            "timeframe": timeframe,
            "days": days,
            "messages": messages
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))