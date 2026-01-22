"""
API routes for channel operations
"""
from typing import List, Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session

from api.dependencies import get_db, get_redis
from api.services.data_service import DataService
from api.schemas import ChannelResponse, ChannelActivity, ChannelStats

router = APIRouter()


@router.get("/", response_model=List[ChannelResponse])
async def get_all_channels(
    channel_type: Optional[str] = Query(None, description="Filter by channel type"),
    min_posts: Optional[int] = Query(None, ge=0, description="Minimum number of posts"),
    active_only: bool = Query(False, description="Only return active channels"),
    db: Session = Depends(get_db),
    redis_client = Depends(get_redis)
):
    """
    Get all Telegram channels with their statistics
    
    Filters available:
    - channel_type: Pharmaceutical, Cosmetics, Medical, Other
    - min_posts: Minimum number of posts
    - active_only: Only channels active in last 7 days
    """
    try:
        data_service = DataService(db, redis_client)
        channels = await data_service.get_all_channels()
        
        # Apply filters
        filtered_channels = channels
        
        if channel_type:
            filtered_channels = [c for c in filtered_channels if c.channel_type == channel_type]
        
        if min_posts:
            filtered_channels = [c for c in filtered_channels if c.total_posts >= min_posts]
        
        if active_only:
            filtered_channels = [c for c in filtered_channels if c.activity_status == "Active"]
        
        return filtered_channels
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{channel_name}", response_model=ChannelResponse)
async def get_channel(
    channel_name: str,
    db: Session = Depends(get_db),
    redis_client = Depends(get_redis)
):
    """
    Get detailed information about a specific channel
    """
    try:
        data_service = DataService(db, redis_client)
        channel = await data_service.get_channel_by_name(channel_name)
        
        if not channel:
            raise HTTPException(status_code=404, detail=f"Channel '{channel_name}' not found")
        
        return channel
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{channel_name}/activity", response_model=List[ChannelActivity])
async def get_channel_activity(
    channel_name: str,
    days: int = Query(30, ge=1, le=365, description="Number of days to analyze"),
    db: Session = Depends(get_db),
    redis_client = Depends(get_redis)
):
    """
    Get posting activity and trends for a specific channel
    
    Returns daily statistics including:
    - Number of messages posted
    - Average views per message
    - Average forwards per message
    """
    try:
        data_service = DataService(db, redis_client)
        activity = await data_service.get_channel_activity(channel_name, days)
        
        if not activity:
            # Check if channel exists
            channel = await data_service.get_channel_by_name(channel_name)
            if not channel:
                raise HTTPException(status_code=404, detail=f"Channel '{channel_name}' not found")
        
        return activity
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{channel_name}/stats", response_model=ChannelStats)
async def get_channel_stats(
    channel_name: str,
    days: int = Query(30, ge=1, le=365, description="Number of days to analyze"),
    db: Session = Depends(get_db),
    redis_client = Depends(get_redis)
):
    """
    Get comprehensive statistics for a channel
    
    Includes:
    - Channel information
    - Activity trends
    - Top products mentioned
    - Image usage statistics
    """
    try:
        data_service = DataService(db, redis_client)
        
        # Get channel info
        channel = await data_service.get_channel_by_name(channel_name)
        if not channel:
            raise HTTPException(status_code=404, detail=f"Channel '{channel_name}' not found")
        
        # Get activity
        activity = await data_service.get_channel_activity(channel_name, days)
        
        # Get top products (simplified)
        all_products = await data_service.get_top_products(limit=20, days=days)
        channel_products = [
            p.product_name for p in all_products 
            if channel_name in p.channels
        ][:5]
        
        # Get image count
        query = """
            SELECT COUNT(*) as image_count
            FROM marts.fct_image_detections fid
            JOIN marts.dim_channels dc ON fid.channel_key = dc.channel_key
            WHERE dc.channel_name = :channel_name
        """
        
        result = db.execute(query, {"channel_name": channel_name}).fetchone()
        total_images = result[0] or 0
        
        # Get total messages
        total_messages = channel.total_posts
        
        return ChannelStats(
            channel=channel,
            activity=activity,
            total_messages=total_messages,
            total_images=total_images,
            top_products=channel_products
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{channel_name}/comparison")
async def compare_channel(
    channel_name: str,
    compare_with: str = Query(..., description="Channel name to compare with"),
    db: Session = Depends(get_db),
    redis_client = Depends(get_redis)
):
    """
    Compare two channels across various metrics
    """
    try:
        data_service = DataService(db, redis_client)
        
        # Get both channels
        channel1 = await data_service.get_channel_by_name(channel_name)
        channel2 = await data_service.get_channel_by_name(compare_with)
        
        if not channel1:
            raise HTTPException(status_code=404, detail=f"Channel '{channel_name}' not found")
        if not channel2:
            raise HTTPException(status_code=404, detail=f"Channel '{compare_with}' not found")
        
        # Get activity for both
        activity1 = await data_service.get_channel_activity(channel_name, 30)
        activity2 = await data_service.get_channel_activity(compare_with, 30)
        
        # Calculate metrics
        metrics = {
            "total_posts": {
                "channel1": channel1.total_posts,
                "channel2": channel2.total_posts,
                "difference": channel1.total_posts - channel2.total_posts,
                "percentage_difference": (
                    (channel1.total_posts - channel2.total_posts) / 
                    max(channel2.total_posts, 1) * 100
                )
            },
            "avg_views": {
                "channel1": channel1.avg_views,
                "channel2": channel2.avg_views,
                "difference": channel1.avg_views - channel2.avg_views,
                "percentage_difference": (
                    (channel1.avg_views - channel2.avg_views) / 
                    max(channel2.avg_views, 1) * 100
                )
            },
            "image_percentage": {
                "channel1": channel1.image_percentage,
                "channel2": channel2.image_percentage,
                "difference": channel1.image_percentage - channel2.image_percentage
            }
        }
        
        return {
            "channel1": channel_name,
            "channel2": compare_with,
            "metrics": metrics,
            "activity_period": "Last 30 days"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))