"""
Additional search and analytics routes
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session

from api.dependencies import get_db

router = APIRouter()


@router.get("/analytics/overview")
async def get_analytics_overview(
    days: int = Query(30, ge=1, le=365, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """
    Get comprehensive analytics overview
    
    Includes:
    - Message statistics
    - Channel statistics  
    - Engagement metrics
    - Visual content insights
    """
    try:
        # Message statistics
        msg_query = """
            SELECT 
                COUNT(*) as total_messages,
                COUNT(DISTINCT channel_key) as active_channels,
                SUM(view_count) as total_views,
                SUM(forward_count) as total_forwards,
                AVG(view_count) as avg_views,
                AVG(forward_count) as avg_forwards
            FROM marts.fct_messages fm
            JOIN marts.dim_dates dd ON fm.date_key = dd.date_key
            WHERE dd.full_date >= CURRENT_DATE - INTERVAL ':days days'
        """
        
        msg_result = db.execute(msg_query, {"days": days}).fetchone()
        
        # Channel statistics
        channel_query = """
            SELECT 
                COUNT(*) as total_channels,
                COUNT(DISTINCT channel_type) as channel_types,
                SUM(total_posts) as total_posts_all_channels,
                AVG(image_percentage) as avg_image_percentage
            FROM marts.dim_channels
        """
        
        channel_result = db.execute(channel_query).fetchone()
        
        # Visual content
        visual_query = """
            SELECT 
                COUNT(*) as total_images,
                AVG(detection_count) as avg_detections,
                COUNT(DISTINCT channel_key) as channels_with_images
            FROM marts.fct_image_detections fid
            JOIN marts.dim_dates dd ON fid.date_key = dd.date_key
            WHERE dd.full_date >= CURRENT_DATE - INTERVAL ':days days'
        """
        
        visual_result = db.execute(visual_query, {"days": days}).fetchone()
        
        # Top channels
        top_channels_query = """
            SELECT 
                dc.channel_name,
                dc.channel_type,
                COUNT(fm.message_key) as message_count,
                SUM(fm.view_count) as total_views
            FROM marts.fct_messages fm
            JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
            JOIN marts.dim_dates dd ON fm.date_key = dd.date_key
            WHERE dd.full_date >= CURRENT_DATE - INTERVAL ':days days'
            GROUP BY dc.channel_name, dc.channel_type
            ORDER BY total_views DESC
            LIMIT 5
        """
        
        top_channels = db.execute(top_channels_query, {"days": days}).fetchall()
        
        return {
            "time_period": f"Last {days} days",
            "message_statistics": {
                "total_messages": msg_result[0] or 0,
                "active_channels": msg_result[1] or 0,
                "total_views": msg_result[2] or 0,
                "total_forwards": msg_result[3] or 0,
                "avg_views_per_message": msg_result[4] or 0,
                "avg_forwards_per_message": msg_result[5] or 0
            },
            "channel_statistics": {
                "total_channels": channel_result[0] or 0,
                "channel_types": channel_result[1] or 0,
                "total_posts": channel_result[2] or 0,
                "avg_image_percentage": channel_result[3] or 0
            },
            "visual_content": {
                "total_images": visual_result[0] or 0,
                "avg_detections_per_image": visual_result[1] or 0,
                "channels_with_images": visual_result[2] or 0
            },
            "top_channels": [
                {
                    "channel_name": row[0],
                    "channel_type": row[1],
                    "message_count": row[2],
                    "total_views": row[3]
                }
                for row in top_channels
            ]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))