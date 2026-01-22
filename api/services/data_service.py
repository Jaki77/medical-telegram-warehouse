"""
Data service for database operations
"""
import json
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime, date, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import text, func, desc, and_, or_
from redis.asyncio import Redis

from api.models import TelegramMessage, Channel, DateDimension, ImageDetection
from api.schemas import (
    MessageResponse, ChannelResponse, TopProduct, 
    VisualContentStats, ChannelActivity, EngagementMetrics
)

import logging

logger = logging.getLogger(__name__)


class DataService:
    """Service for database operations"""
    
    def __init__(self, db: Session, redis_client: Optional[Redis] = None):
        self.db = db
        self.redis = redis_client
        self.cache_ttl = 300  # 5 minutes cache TTL
    
    async def get_cached(self, key: str) -> Optional[Any]:
        """Get data from Redis cache"""
        if not self.redis:
            return None
        
        try:
            data = await self.redis.get(key)
            if data:
                return json.loads(data)
        except Exception as e:
            logger.warning(f"Cache get failed: {e}")
        return None
    
    async def set_cached(self, key: str, data: Any) -> bool:
        """Set data in Redis cache"""
        if not self.redis:
            return False
        
        try:
            await self.redis.setex(
                key, 
                self.cache_ttl, 
                json.dumps(data, default=str)
            )
            return True
        except Exception as e:
            logger.warning(f"Cache set failed: {e}")
            return False
    
    # Channel operations
    async def get_all_channels(self) -> List[ChannelResponse]:
        """Get all channels"""
        cache_key = "channels:all"
        cached = await self.get_cached(cache_key)
        if cached:
            return cached
        
        try:
            channels = self.db.query(Channel).order_by(desc(Channel.total_posts)).all()
            
            result = []
            for channel in channels:
                result.append(ChannelResponse(
                    channel_key=channel.channel_key,
                    channel_name=channel.channel_name,
                    channel_type=channel.channel_type,
                    first_post_date=channel.first_post_date,
                    last_post_date=channel.last_post_date,
                    total_posts=channel.total_posts,
                    avg_views=channel.avg_views,
                    image_percentage=channel.image_percentage,
                    activity_status=channel.activity_status
                ))
            
            await self.set_cached(cache_key, result)
            return result
            
        except Exception as e:
            logger.error(f"Error getting channels: {e}")
            raise
    
    async def get_channel_by_name(self, channel_name: str) -> Optional[ChannelResponse]:
        """Get channel by name"""
        cache_key = f"channel:{channel_name}"
        cached = await self.get_cached(cache_key)
        if cached:
            return cached
        
        try:
            channel = self.db.query(Channel).filter(
                Channel.channel_name == channel_name
            ).first()
            
            if not channel:
                return None
            
            result = ChannelResponse(
                channel_key=channel.channel_key,
                channel_name=channel.channel_name,
                channel_type=channel.channel_type,
                first_post_date=channel.first_post_date,
                last_post_date=channel.last_post_date,
                total_posts=channel.total_posts,
                avg_views=channel.avg_views,
                image_percentage=channel.image_percentage,
                activity_status=channel.activity_status
            )
            
            await self.set_cached(cache_key, result)
            return result
            
        except Exception as e:
            logger.error(f"Error getting channel {channel_name}: {e}")
            raise
    
    async def get_channel_activity(self, channel_name: str, days: int = 30) -> List[ChannelActivity]:
        """Get channel activity over time"""
        cache_key = f"channel_activity:{channel_name}:{days}d"
        cached = await self.get_cached(cache_key)
        if cached:
            return cached
        
        try:
            # Get channel first
            channel = await self.get_channel_by_name(channel_name)
            if not channel:
                return []
            
            # Query activity data
            query = text("""
                SELECT 
                    dd.full_date,
                    COUNT(fm.message_key) as message_count,
                    AVG(fm.view_count) as avg_views,
                    AVG(fm.forward_count) as avg_forwards
                FROM marts.fct_messages fm
                JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
                JOIN marts.dim_dates dd ON fm.date_key = dd.date_key
                WHERE dc.channel_name = :channel_name
                    AND dd.full_date >= CURRENT_DATE - INTERVAL ':days days'
                GROUP BY dd.full_date
                ORDER BY dd.full_date DESC
            """)
            
            result = self.db.execute(
                query, 
                {"channel_name": channel_name, "days": days}
            ).fetchall()
            
            activity = []
            for row in result:
                activity.append(ChannelActivity(
                    date=row[0],
                    message_count=row[1] or 0,
                    avg_views=row[2] or 0,
                    avg_forwards=row[3] or 0
                ))
            
            await self.set_cached(cache_key, activity)
            return activity
            
        except Exception as e:
            logger.error(f"Error getting channel activity for {channel_name}: {e}")
            raise
    
    # Message operations
    async def search_messages(
        self, 
        query: str, 
        channel_name: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        has_images: Optional[bool] = None,
        min_views: Optional[int] = None,
        limit: int = 20
    ) -> List[MessageResponse]:
        """Search messages with filters"""
        cache_key = f"search:{query}:{channel_name}:{start_date}:{end_date}:{has_images}:{min_views}:{limit}"
        cached = await self.get_cached(cache_key)
        if cached:
            return cached
        
        try:
            # Build query
            sql_query = self.db.query(TelegramMessage).join(
                Channel, TelegramMessage.channel_key == Channel.channel_key
            ).join(
                DateDimension, TelegramMessage.date_key == DateDimension.date_key
            ).filter(
                TelegramMessage.message_text.ilike(f"%{query}%")
            )
            
            # Apply filters
            if channel_name:
                sql_query = sql_query.filter(Channel.channel_name == channel_name)
            
            if start_date:
                sql_query = sql_query.filter(DateDimension.full_date >= start_date)
            
            if end_date:
                sql_query = sql_query.filter(DateDimension.full_date <= end_date)
            
            if has_images is not None:
                sql_query = sql_query.filter(TelegramMessage.has_image_flag == has_images)
            
            if min_views:
                sql_query = sql_query.filter(TelegramMessage.view_count >= min_views)
            
            # Execute query
            messages = sql_query.order_by(
                desc(TelegramMessage.view_count)
            ).limit(limit).all()
            
            result = []
            for msg in messages:
                # Get channel name
                channel = self.db.query(Channel).filter(
                    Channel.channel_key == msg.channel_key
                ).first()
                
                result.append(MessageResponse(
                    message_id=msg.message_id,
                    message_text=msg.message_text[:500] + "..." if len(msg.message_text) > 500 else msg.message_text,
                    message_date=msg.loaded_at,  # Using loaded_at as proxy for message_date
                    channel_name=channel.channel_name if channel else "Unknown",
                    view_count=msg.view_count,
                    forward_count=msg.forward_count,
                    has_image=msg.has_image_flag,
                    message_length=msg.message_length,
                    engagement_score=msg.engagement_score
                ))
            
            await self.set_cached(cache_key, result)
            return result
            
        except Exception as e:
            logger.error(f"Error searching messages: {e}")
            raise
    
    # Report operations
    async def get_top_products(self, limit: int = 10, days: int = 30) -> List[TopProduct]:
        """Get top mentioned products"""
        cache_key = f"top_products:{limit}:{days}d"
        cached = await self.get_cached(cache_key)
        if cached:
            return cached
        
        try:
            # Simple keyword-based product detection
            # In production, use NLP or more sophisticated detection
            product_keywords = {
                'Paracetamol': ['paracetamol', 'panadol', 'acetaminophen'],
                'Amoxicillin': ['amoxicillin', 'amoxil', 'amoxycillin'],
                'Vitamin C': ['vitamin c', 'ascorbic acid'],
                'Antibiotics': ['antibiotic', 'antibiotics', 'amoxicillin', 'azithromycin'],
                'Pain Relief': ['pain', 'relief', 'analgesic'],
                'Cough Syrup': ['cough', 'syrup', 'expectorant'],
                'Antiseptic': ['antiseptic', 'disinfectant', 'dettol'],
                'Skin Cream': ['cream', 'ointment', 'lotion', 'moisturizer'],
                'Supplements': ['supplement', 'vitamin', 'mineral'],
                'Medical Equipment': ['mask', 'gloves', 'thermometer']
            }
            
            # Get recent messages
            query = text("""
                SELECT 
                    fm.message_text,
                    dc.channel_name,
                    fm.view_count
                FROM marts.fct_messages fm
                JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
                JOIN marts.dim_dates dd ON fm.date_key = dd.date_key
                WHERE dd.full_date >= CURRENT_DATE - INTERVAL ':days days'
                LIMIT 1000
            """)
            
            result = self.db.execute(
                query, {"days": days}
            ).fetchall()
            
            # Count product mentions
            product_counts = {}
            product_channels = {}
            product_views = {}
            
            for row in result:
                message_text = row[0].lower()
                channel_name = row[1]
                views = row[2] or 0
                
                for product, keywords in product_keywords.items():
                    for keyword in keywords:
                        if keyword in message_text:
                            product_counts[product] = product_counts.get(product, 0) + 1
                            
                            if product not in product_channels:
                                product_channels[product] = set()
                            product_channels[product].add(channel_name)
                            
                            if product not in product_views:
                                product_views[product] = []
                            product_views[product].append(views)
                            break  # Only count once per product per message
            
            # Prepare results
            top_products = []
            for product, count in sorted(product_counts.items(), key=lambda x: x[1], reverse=True)[:limit]:
                avg_views = sum(product_views.get(product, [0])) / max(len(product_views.get(product, [1])), 1)
                
                top_products.append(TopProduct(
                    product_name=product,
                    mention_count=count,
                    channels=list(product_channels.get(product, set()))[:5],  # Top 5 channels
                    avg_views=avg_views
                ))
            
            await self.set_cached(cache_key, top_products)
            return top_products
            
        except Exception as e:
            logger.error(f"Error getting top products: {e}")
            raise
    
    async def get_visual_content_stats(self) -> VisualContentStats:
        """Get visual content statistics"""
        cache_key = "visual_content_stats"
        cached = await self.get_cached(cache_key)
        if cached:
            return cached
        
        try:
            # Images by channel
            query_channels = text("""
                SELECT 
                    dc.channel_name,
                    COUNT(fid.detection_key) as image_count
                FROM marts.fct_image_detections fid
                JOIN marts.dim_channels dc ON fid.channel_key = dc.channel_key
                GROUP BY dc.channel_name
                ORDER BY image_count DESC
            """)
            
            channels_result = self.db.execute(query_channels).fetchall()
            images_by_channel = {row[0]: row[1] for row in channels_result}
            
            # Images by category
            query_categories = text("""
                SELECT 
                    image_category,
                    COUNT(*) as count
                FROM marts.fct_image_detections
                GROUP BY image_category
                ORDER BY count DESC
            """)
            
            categories_result = self.db.execute(query_categories).fetchall()
            images_by_category = {row[0]: row[1] for row in categories_result}
            
            # Top detected objects
            query_objects = text("""
                SELECT 
                    unnest(detected_objects) as object_name,
                    COUNT(*) as detection_count
                FROM marts.fct_image_detections
                WHERE detected_objects IS NOT NULL
                GROUP BY unnest(detected_objects)
                ORDER BY detection_count DESC
                LIMIT 10
            """)
            
            objects_result = self.db.execute(query_objects).fetchall()
            top_objects = [
                {"object": row[0], "count": row[1]}
                for row in objects_result
            ]
            
            # Average detections
            query_avg = text("""
                SELECT AVG(detection_count) as avg_detections
                FROM marts.fct_image_detections
                WHERE detection_count > 0
            """)
            
            avg_result = self.db.execute(query_avg).fetchone()
            avg_detections = avg_result[0] or 0
            
            # Total images
            total_images = sum(images_by_channel.values())
            
            stats = VisualContentStats(
                total_images=total_images,
                images_by_channel=images_by_channel,
                images_by_category=images_by_category,
                avg_detections_per_image=avg_detections,
                top_detected_objects=top_objects
            )
            
            await self.set_cached(cache_key, stats)
            return stats
            
        except Exception as e:
            logger.error(f"Error getting visual content stats: {e}")
            raise
    
    async def get_engagement_metrics(self, days: int = 7) -> EngagementMetrics:
        """Get engagement metrics"""
        cache_key = f"engagement_metrics:{days}d"
        cached = await self.get_cached(cache_key)
        if cached:
            return cached
        
        try:
            # Total metrics
            query_totals = text("""
                SELECT 
                    COUNT(*) as total_messages,
                    SUM(view_count) as total_views,
                    SUM(forward_count) as total_forwards,
                    AVG(view_count) as avg_views,
                    AVG(forward_count) as avg_forwards
                FROM marts.fct_messages fm
                JOIN marts.dim_dates dd ON fm.date_key = dd.date_key
                WHERE dd.full_date >= CURRENT_DATE - INTERVAL ':days days'
            """)
            
            totals_result = self.db.execute(query_totals, {"days": days}).fetchone()
            
            # Top performing messages
            query_top = text("""
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
                LIMIT 10
            """)
            
            top_result = self.db.execute(query_top, {"days": days}).fetchall()
            
            top_messages = []
            for row in top_result:
                top_messages.append(MessageResponse(
                    message_id=row[0],
                    message_text=row[1][:200] + "..." if len(row[1]) > 200 else row[1],
                    message_date=row[8],
                    channel_name=row[2],
                    view_count=row[3],
                    forward_count=row[4],
                    has_image=row[5],
                    message_length=row[6],
                    engagement_score=row[7]
                ))
            
            metrics = EngagementMetrics(
                total_messages=totals_result[0] or 0,
                total_views=totals_result[1] or 0,
                total_forwards=totals_result[2] or 0,
                avg_views_per_message=totals_result[3] or 0,
                avg_forwards_per_message=totals_result[4] or 0,
                top_performing_messages=top_messages
            )
            
            await self.set_cached(cache_key, metrics)
            return metrics
            
        except Exception as e:
            logger.error(f"Error getting engagement metrics: {e}")
            raise


async def initialize_cache(redis_client: Redis):
    """Initialize cache with frequently accessed data"""
    try:
        # Cache warm-up tasks
        tasks = [
            ("health_check", {"status": "ok"}),
        ]
        
        for key, value in tasks:
            await redis_client.setex(
                f"init:{key}",
                3600,  # 1 hour
                json.dumps(value)
            )
        
        logger.info("Cache initialized")
    except Exception as e:
        logger.warning(f"Cache warm-up failed: {e}")