"""
Pydantic schemas for request/response validation
"""
from typing import List, Optional, Dict, Any
from datetime import datetime, date
from pydantic import BaseModel, Field, validator


# Common schemas
class HealthCheck(BaseModel):
    """Health check response schema"""
    status: str
    database: str
    cache: str
    timestamp: str


class PaginationParams(BaseModel):
    """Pagination parameters"""
    page: int = Field(1, ge=1, description="Page number")
    limit: int = Field(10, ge=1, le=100, description="Items per page")
    
    @validator('limit')
    def validate_limit(cls, v):
        if v > 100:
            return 100
        return v


# Channel schemas
class ChannelBase(BaseModel):
    """Base channel schema"""
    channel_name: str
    channel_type: str
    total_posts: int
    avg_views: float


class ChannelResponse(ChannelBase):
    """Channel response schema"""
    channel_key: str
    first_post_date: Optional[datetime]
    last_post_date: Optional[datetime]
    image_percentage: float
    activity_status: str


class ChannelActivity(BaseModel):
    """Channel activity schema"""
    date: date
    message_count: int
    avg_views: float
    avg_forwards: float


class ChannelStats(BaseModel):
    """Channel statistics schema"""
    channel: ChannelResponse
    activity: List[ChannelActivity]
    total_messages: int
    total_images: int
    top_products: List[str]


# Message schemas
class MessageBase(BaseModel):
    """Base message schema"""
    message_id: int
    message_text: str
    message_date: datetime


class MessageResponse(MessageBase):
    """Message response schema"""
    channel_name: str
    view_count: int
    forward_count: int
    has_image: bool
    message_length: int
    engagement_score: float


class MessageSearchParams(BaseModel):
    """Message search parameters"""
    query: str = Field(..., min_length=1, max_length=100, description="Search query")
    channel_name: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    has_images: Optional[bool] = None
    min_views: Optional[int] = Field(None, ge=0)
    limit: int = Field(20, ge=1, le=100)


# Report schemas
class TopProduct(BaseModel):
    """Top product schema"""
    product_name: str
    mention_count: int
    channels: List[str]
    avg_views: float


class TopProductsResponse(BaseModel):
    """Top products response schema"""
    products: List[TopProduct]
    total_mentions: int
    time_period: Dict[str, date]


class VisualContentStats(BaseModel):
    """Visual content statistics schema"""
    total_images: int
    images_by_channel: Dict[str, int]
    images_by_category: Dict[str, int]
    avg_detections_per_image: float
    top_detected_objects: List[Dict[str, Any]]


class EngagementMetrics(BaseModel):
    """Engagement metrics schema"""
    total_messages: int
    total_views: int
    total_forwards: int
    avg_views_per_message: float
    avg_forwards_per_message: float
    top_performing_messages: List[MessageResponse]


# Search schemas
class SearchResponse(BaseModel):
    """Search response schema"""
    query: str
    total_results: int
    messages: List[MessageResponse]
    page: int
    total_pages: int


# Analytics schemas
class TrendPoint(BaseModel):
    """Trend data point schema"""
    date: date
    value: float


class TimeSeriesData(BaseModel):
    """Time series data schema"""
    metric: str
    data: List[TrendPoint]
    total: float
    avg_per_day: float


class AnalyticsResponse(BaseModel):
    """Analytics response schema"""
    time_period: Dict[str, date]
    metrics: Dict[str, TimeSeriesData]
    summary: Dict[str, Any]