"""
SQLAlchemy models for database tables
"""
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Float, Text, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import ARRAY

Base = declarative_base()


class TelegramMessage(Base):
    """SQLAlchemy model for fct_messages table"""
    __tablename__ = "fct_messages"
    __table_args__ = {"schema": "marts"}
    
    message_key = Column(String, primary_key=True)
    message_id = Column(Integer)
    channel_key = Column(String)
    date_key = Column(String)
    message_text = Column(Text)
    message_length = Column(Integer)
    view_count = Column(Integer)
    forward_count = Column(Integer)
    has_image_flag = Column(Boolean)
    detected_product_category = Column(String)
    engagement_score = Column(Float)
    message_length_category = Column(String)
    loaded_at = Column(DateTime)


class Channel(Base):
    """SQLAlchemy model for dim_channels table"""
    __tablename__ = "dim_channels"
    __table_args__ = {"schema": "marts"}
    
    channel_key = Column(String, primary_key=True)
    channel_name = Column(String)
    channel_type = Column(String)
    first_post_date = Column(DateTime)
    last_post_date = Column(DateTime)
    total_posts = Column(Integer)
    avg_views = Column(Float)
    avg_forwards = Column(Float)
    total_images = Column(Integer)
    image_percentage = Column(Float)
    activity_status = Column(String)
    loaded_at = Column(DateTime)


class DateDimension(Base):
    """SQLAlchemy model for dim_dates table"""
    __tablename__ = "dim_dates"
    __table_args__ = {"schema": "marts"}
    
    date_key = Column(String, primary_key=True)
    full_date = Column(Date)
    year = Column(Integer)
    quarter = Column(Integer)
    month = Column(Integer)
    month_name = Column(String)
    week_of_year = Column(Integer)
    day_of_month = Column(Integer)
    day_of_week = Column(Integer)
    day_name = Column(String)
    day_of_year = Column(Integer)
    is_weekend = Column(Boolean)
    holiday_flag = Column(String)
    loaded_at = Column(DateTime)


class ImageDetection(Base):
    """SQLAlchemy model for fct_image_detections table"""
    __tablename__ = "fct_image_detections"
    __table_args__ = {"schema": "marts"}
    
    detection_key = Column(String, primary_key=True)
    message_id = Column(Integer)
    channel_key = Column(String)
    date_key = Column(String)
    image_path = Column(String)
    detected_objects = Column(ARRAY(String))
    confidence_scores = Column(ARRAY(Float))
    detection_count = Column(Integer)
    image_category = Column(String)
    avg_confidence = Column(Float)
    max_confidence = Column(Float)
    has_person = Column(Boolean)
    has_container = Column(Boolean)
    processing_time = Column(Float)
    model_version = Column(String)
    processed_at = Column(DateTime)
    content_strategy = Column(String)
    loaded_at = Column(DateTime)
    scene_composition = Column(String)
    detection_quality = Column(String)