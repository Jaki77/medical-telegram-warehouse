"""
Dagster repository containing all jobs, schedules, and resources
"""
from dagster import repository, Definitions

from dagster.resources.database import postgres_resource, docker_resource
from dagster.resources.telegram_api import telegram_resource
from dagster.resources.yolo_model import yolo_resource

from dagster.jobs.scraping_pipeline import scraping_pipeline
from dagster.jobs.transformation_pipeline import transformation_pipeline
from dagster.jobs.enrichment_pipeline import enrichment_pipeline
from dagster.jobs.api_pipeline import api_pipeline
from dagster.jobs.full_pipeline import full_pipeline

from dagster.schedules.daily_schedule import (
    daily_scraping_schedule,
    daily_transformation_schedule,
    daily_full_pipeline_schedule
)
from dagster.schedules.hourly_schedule import (
    hourly_enrichment_schedule,
    hourly_api_test_schedule
)

# Define resources
resources = {
    "database": postgres_resource.configured({
        "host": "localhost",
        "port": 5432,
        "database": "medical_warehouse",
        "username": "postgres",
        "password": "postgres123"
    }),
    "docker": docker_resource.configured({
        "docker_compose_path": "docker-compose.yml"
    }),
    "telegram": telegram_resource.configured({
        "api_id": "your_api_id",  # Should come from environment
        "api_hash": "your_api_hash",
        "phone_number": "+251XXXXXXXXX",
        "channels": "chemed,lobelia4cosmetics,tikvahpharma",
        "days_back": 30
    }),
    "yolo": yolo_resource.configured({
        "model_path": "models/yolov8n.pt",
        "confidence_threshold": 0.25,
        "device": "cpu"
    })
}

# Define jobs
jobs = [
    scraping_pipeline,
    transformation_pipeline,
    enrichment_pipeline,
    api_pipeline,
    full_pipeline
]

# Define schedules
schedules = [
    daily_scraping_schedule,
    daily_transformation_schedule,
    daily_full_pipeline_schedule,
    hourly_enrichment_schedule,
    hourly_api_test_schedule
]

# Create definitions
defs = Definitions(
    assets=[],
    jobs=jobs,
    resources=resources,
    schedules=schedules
)

@repository
def medical_telegram_repository():
    """Repository for Medical Telegram Pipeline"""
    return [
        scraping_pipeline,
        transformation_pipeline,
        enrichment_pipeline,
        api_pipeline,
        full_pipeline,
        daily_scraping_schedule,
        daily_transformation_schedule,
        daily_full_pipeline_schedule,
        hourly_enrichment_schedule,
        hourly_api_test_schedule
    ]