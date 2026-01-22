"""
Daily schedules for Dagster pipelines
"""
from dagster import schedule, ScheduleEvaluationContext
from dagster.jobs.scraping_pipeline import scraping_pipeline
from dagster.jobs.transformation_pipeline import transformation_pipeline
from dagster.jobs.enrichment_pipeline import enrichment_pipeline
from dagster.jobs.full_pipeline import full_pipeline

@schedule(
    job=scraping_pipeline,
    cron_schedule="0 2 * * *",  # Daily at 2:00 AM
    execution_timezone="UTC",
    description="Daily Telegram data scraping"
)
def daily_scraping_schedule(context: ScheduleEvaluationContext):
    """Schedule for daily Telegram scraping"""
    run_config = {
        "resources": {
            "telegram": {
                "config": {
                    "api_id": {"env": "TELEGRAM_API_ID"},
                    "api_hash": {"env": "TELEGRAM_API_HASH"},
                    "phone_number": {"env": "TELEGRAM_PHONE_NUMBER"},
                    "channels": "chemed,lobelia4cosmetics,tikvahpharma",
                    "days_back": 1  # Only get last day's data
                }
            }
        }
    }
    return run_config


@schedule(
    job=transformation_pipeline,
    cron_schedule="0 3 * * *",  # Daily at 3:00 AM
    execution_timezone="UTC",
    description="Daily data transformation"
)
def daily_transformation_schedule(context: ScheduleEvaluationContext):
    """Schedule for daily data transformation"""
    run_config = {
        "resources": {
            "database": {
                "config": {
                    "host": "localhost",
                    "port": 5432,
                    "database": "medical_warehouse",
                    "username": "postgres",
                    "password": {"env": "DB_PASSWORD"}
                }
            }
        }
    }
    return run_config


@schedule(
    job=full_pipeline,
    cron_schedule="0 4 * * *",  # Daily at 4:00 AM
    execution_timezone="UTC",
    description="Daily full pipeline execution"
)
def daily_full_pipeline_schedule(context: ScheduleEvaluationContext):
    """Schedule for daily full pipeline execution"""
    run_config = {
        "resources": {
            "docker": {
                "config": {
                    "docker_compose_path": "docker-compose.yml"
                }
            },
            "telegram": {
                "config": {
                    "api_id": {"env": "TELEGRAM_API_ID"},
                    "api_hash": {"env": "TELEGRAM_API_HASH"},
                    "phone_number": {"env": "TELEGRAM_PHONE_NUMBER"},
                    "channels": "chemed,lobelia4cosmetics,tikvahpharma",
                    "days_back": 1
                }
            },
            "database": {
                "config": {
                    "host": "localhost",
                    "port": 5432,
                    "database": "medical_warehouse",
                    "username": "postgres",
                    "password": {"env": "DB_PASSWORD"}
                }
            },
            "yolo": {
                "config": {
                    "model_path": "models/yolov8n.pt",
                    "confidence_threshold": 0.25,
                    "device": "cpu"
                }
            }
        }
    }
    return run_config