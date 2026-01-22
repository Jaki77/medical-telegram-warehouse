"""
Hourly schedules for Dagster pipelines
"""
from dagster import schedule, ScheduleEvaluationContext
from dagster.jobs.enrichment_pipeline import enrichment_pipeline
from dagster.jobs.api_pipeline import api_pipeline

@schedule(
    job=enrichment_pipeline,
    cron_schedule="0 * * * *",  # Hourly
    execution_timezone="UTC",
    description="Hourly image enrichment"
)
def hourly_enrichment_schedule(context: ScheduleEvaluationContext):
    """Schedule for hourly image enrichment"""
    run_config = {
        "resources": {
            "yolo": {
                "config": {
                    "model_path": "models/yolov8n.pt",
                    "confidence_threshold": 0.25,
                    "device": "cpu"
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
            }
        }
    }
    return run_config


@schedule(
    job=api_pipeline,
    cron_schedule="30 * * * *",  # Hourly at :30
    execution_timezone="UTC",
    description="Hourly API testing"
)
def hourly_api_test_schedule(context: ScheduleEvaluationContext):
    """Schedule for hourly API testing"""
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
        },
        "ops": {
            "start_fastapi": {
                "config": {
                    "api_process": None
                }
            }
        }
    }
    return run_config