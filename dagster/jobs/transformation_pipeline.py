"""
Dagster job for data transformation pipeline (Task 2)
"""
from dagster import job
from dagster.ops.transformation_ops import (
    load_raw_to_postgres, 
    run_dbt_transformations, 
    validate_data_warehouse
)

@job(
    description="Orchestrate data transformation pipeline",
    tags={"pipeline": "transformation", "task": "2"}
)
def transformation_pipeline():
    """
    Job to transform raw data into analytical data warehouse
    
    This job:
    1. Loads raw JSON data to PostgreSQL
    2. Runs dbt transformations
    3. Validates the data warehouse
    """
    # Load raw data to PostgreSQL
    loading_results = load_raw_to_postgres()
    
    # Run dbt transformations
    transformation_results = run_dbt_transformations(loading_results)
    
    # Validate data warehouse
    validate_data_warehouse(transformation_results)