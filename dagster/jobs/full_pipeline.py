"""
Dagster job for complete end-to-end pipeline (All Tasks)
"""
from dagster import job, OpExecutionContext, op
from dagster.ops.scraping_ops import (
    scrape_telegram_data, 
    validate_scraped_data
)
from dagster.ops.transformation_ops import (
    load_raw_to_postgres, 
    run_dbt_transformations, 
    validate_data_warehouse
)
from dagster.ops.enrichment_ops import (
    run_yolo_enrichment, 
    load_yolo_to_warehouse, 
    analyze_yolo_results
)
from dagster.ops.api_ops import (
    start_fastapi, 
    test_api_endpoints, 
    stop_fastapi,
    generate_pipeline_report
)

@op(
    required_resource_keys={"docker"},
    description="Start required infrastructure",
    tags={"component": "infrastructure"}
)
def start_infrastructure(context: OpExecutionContext):
    """Start Docker infrastructure"""
    context.log.info("Starting infrastructure...")
    
    docker = context.resources.docker
    
    # Start PostgreSQL and Redis
    if docker.start_services(["postgres", "redis"]):
        # Wait for services to be ready
        import time
        time.sleep(10)
        
        # Check PostgreSQL health
        if docker.check_service_health("medical_warehouse_db", timeout=30):
            context.log.info("Infrastructure started successfully")
            return {"status": "started", "services": ["postgres", "redis"]}
        else:
            raise Failure("PostgreSQL failed to start")
    else:
        raise Failure("Failed to start infrastructure")


@op(
    required_resource_keys={"docker"},
    description="Stop infrastructure",
    tags={"component": "infrastructure"}
)
def stop_infrastructure(context: OpExecutionContext, pipeline_report: dict):
    """Stop Docker infrastructure"""
    context.log.info("Stopping infrastructure...")
    
    docker = context.resources.docker
    
    if docker.stop_services():
        context.log.info("Infrastructure stopped successfully")
        return {"status": "stopped"}
    else:
        context.log.warning("Failed to stop infrastructure")
        return {"status": "stop_failed"}


@job(
    description="Complete end-to-end medical telegram pipeline",
    tags={"pipeline": "full", "tasks": "1-5"},
    config={
        "ops": {
            "start_fastapi": {
                "config": {
                    "api_process": None
                }
            }
        }
    }
)
def full_pipeline():
    """
    Complete end-to-end pipeline orchestrating all tasks
    
    This job orchestrates:
    1. Infrastructure startup
    2. Telegram data scraping (Task 1)
    3. Data transformation (Task 2)
    4. Image enrichment with YOLO (Task 3)
    5. API testing (Task 4)
    6. Report generation
    7. Infrastructure shutdown
    """
    # Start infrastructure
    infrastructure = start_infrastructure()
    
    # Task 1: Scraping pipeline
    scraping_results = scrape_telegram_data()
    validation_results = validate_scraped_data(scraping_results)
    
    # Task 2: Transformation pipeline
    loading_results = load_raw_to_postgres()
    transformation_results = run_dbt_transformations(loading_results)
    warehouse_validation = validate_data_warehouse(transformation_results)
    
    # Task 3: Enrichment pipeline
    enrichment_results = run_yolo_enrichment()
    yolo_loading_results = load_yolo_to_warehouse(enrichment_results)
    yolo_analysis_results = analyze_yolo_results(yolo_loading_results)
    
    # Task 4: API pipeline
    api_startup_results = start_fastapi()
    api_test_results = test_api_endpoints(api_startup_results)
    api_shutdown_results = stop_fastapi(api_test_results)
    
    # Generate comprehensive report
    pipeline_report = generate_pipeline_report(
        scraping_results,
        validation_results,
        transformation_results,
        warehouse_validation,
        enrichment_results,
        yolo_analysis_results,
        api_test_results
    )
    
    # Stop infrastructure
    stop_infrastructure(pipeline_report)