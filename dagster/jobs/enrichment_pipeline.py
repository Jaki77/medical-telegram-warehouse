"""
Dagster job for data enrichment pipeline (Task 3)
"""
from dagster import job
from dagster.ops.enrichment_ops import (
    run_yolo_enrichment, 
    load_yolo_to_warehouse, 
    analyze_yolo_results
)

@job(
    description="Orchestrate data enrichment pipeline with YOLO",
    tags={"pipeline": "enrichment", "task": "3"}
)
def enrichment_pipeline():
    """
    Job to enrich data with YOLO object detection
    
    This job:
    1. Runs YOLO object detection on images
    2. Loads detection results to data warehouse
    3. Analyzes results for business insights
    """
    # Run YOLO object detection
    enrichment_results = run_yolo_enrichment()
    
    # Load YOLO results to warehouse
    yolo_loading_results = load_yolo_to_warehouse(enrichment_results)
    
    # Analyze YOLO results
    analyze_yolo_results(yolo_loading_results)