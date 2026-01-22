"""
Dagster operations for data enrichment with YOLO (Task 3)
"""
import subprocess
import sys
from pathlib import Path
from datetime import datetime
from dagster import op, Out, Output, Failure

@op(
    required_resource_keys={"yolo", "database"},
    description="Run YOLO object detection on images",
    tags={"task": "enrichment", "component": "yolo"}
)
def run_yolo_enrichment(context) -> Output[dict]:
    """
    Operation to run YOLO object detection on scraped images
    
    Returns:
        Dictionary with YOLO detection results
    """
    context.log.info("Running YOLO object detection...")
    
    # Check if YOLO model is loaded
    yolo_config = context.resources.yolo.get_config()
    if not yolo_config.get("loaded", False):
        context.log.info("Loading YOLO model...")
        if not context.resources.yolo.load_model():
            raise Failure("Failed to load YOLO model")
    
    try:
        # Run the YOLO detection script
        result = subprocess.run(
            [sys.executable, "src/yolo_detect.py"],
            capture_output=True,
            text=True
        )
        
        enrichment_results = {
            "timestamp": datetime.now().isoformat(),
            "yolo_detection": "completed" if result.returncode == 0 else "failed",
            "output": result.stdout[-500:] if result.stdout else "No output",
            "error": result.stderr if result.returncode != 0 else None,
            "success": result.returncode == 0
        }
        
        if result.returncode != 0:
            context.log.error(f"YOLO detection failed: {result.stderr}")
            # Don't fail the pipeline, just log error
        else:
            context.log.info("YOLO detection completed successfully")
        
        return Output(enrichment_results, "enrichment_results")
        
    except Exception as e:
        raise Failure(f"YOLO enrichment failed: {e}")


@op(
    required_resource_keys={"database"},
    description="Load YOLO results to data warehouse",
    tags={"task": "enrichment", "component": "dbt"}
)
def load_yolo_to_warehouse(context, enrichment_results: dict) -> Output[dict]:
    """
    Operation to load YOLO detection results into data warehouse
    
    Args:
        enrichment_results: Results from run_yolo_enrichment operation
    
    Returns:
        Dictionary with loading results
    """
    context.log.info("Loading YOLO results to data warehouse...")
    
    try:
        # Run dbt model for image detections
        dbt_dir = Path("dbt_medical")
        
        result = subprocess.run(
            ["dbt", "run", "--select", "+fct_image_detections"],
            cwd=dbt_dir,
            capture_output=True,
            text=True
        )
        
        loading_results = {
            "timestamp": datetime.now().isoformat(),
            "dbt_model": "completed" if result.returncode == 0 else "failed",
            "output": result.stdout[-500:] if result.stdout else "No output",
            "success": result.returncode == 0
        }
        
        if result.returncode != 0:
            context.log.error(f"Failed to load YOLO results: {result.stderr}")
        else:
            context.log.info("YOLO results loaded to data warehouse")
        
        # Run tests on image detections
        test_result = subprocess.run(
            ["dbt", "test", "--select", "fct_image_detections"],
            cwd=dbt_dir,
            capture_output=True,
            text=True
        )
        
        if test_result.returncode == 0:
            loading_results["tests"] = "passed"
        else:
            loading_results["tests"] = "failed"
            loading_results["test_output"] = test_result.stdout
            context.log.warning(f"YOLO model tests failed: {test_result.stdout}")
        
        return Output(loading_results, "yolo_loading_results")
        
    except Exception as e:
        raise Failure(f"Failed to load YOLO results to warehouse: {e}")


@op(
    required_resource_keys={"database"},
    description="Analyze YOLO detection results",
    tags={"task": "enrichment", "component": "analysis"}
)
def analyze_yolo_results(context, yolo_loading_results: dict) -> Output[dict]:
    """
    Operation to analyze YOLO detection results and generate business insights
    
    Args:
        yolo_loading_results: Results from load_yolo_to_warehouse operation
    
    Returns:
        Dictionary with analysis results
    """
    context.log.info("Analyzing YOLO detection results...")
    
    try:
        # Run analysis script
        result = subprocess.run(
            [sys.executable, "src/analyze_detections.py"],
            capture_output=True,
            text=True
        )
        
        analysis_results = {
            "timestamp": datetime.now().isoformat(),
            "analysis": "completed" if result.returncode == 0 else "failed",
            "output": result.stdout[-500:] if result.stdout else "No output",
            "success": result.returncode == 0
        }
        
        if result.returncode != 0:
            context.log.error(f"YOLO analysis failed: {result.stderr}")
        else:
            context.log.info("YOLO analysis completed successfully")
            
            # Check if analysis files were created
            results_dir = Path("data/processed/results")
            if results_dir.exists():
                analysis_files = list(results_dir.glob("*.json")) + list(results_dir.glob("*.txt"))
                analysis_results["files_generated"] = [f.name for f in analysis_files]
                analysis_results["file_count"] = len(analysis_files)
        
        return Output(analysis_results, "yolo_analysis_results")
        
    except Exception as e:
        raise Failure(f"YOLO analysis failed: {e}")