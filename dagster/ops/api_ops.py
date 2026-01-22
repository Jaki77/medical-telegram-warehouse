"""
Dagster operations for API management (Task 4)
"""
import subprocess
import time
import signal
import sys
from pathlib import Path
from datetime import datetime
from dagster import op, Out, Output, Failure

@op(
    required_resource_keys={"database"},
    description="Start FastAPI application",
    tags={"task": "api", "component": "fastapi"}
)
def start_fastapi(context) -> Output[dict]:
    """
    Operation to start the FastAPI application
    
    Returns:
        Dictionary with API startup results
    """
    context.log.info("Starting FastAPI application...")
    
    api_process = None
    
    try:
        # Start API in a subprocess
        api_process = subprocess.Popen(
            [sys.executable, "api/main.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Give API time to start
        time.sleep(5)
        
        # Check if API is running
        import requests
        try:
            response = requests.get("http://localhost:8000/health", timeout=10)
            api_status = "running" if response.status_code == 200 else "failed"
        except:
            api_status = "failed"
        
        api_results = {
            "timestamp": datetime.now().isoformat(),
            "status": api_status,
            "pid": api_process.pid,
            "success": api_status == "running"
        }
        
        if api_status == "running":
            context.log.info("FastAPI application started successfully")
        else:
            context.log.error("Failed to start FastAPI application")
            if api_process:
                api_process.terminate()
            raise Failure("FastAPI failed to start")
        
        # Store process in context for cleanup
        context.op_config["api_process"] = api_process
        
        return Output(api_results, "api_startup_results")
        
    except Exception as e:
        if api_process:
            api_process.terminate()
        raise Failure(f"Failed to start FastAPI: {e}")


@op(
    required_resource_keys={"database"},
    description="Test API endpoints",
    tags={"task": "api", "component": "testing"}
)
def test_api_endpoints(context, api_startup_results: dict) -> Output[dict]:
    """
    Operation to test API endpoints
    
    Args:
        api_startup_results: Results from start_fastapi operation
    
    Returns:
        Dictionary with API test results
    """
    context.log.info("Testing API endpoints...")
    
    try:
        # Run API tests
        result = subprocess.run(
            [sys.executable, "tests/test_api.py"],
            capture_output=True,
            text=True
        )
        
        test_results = {
            "timestamp": datetime.now().isoformat(),
            "tests": "passed" if result.returncode == 0 else "failed",
            "output": result.stdout[-500:] if result.stdout else "No output",
            "success": result.returncode == 0
        }
        
        if result.returncode != 0:
            context.log.warning(f"API tests failed: {result.stderr}")
            # Don't fail pipeline for test failures
        else:
            context.log.info("API tests passed")
        
        # Additional endpoint checks
        import requests
        
        endpoints_to_test = [
            ("/health", 200),
            ("/api/reports/top-products?limit=5", 200),
            ("/api/reports/visual-content", 200),
        ]
        
        endpoint_results = []
        for endpoint, expected_status in endpoints_to_test:
            try:
                response = requests.get(f"http://localhost:8000{endpoint}", timeout=10)
                endpoint_results.append({
                    "endpoint": endpoint,
                    "status": response.status_code,
                    "expected": expected_status,
                    "passed": response.status_code == expected_status
                })
            except Exception as e:
                endpoint_results.append({
                    "endpoint": endpoint,
                    "status": "error",
                    "expected": expected_status,
                    "passed": False,
                    "error": str(e)
                })
        
        test_results["endpoint_checks"] = endpoint_results
        
        # Count passed checks
        passed_checks = sum(1 for check in endpoint_results if check["passed"])
        test_results["endpoints_passed"] = f"{passed_checks}/{len(endpoint_results)}"
        
        return Output(test_results, "api_test_results")
        
    except Exception as e:
        raise Failure(f"API testing failed: {e}")


@op(
    required_resource_keys={"database"},
    description="Stop FastAPI application",
    tags={"task": "api", "component": "fastapi"}
)
def stop_fastapi(context, api_test_results: dict) -> Output[dict]:
    """
    Operation to stop the FastAPI application
    
    Args:
        api_test_results: Results from test_api_endpoints operation
    
    Returns:
        Dictionary with API shutdown results
    """
    context.log.info("Stopping FastAPI application...")
    
    try:
        # Try to stop API gracefully
        import requests
        try:
            response = requests.get("http://localhost:8000/health", timeout=5)
            if response.status_code == 200:
                context.log.info("API was running, attempting graceful shutdown")
        except:
            context.log.info("API not running or already stopped")
        
        # If we stored the process in context, terminate it
        api_process = context.op_config.get("api_process")
        if api_process:
            api_process.terminate()
            api_process.wait(timeout=10)
            context.log.info("API process terminated")
        
        shutdown_results = {
            "timestamp": datetime.now().isoformat(),
            "status": "stopped",
            "success": True
        }
        
        return Output(shutdown_results, "api_shutdown_results")
        
    except Exception as e:
        context.log.warning(f"Error stopping API: {e}")
        # Don't fail the pipeline for shutdown issues
        return Output({
            "timestamp": datetime.now().isoformat(),
            "status": "shutdown_error",
            "error": str(e),
            "success": False
        }, "api_shutdown_results")


@op(
    required_resource_keys={"database"},
    description="Generate pipeline report",
    tags={"task": "reporting", "component": "documentation"}
)
def generate_pipeline_report(
    context, 
    scraping_results: dict,
    validation_results: dict,
    transformation_results: dict,
    warehouse_validation: dict,
    enrichment_results: dict,
    yolo_analysis_results: dict,
    api_test_results: dict
) -> Output[dict]:
    """
    Operation to generate a comprehensive pipeline report
    
    Args:
        Various results from previous operations
    
    Returns:
        Dictionary with pipeline report
    """
    context.log.info("Generating pipeline report...")
    
    try:
        # Compile results from all operations
        pipeline_report = {
            "timestamp": datetime.now().isoformat(),
            "pipeline_version": "1.0.0",
            "stages": {
                "scraping": {
                    "messages": scraping_results.get("total_messages", 0),
                    "images": scraping_results.get("total_images", 0),
                    "channels": len(scraping_results.get("channels_scraped", [])),
                    "validation_passed": validation_results.get("passed", False)
                },
                "transformation": {
                    "dbt_run": transformation_results.get("dbt_run") == "passed",
                    "dbt_tests": transformation_results.get("dbt_tests") == "passed",
                    "warehouse_validation": warehouse_validation.get("success", False)
                },
                "enrichment": {
                    "yolo_detection": enrichment_results.get("success", False),
                    "yolo_analysis": yolo_analysis_results.get("success", False)
                },
                "api": {
                    "tests_passed": api_test_results.get("tests") == "passed",
                    "endpoints_tested": api_test_results.get("endpoints_passed", "0/0")
                }
            },
            "summary": {},
            "recommendations": []
        }
        
        # Calculate overall success
        stages = pipeline_report["stages"]
        all_passed = (
            stages["scraping"]["validation_passed"] and
            stages["transformation"]["dbt_run"] and
            stages["transformation"]["warehouse_validation"] and
            stages["enrichment"]["yolo_detection"] and
            stages["api"]["tests_passed"]
        )
        
        pipeline_report["overall_success"] = all_passed
        pipeline_report["summary"]["status"] = "SUCCESS" if all_passed else "PARTIAL_SUCCESS"
        
        # Generate recommendations
        if not stages["scraping"]["validation_passed"]:
            pipeline_report["recommendations"].append(
                "Review scraping errors and check Telegram API credentials"
            )
        
        if not stages["transformation"]["dbt_tests"]:
            pipeline_report["recommendations"].append(
                "Investigate failed dbt tests and fix data quality issues"
            )
        
        if not stages["enrichment"]["yolo_detection"]:
            pipeline_report["recommendations"].append(
                "Check YOLO model and image processing pipeline"
            )
        
        # Save report to file
        report_dir = Path("reports")
        report_dir.mkdir(exist_ok=True)
        
        report_path = report_dir / f"pipeline_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        import json
        with open(report_path, 'w') as f:
            json.dump(pipeline_report, f, indent=2, default=str)
        
        # Generate human-readable summary
        summary_path = report_dir / f"pipeline_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        summary_lines = [
            "="*60,
            "MEDICAL TELEGRAM PIPELINE - EXECUTION REPORT",
            "="*60,
            f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Status: {pipeline_report['summary']['status']}",
            "",
            "STAGE RESULTS:",
            "-"*40,
            f"Scraping: {stages['scraping']['messages']} messages, "
            f"{stages['scraping']['images']} images from {stages['scraping']['channels']} channels",
            f"  Validation: {'PASSED' if stages['scraping']['validation_passed'] else 'FAILED'}",
            "",
            f"Transformation: dbt run {'PASSED' if stages['transformation']['dbt_run'] else 'FAILED'}, "
            f"tests {'PASSED' if stages['transformation']['dbt_tests'] else 'FAILED'}",
            f"  Warehouse validation: {'PASSED' if stages['transformation']['warehouse_validation'] else 'FAILED'}",
            "",
            f"Enrichment: YOLO detection {'PASSED' if stages['enrichment']['yolo_detection'] else 'FAILED'}, "
            f"analysis {'PASSED' if stages['enrichment']['yolo_analysis'] else 'FAILED'}",
            "",
            f"API: Tests {'PASSED' if stages['api']['tests_passed'] else 'FAILED'}, "
            f"endpoints: {stages['api']['endpoints_tested']}",
            "",
            "RECOMMENDATIONS:" if pipeline_report["recommendations"] else "NO ISSUES DETECTED",
            "-"*40 if pipeline_report["recommendations"] else "",
        ]
        
        for rec in pipeline_report["recommendations"]:
            summary_lines.append(f"• {rec}")
        
        summary_lines.extend([
            "",
            "="*60,
            f"Report saved to: {report_path}",
            f"Summary saved to: {summary_path}",
            "="*60
        ])
        
        summary_text = "\n".join(summary_lines)
        
        with open(summary_path, 'w') as f:
            f.write(summary_text)
        
        # Log summary
        context.log.info(summary_text)
        
        return Output(pipeline_report, "pipeline_report")
        
    except Exception as e:
        raise Failure(f"Failed to generate pipeline report: {e}")