"""
Dagster operations for data transformation (Task 2)
"""
import subprocess
import sys
from pathlib import Path
from datetime import datetime
from dagster import op, Out, Output, Failure

@op(
    required_resource_keys={"database"},
    description="Load raw data to PostgreSQL",
    tags={"task": "transformation", "component": "postgres"}
)
def load_raw_to_postgres(context) -> Output[dict]:
    """
    Operation to load scraped JSON data into PostgreSQL
    
    Returns:
        Dictionary with loading results
    """
    context.log.info("Loading raw data to PostgreSQL...")
    
    try:
        # Run the existing load script
        result = subprocess.run(
            [sys.executable, "src/load_to_postgres.py"],
            capture_output=True,
            text=True,
            check=True
        )
        
        context.log.info(f"Data load completed: {result.stdout}")
        
        # Parse output if needed
        loading_results = {
            "timestamp": datetime.now().isoformat(),
            "status": "completed",
            "output": result.stdout[-500:],  # Last 500 chars
            "success": True
        }
        
        return Output(loading_results, "loading_results")
        
    except subprocess.CalledProcessError as e:
        context.log.error(f"Failed to load data: {e.stderr}")
        raise Failure(f"Data loading failed: {e.stderr}")
    except Exception as e:
        raise Failure(f"Data loading failed: {e}")


@op(
    required_resource_keys={"database"},
    description="Run dbt transformations",
    tags={"task": "transformation", "component": "dbt"}
)
def run_dbt_transformations(context, loading_results: dict) -> Output[dict]:
    """
    Operation to run dbt transformations
    
    Args:
        loading_results: Results from load_raw_to_postgres operation
    
    Returns:
        Dictionary with dbt transformation results
    """
    context.log.info("Running dbt transformations...")
    
    try:
        # Run dbt commands
        dbt_dir = Path("dbt_medical")
        
        # 1. Run dbt debug (test connection)
        debug_result = subprocess.run(
            ["dbt", "debug"],
            cwd=dbt_dir,
            capture_output=True,
            text=True
        )
        
        if debug_result.returncode != 0:
            raise Failure(f"dbt debug failed: {debug_result.stderr}")
        
        # 2. Run dbt models
        run_result = subprocess.run(
            ["dbt", "run"],
            cwd=dbt_dir,
            capture_output=True,
            text=True
        )
        
        if run_result.returncode != 0:
            raise Failure(f"dbt run failed: {run_result.stderr}")
        
        # 3. Run dbt tests
        test_result = subprocess.run(
            ["dbt", "test"],
            cwd=dbt_dir,
            capture_output=True,
            text=True
        )
        
        test_failed = test_result.returncode != 0
        
        # 4. Generate documentation
        docs_result = subprocess.run(
            ["dbt", "docs", "generate"],
            cwd=dbt_dir,
            capture_output=True,
            text=True
        )
        
        transformation_results = {
            "timestamp": datetime.now().isoformat(),
            "dbt_debug": "passed" if debug_result.returncode == 0 else "failed",
            "dbt_run": "passed" if run_result.returncode == 0 else "failed",
            "dbt_tests": "passed" if not test_failed else "failed",
            "dbt_docs": "generated" if docs_result.returncode == 0 else "failed",
            "test_failures": test_result.stdout if test_failed else None,
            "success": not test_failed
        }
        
        if test_failed:
            context.log.warning(f"dbt tests failed: {test_result.stdout}")
            # Don't fail the pipeline, just log warning
        else:
            context.log.info("dbt transformations completed successfully")
        
        return Output(transformation_results, "transformation_results")
        
    except Exception as e:
        raise Failure(f"dbt transformations failed: {e}")


@op(
    required_resource_keys={"database"},
    description="Validate data warehouse",
    tags={"task": "transformation", "component": "validation"}
)
def validate_data_warehouse(context, transformation_results: dict) -> Output[dict]:
    """
    Operation to validate the data warehouse after transformations
    
    Args:
        transformation_results: Results from run_dbt_transformations operation
    
    Returns:
        Dictionary with validation results
    """
    context.log.info("Validating data warehouse...")
    
    try:
        # Run validation script
        result = subprocess.run(
            [sys.executable, "src/validate_warehouse.py"],
            capture_output=True,
            text=True
        )
        
        validation_results = {
            "timestamp": datetime.now().isoformat(),
            "validation_script": "completed" if result.returncode == 0 else "failed",
            "output": result.stdout[-500:] if result.stdout else "No output",
            "success": result.returncode == 0
        }
        
        if result.returncode != 0:
            context.log.warning(f"Validation script failed: {result.stderr}")
        
        # Additional validation: Check if tables exist
        db = context.resources.database
        
        try:
            with db.get_session() as session:
                # Check staging table
                staging_check = session.execute(
                    "SELECT COUNT(*) FROM staging.stg_telegram_messages LIMIT 1"
                ).fetchone()
                
                # Check mart tables
                channels_check = session.execute(
                    "SELECT COUNT(*) FROM marts.dim_channels LIMIT 1"
                ).fetchone()
                
                messages_check = session.execute(
                    "SELECT COUNT(*) FROM marts.fct_messages LIMIT 1"
                ).fetchone()
                
                validation_results["table_counts"] = {
                    "stg_telegram_messages": staging_check[0] if staging_check else 0,
                    "dim_channels": channels_check[0] if channels_check else 0,
                    "fct_messages": messages_check[0] if messages_check else 0
                }
                
                context.log.info(f"Table counts: {validation_results['table_counts']}")
                
        except Exception as e:
            context.log.warning(f"Database validation query failed: {e}")
            validation_results["table_validation"] = "failed"
        
        return Output(validation_results, "warehouse_validation")
        
    except Exception as e:
        raise Failure(f"Warehouse validation failed: {e}")