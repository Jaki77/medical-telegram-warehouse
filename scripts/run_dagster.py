#!/usr/bin/env python3
"""
Task 5: Run Dagster Orchestration Pipeline
"""
import subprocess
import sys
import time
from pathlib import Path
from datetime import datetime

def check_dagster_installation():
    """Check if Dagster is installed"""
    print("\n" + "="*60)
    print("DAGSTER INSTALLATION CHECK")
    print("="*60)
    
    try:
        import dagster
        print("✓ Dagster installed")
    except:
        print("✗ Dagster not installed. Run: pip install dagster dagster-webserver")
        return False
    
    try:
        import dagster_webserver
        print("✓ Dagster webserver installed")
    except:
        print("✗ Dagster webserver not installed. Run: pip install dagster-webserver")
        return False
    
    return True

def start_dagster_ui():
    """Start Dagster UI"""
    print("\n" + "="*60)
    print("STARTING DAGSTER UI")
    print("="*60)
    
    print("Dagster UI will be available at: http://localhost:3000")
    print("Press Ctrl+C to stop the UI")
    print("\nAvailable pipelines:")
    print("  • scraping_pipeline - Telegram data scraping")
    print("  • transformation_pipeline - Data transformation")
    print("  • enrichment_pipeline - YOLO image enrichment")
    print("  • api_pipeline - API testing")
    print("  • full_pipeline - Complete end-to-end pipeline")
    print("\nSchedules:")
    print("  • Daily at 2:00 AM - Telegram scraping")
    print("  • Daily at 3:00 AM - Data transformation")
    print("  • Daily at 4:00 AM - Full pipeline")
    print("  • Hourly - Image enrichment")
    print("  • Hourly at :30 - API testing")
    print("\n" + "="*60)
    
    try:
        # Start Dagster webserver
        subprocess.run([
            "dagster", "dev",
            "-f", "dagster/repository.py",
            "-h", "0.0.0.0",
            "-p", "3000"
        ])
        return True
    except KeyboardInterrupt:
        print("\nDagster UI stopped by user")
        return True
    except Exception as e:
        print(f"✗ Failed to start Dagster UI: {e}")
        return False

def run_pipeline(pipeline_name: str):
    """Run a specific pipeline"""
    print(f"\n" + "="*60)
    print(f"RUNNING PIPELINE: {pipeline_name}")
    print("="*60)
    
    try:
        # Create run config file
        run_config = {
            "resources": {
                "database": {
                    "config": {
                        "host": "localhost",
                        "port": 5432,
                        "database": "medical_warehouse",
                        "username": "postgres",
                        "password": "postgres123"
                    }
                },
                "telegram": {
                    "config": {
                        "api_id": "YOUR_API_ID",
                        "api_hash": "YOUR_API_HASH",
                        "phone_number": "+251XXXXXXXXX",
                        "channels": "chemed,lobelia4cosmetics,tikvahpharma",
                        "days_back": 7
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
        
        import yaml
        config_path = Path("dagster_run_config.yaml")
        with open(config_path, 'w') as f:
            yaml.dump(run_config, f)
        
        # Run pipeline using dagster CLI
        result = subprocess.run([
            "dagster", "job", "execute",
            "-f", "dagster/repository.py",
            "-j", pipeline_name,
            "-c", str(config_path)
        ], capture_output=True, text=True)
        
        # Clean up config file
        config_path.unlink(missing_ok=True)
        
        if result.returncode == 0:
            print(f"✓ Pipeline {pipeline_name} executed successfully")
            print(f"Output: {result.stdout[-500:]}")
            return True
        else:
            print(f"✗ Pipeline {pipeline_name} failed")
            print(f"Error: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"✗ Failed to run pipeline: {e}")
        return False

def main():
    """Main execution function"""
    print("="*60)
    print("TASK 5: PIPELINE ORCHESTRATION WITH DAGSTER")
    print("="*60)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check installation
    if not check_dagster_installation():
        print("\n⚠ Please install Dagster before continuing.")
        return False
    
    # Create necessary directories
    Path("logs").mkdir(exist_ok=True)
    Path("reports").mkdir(exist_ok=True)
    
    print("\nOptions:")
    print("1. Start Dagster UI (recommended)")
    print("2. Run scraping_pipeline")
    print("3. Run transformation_pipeline")
    print("4. Run enrichment_pipeline")
    print("5. Run api_pipeline")
    print("6. Run full_pipeline (complete end-to-end)")
    print("7. Exit")
    
    choice = input("\nEnter choice (1-7): ").strip()
    
    if choice == "1":
        return start_dagster_ui()
    elif choice == "2":
        return run_pipeline("scraping_pipeline")
    elif choice == "3":
        return run_pipeline("transformation_pipeline")
    elif choice == "4":
        return run_pipeline("enrichment_pipeline")
    elif choice == "5":
        return run_pipeline("api_pipeline")
    elif choice == "6":
        return run_pipeline("full_pipeline")
    elif choice == "7":
        print("Exiting.")
        return True
    else:
        print("Invalid choice.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)