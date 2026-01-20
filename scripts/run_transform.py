"""
Complete Task 2 Execution Pipeline
"""
import subprocess
import sys
from pathlib import Path

def run_step(step_name, command):
    """Run a step with proper logging"""
    print(f"\n{'='*60}")
    print(f"STEP: {step_name}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            capture_output=True,
            text=True
        )
        print(f"✓ {step_name} completed successfully")
        if result.stdout:
            print(f"Output:\n{result.stdout[:500]}")  # Show first 500 chars
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ {step_name} failed")
        print(f"Error: {e.stderr}")
        return False

def main():
    """Main execution function"""
    
    steps = [
        {
            "name": "Start PostgreSQL Database",
            "command": "docker-compose up -d postgres",
            "dir": "."
        },
        {
            "name": "Load Raw Data to PostgreSQL",
            "command": "python src/load_to_postgres.py",
            "dir": "."
        },
        {
            "name": "Initialize dbt Project",
            "command": "dbt debug",
            "dir": "dbt_medical"
        },
        {
            "name": "Run dbt Models",
            "command": "dbt run",
            "dir": "dbt_medical"
        },
        {
            "name": "Run dbt Tests",
            "command": "dbt test",
            "dir": "dbt_medical"
        },
        {
            "name": "Generate dbt Documentation",
            "command": "dbt docs generate",
            "dir": "dbt_medical"
        },
        {
            "name": "Validate Warehouse",
            "command": "python src/validate_warehouse.py",
            "dir": "."
        }
    ]
    
    print("="*60)
    print("TASK 2: DATA MODELING AND TRANSFORMATION PIPELINE")
    print("="*60)
    
    successful_steps = []
    failed_steps = []
    
    for step in steps:
        # Change directory if specified
        original_dir = Path.cwd()
        if "dir" in step:
            try:
                os.chdir(step["dir"])
            except:
                pass
        
        success = run_step(step["name"], step["command"])
        
        # Change back
        os.chdir(original_dir)
        
        if success:
            successful_steps.append(step["name"])
        else:
            failed_steps.append(step["name"])
            # Optionally stop on first failure
            # print("\nPipeline stopped due to failure.")
            # break
    
    # Summary
    print("\n" + "="*60)
    print("PIPELINE EXECUTION SUMMARY")
    print("="*60)
    print(f"Successful steps: {len(successful_steps)}/{len(steps)}")
    print(f"Failed steps: {len(failed_steps)}/{len(steps)}")
    
    if successful_steps:
        print("\n✓ SUCCESSFUL:")
        for step in successful_steps:
            print(f"  - {step}")
    
    if failed_steps:
        print("\n✗ FAILED:")
        for step in failed_steps:
            print(f"  - {step}")
    
    print("\n" + "="*60)
    print("NEXT STEPS:")
    print("1. View dbt documentation: cd dbt_medical && dbt docs serve")
    print("2. Check logs in logs/ directory")
    print("3. Proceed to Task 3: Data Enrichment with YOLO")
    print("="*60)
    
    return len(failed_steps) == 0

if __name__ == "__main__":
    import os
    success = main()
    sys.exit(0 if success else 1)