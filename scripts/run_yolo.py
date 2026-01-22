"""
Task 3: Data Enrichment with Object Detection - Complete Pipeline
"""
import subprocess
import sys
import time
from pathlib import Path
from datetime import datetime

def run_step(step_name, command, cwd=None):
    """Run a step with logging"""
    print(f"\n{'='*60}")
    print(f"STEP: {step_name}")
    print(f"{'='*60}")
    
    start_time = time.time()
    
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            capture_output=True,
            text=True,
            cwd=cwd
        )
        
        elapsed = time.time() - start_time
        
        print(f"✓ {step_name} completed successfully ({elapsed:.1f}s)")
        if result.stdout and len(result.stdout) > 0:
            # Show last 10 lines of output
            lines = result.stdout.strip().split('\n')
            if len(lines) > 10:
                print("...")
                for line in lines[-10:]:
                    if line.strip():
                        print(f"  {line}")
            else:
                for line in lines:
                    if line.strip():
                        print(f"  {line}")
        
        return True, elapsed
        
    except subprocess.CalledProcessError as e:
        elapsed = time.time() - start_time
        print(f"✗ {step_name} failed after {elapsed:.1f}s")
        print(f"Error: {e.stderr}")
        return False, elapsed

def check_prerequisites():
    """Check if all prerequisites are met"""
    print("\n" + "="*60)
    print("PREREQUISITE CHECK")
    print("="*60)
    
    checks = []
    
    # Check Python packages
    try:
        import ultralytics
        checks.append(("✓ ultralytics", True))
    except:
        checks.append(("✗ ultralytics - Install: pip install ultralytics", False))
    
    try:
        import cv2
        checks.append(("✓ opencv-python", True))
    except:
        checks.append(("✗ opencv-python - Install: pip install opencv-python", False))
    
    # Check YOLO model
    model_path = Path("models/yolov8n.pt")
    if model_path.exists():
        checks.append((f"✓ YOLO model found: {model_path}", True))
    else:
        checks.append(("✗ YOLO model not found - Will download automatically", True))
    
    # Check data directory
    data_dir = Path("data/raw/images")
    if data_dir.exists() and any(data_dir.rglob("*.jpg")):
        checks.append(("✓ Images found in data directory", True))
    else:
        checks.append(("⚠ No images found - Run Task 1 first", False))
    
    # Print results
    all_ok = True
    for check_name, status in checks:
        print(check_name)
        if not status:
            all_ok = False
    
    return all_ok

def main():
    """Main execution function"""
    
    print("="*60)
    print("TASK 3: DATA ENRICHMENT WITH YOLO OBJECT DETECTION")
    print("="*60)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check prerequisites
    if not check_prerequisites():
        print("\n⚠ Some prerequisites missing. Continue anyway? (y/n)")
        if input().lower() != 'y':
            print("Exiting.")
            sys.exit(1)
    
    steps = [
        {
            "name": "Run YOLO Object Detection",
            "command": "python src/yolo_detect.py",
            "cwd": "."
        },
        {
            "name": "Run dbt Models for Image Detections",
            "command": "dbt run --select +fct_image_detections",
            "cwd": "dbt_medical"
        },
        {
            "name": "Test dbt Models",
            "command": "dbt test --select fct_image_detections",
            "cwd": "dbt_medical"
        },
        {
            "name": "Generate Business Analysis",
            "command": "python src/analyze_detections.py",
            "cwd": "."
        },
        {
            "name": "Generate dbt Documentation",
            "command": "dbt docs generate",
            "cwd": "dbt_medical"
        }
    ]
    
    successful_steps = []
    failed_steps = []
    total_time = 0
    
    for step in steps:
        success, elapsed = run_step(
            step["name"], 
            step["command"], 
            step.get("cwd", ".")
        )
        
        total_time += elapsed
        
        if success:
            successful_steps.append(step["name"])
        else:
            failed_steps.append(step["name"])
            # Ask if should continue
            print("\n⚠ Step failed. Continue with next steps? (y/n)")
            if input().lower() != 'y':
                break
    
    # Summary
    print("\n" + "="*60)
    print("EXECUTION SUMMARY")
    print("="*60)
    print(f"Total Time: {total_time:.1f}s")
    print(f"Successful: {len(successful_steps)}/{len(steps)}")
    print(f"Failed: {len(failed_steps)}/{len(steps)}")
    
    if successful_steps:
        print("\n✓ COMPLETED STEPS:")
        for step in successful_steps:
            print(f"  - {step}")
    
    if failed_steps:
        print("\n✗ FAILED STEPS:")
        for step in failed_steps:
            print(f"  - {step}")
    
    # Next steps
    print("\n" + "="*60)
    print("NEXT ACTIONS")
    print("="*60)
    print("1. View analysis results in:")
    print("   - data/processed/results/")
    print("   - logs/yolo_detection.log")
    print("\n2. Access dbt documentation:")
    print("   cd dbt_medical && dbt docs serve --port 8080")
    print("\n3. View generated visualizations:")
    print("   Open PNG files in data/processed/results/")
    print("\n4. Proceed to Task 4: Build Analytical API")
    print("="*60)
    
    return len(failed_steps) == 0

if __name__ == "__main__":
    import os
    
    # Create necessary directories
    Path("logs").mkdir(exist_ok=True)
    Path("models").mkdir(exist_ok=True)
    Path("data/processed/results").mkdir(parents=True, exist_ok=True)
    
    success = main()
    sys.exit(0 if success else 1)