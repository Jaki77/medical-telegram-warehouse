"""
Quick test script for YOLO functionality
"""
from ultralytics import YOLO
import cv2
import numpy as np
from pathlib import Path

def test_yolo_basic():
    """Test basic YOLO functionality"""
    print("Testing YOLO installation...")
    
    try:
        # Create a test image
        test_img = np.random.randint(0, 255, (640, 640, 3), dtype=np.uint8)
        
        # Save test image
        test_path = Path("test_image.jpg")
        cv2.imwrite(str(test_path), test_img)
        
        # Load model
        print("Loading YOLO model...")
        model = YOLO('yolov8n.pt')
        
        # Run inference
        print("Running inference...")
        results = model(test_path, conf=0.25)
        
        print(f"✓ YOLO test successful")
        print(f"  Model: {model}")
        print(f"  Results: {len(results)} detection set(s)")
        
        if results and len(results) > 0:
            boxes = results[0].boxes
            if boxes is not None:
                print(f"  Detections: {len(boxes)} objects")
        
        # Clean up
        test_path.unlink(missing_ok=True)
        
        return True
        
    except Exception as e:
        print(f"✗ YOLO test failed: {e}")
        return False

if __name__ == "__main__":
    print("="*50)
    print("YOLO QUICK TEST")
    print("="*50)
    
    success = test_yolo_basic()
    
    print("\n" + "="*50)
    if success:
        print("✓ YOLO is correctly installed and working")
        print("  You can proceed with Task 3")
    else:
        print("✗ YOLO installation has issues")
        print("  Check: pip install ultralytics opencv-python")
    print("="*50)