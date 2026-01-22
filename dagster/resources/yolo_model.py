"""
YOLO model resources for Dagster pipeline
"""
from dagster import resource, Field, String
from pathlib import Path

@resource(
    config_schema={
        "model_path": Field(String, default_value="models/yolov8n.pt", is_required=False),
        "confidence_threshold": Field(float, default_value=0.25, is_required=False),
        "device": Field(String, default_value="cpu", is_required=False),  # cpu or cuda
    }
)
def yolo_resource(context):
    """Resource for YOLO model operations"""
    
    class YOLOModel:
        def __init__(self, config: dict):
            self.model_path = config["model_path"]
            self.confidence_threshold = config["confidence_threshold"]
            self.device = config["device"]
            self.context = context
            self.model = None
        
        def load_model(self):
            """Load YOLO model"""
            try:
                from ultralytics import YOLO
                
                if not Path(self.model_path).exists():
                    self.context.log.info(f"Model not found at {self.model_path}, downloading...")
                    self.model = YOLO('yolov8n.pt')
                    self.model.save(self.model_path)
                else:
                    self.model = YOLO(self.model_path)
                
                self.context.log.info(f"YOLO model loaded from {self.model_path}")
                return True
            except Exception as e:
                self.context.log.error(f"Failed to load YOLO model: {e}")
                return False
        
        def get_config(self) -> dict:
            """Get model configuration"""
            return {
                "model_path": self.model_path,
                "confidence_threshold": self.confidence_threshold,
                "device": self.device,
                "loaded": self.model is not None
            }
    
    return YOLOModel(context.resource_config)