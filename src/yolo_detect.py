import os
import cv2
import json
import logging
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
import matplotlib.pyplot as plt
import seaborn as sns

from ultralytics import YOLO
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv('config/.env')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/yolo_detection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class DetectionResult:
    """Data class for YOLO detection results"""
    message_id: int
    channel_name: str
    image_path: str
    detected_objects: List[str]
    confidence_scores: List[float]
    detection_count: int
    image_category: str
    processing_time: float
    model_version: str
    processed_at: datetime


class YOLODetector:
    def __init__(self, model_path: str = 'yolov8n.pt'):
        """
        Initialize YOLO detector
        
        Args:
            model_path: Path to YOLO model weights
        """
        # Model paths
        self.model_path = model_path
        self.model = None
        
        # Base directories
        self.data_dir = Path("data")
        self.raw_images_dir = self.data_dir / "raw" / "images"
        self.processed_dir = self.data_dir / "processed"
        self.detections_dir = self.processed_dir / "detections"
        self.results_dir = self.processed_dir / "results"
        
        # Create directories
        self.detections_dir.mkdir(parents=True, exist_ok=True)
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
        # Database configuration
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'medical_warehouse'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'postgres123')
        }
        
        # YOLO class names for medical relevance
        self.medical_classes = {
            'person': 'person',
            'bottle': 'bottle',
            'cup': 'container',
            'bowl': 'container',
            'handbag': 'accessory',
            'backpack': 'accessory',
            'book': 'document',
            'cell phone': 'device',
            'clock': 'device',
            'chair': 'furniture',
            'couch': 'furniture',
            'bed': 'furniture',
            'dining table': 'furniture',
            'toilet': 'furniture',
            'tv': 'device',
            'laptop': 'device',
            'mouse': 'device',
            'remote': 'device',
            'keyboard': 'device',
            'vase': 'container'
        }
        
        # Classification scheme
        self.classification_scheme = {
            'promotional': ['person', 'bottle', 'cup', 'handbag'],
            'product_display': ['bottle', 'cup', 'bowl', 'vase'],
            'lifestyle': ['person', 'chair', 'couch', 'bed'],
            'other': []
        }
        
        logger.info(f"Initialized YOLODetector with model: {model_path}")
    
    def load_model(self):
        """Load YOLO model"""
        try:
            logger.info(f"Loading YOLO model from {self.model_path}")
            
            # Download model if not exists
            if not Path(self.model_path).exists():
                logger.info("Model not found locally, downloading...")
                self.model = YOLO('yolov8n.pt')
                self.model.save(self.model_path)
            else:
                self.model = YOLO(self.model_path)
            
            logger.info(f"Model loaded successfully: {self.model}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load YOLO model: {e}")
            return False
    
    def find_images(self) -> List[Dict]:
        """Find all images to process from data lake"""
        images = []
        
        try:
            # Get images from database that have not been processed
            engine = self.create_db_engine()
            with engine.connect() as conn:
                query = text("""
                    SELECT 
                        tm.message_id,
                        tm.channel_name,
                        tm.image_path,
                        tm.message_date
                    FROM raw.telegram_messages tm
                    LEFT JOIN marts.fct_image_detections fid 
                        ON tm.message_id = fid.message_id 
                        AND tm.channel_name = fid.channel_name
                    WHERE tm.has_media = TRUE 
                        AND tm.image_path IS NOT NULL
                        AND fid.message_id IS NULL  -- Not already processed
                    ORDER BY tm.message_date DESC
                    LIMIT 1000  -- Process in batches
                """)
                
                result = conn.execute(query)
                rows = result.fetchall()
                
                for row in rows:
                    images.append({
                        'message_id': row[0],
                        'channel_name': row[1],
                        'image_path': row[2],
                        'message_date': row[3]
                    })
            
            logger.info(f"Found {len(images)} images to process")
            return images
            
        except Exception as e:
            logger.error(f"Error finding images: {e}")
            # Fallback: scan directory
            return self.find_images_from_directory()
    
    def find_images_from_directory(self) -> List[Dict]:
        """Fallback: Find images from directory structure"""
        images = []
        
        try:
            image_files = list(self.raw_images_dir.rglob("*.jpg")) + \
                         list(self.raw_images_dir.rglob("*.jpeg")) + \
                         list(self.raw_images_dir.rglob("*.png"))
            
            for img_path in image_files:
                # Extract metadata from path
                channel_name = img_path.parent.name
                
                # Try to extract message_id from filename
                try:
                    message_id = int(img_path.stem)
                except:
                    message_id = hash(str(img_path))
                
                images.append({
                    'message_id': message_id,
                    'channel_name': channel_name,
                    'image_path': str(img_path),
                    'message_date': datetime.now()  # Approximate
                })
            
            logger.info(f"Found {len(images)} images from directory scan")
            return images
            
        except Exception as e:
            logger.error(f"Error scanning directory: {e}")
            return []
    
    def detect_objects(self, image_path: str) -> Tuple[List[str], List[float]]:
        """
        Detect objects in an image using YOLO
        
        Args:
            image_path: Path to image file
            
        Returns:
            Tuple of (detected_objects, confidence_scores)
        """
        try:
            if not Path(image_path).exists():
                logger.warning(f"Image not found: {image_path}")
                return [], []
            
            # Run inference
            results = self.model(image_path, conf=0.25)  # Confidence threshold
            
            detected_objects = []
            confidence_scores = []
            
            if results and len(results) > 0:
                # Get detections for first result
                boxes = results[0].boxes
                
                if boxes is not None and len(boxes) > 0:
                    # Get class names and confidences
                    class_ids = boxes.cls.cpu().numpy().astype(int)
                    confidences = boxes.conf.cpu().numpy()
                    
                    # Convert to class names
                    for class_id, confidence in zip(class_ids, confidences):
                        class_name = self.model.names[class_id]
                        
                        # Only include if in our medical classes
                        if class_name in self.medical_classes:
                            detected_objects.append(class_name)
                            confidence_scores.append(float(confidence))
            
            # Deduplicate while preserving order
            unique_objects = []
            unique_scores = []
            seen = set()
            
            for obj, score in zip(detected_objects, confidence_scores):
                if obj not in seen:
                    seen.add(obj)
                    unique_objects.append(obj)
                    unique_scores.append(score)
            
            return unique_objects, unique_scores
            
        except Exception as e:
            logger.error(f"Error detecting objects in {image_path}: {e}")
            return [], []
    
    def classify_image(self, detected_objects: List[str]) -> str:
        """
        Classify image based on detected objects
        
        Classification Scheme:
        - promotional: Contains person AND product (someone showing/holding item)
        - product_display: Contains bottle/container, no person
        - lifestyle: Contains person, no product
        - other: Neither detected
        
        Args:
            detected_objects: List of detected object names
            
        Returns:
            Image category
        """
        has_person = 'person' in detected_objects
        has_product = any(obj in ['bottle', 'cup', 'bowl', 'vase'] for obj in detected_objects)
        has_container = any(obj in ['bottle', 'cup', 'bowl', 'vase'] for obj in detected_objects)
        
        if has_person and has_product:
            return 'promotional'
        elif has_container and not has_person:
            return 'product_display'
        elif has_person and not has_product:
            return 'lifestyle'
        else:
            return 'other'
    
    def process_image(self, image_info: Dict) -> Optional[DetectionResult]:
        """
        Process a single image
        
        Args:
            image_info: Dictionary with image metadata
            
        Returns:
            DetectionResult object or None if failed
        """
        start_time = datetime.now()
        
        try:
            message_id = image_info['message_id']
            channel_name = image_info['channel_name']
            image_path = image_info['image_path']
            
            logger.info(f"Processing image: {image_path}")
            
            # Detect objects
            detected_objects, confidence_scores = self.detect_objects(image_path)
            
            if not detected_objects:
                logger.warning(f"No objects detected in {image_path}")
                # Still create result for tracking
                detected_objects = ['no_detection']
                confidence_scores = [0.0]
            
            # Classify image
            image_category = self.classify_image(detected_objects)
            
            # Calculate processing time
            processing_time = (datetime.now() - start_time).total_seconds()
            
            # Create result object
            result = DetectionResult(
                message_id=message_id,
                channel_name=channel_name,
                image_path=image_path,
                detected_objects=detected_objects,
                confidence_scores=confidence_scores,
                detection_count=len(detected_objects),
                image_category=image_category,
                processing_time=processing_time,
                model_version='yolov8n',
                processed_at=datetime.now()
            )
            
            # Save visualization
            self.save_detection_visualization(result, image_path)
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing image {image_info.get('image_path', 'unknown')}: {e}")
            return None
    
    def save_detection_visualization(self, result: DetectionResult, image_path: str):
        """Save visualization of detections"""
        try:
            if not Path(image_path).exists():
                return
            
            # Load image
            img = cv2.imread(image_path)
            if img is None:
                return
            
            # Create visualization directory
            vis_dir = self.detections_dir / result.channel_name
            vis_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate visualization filename
            vis_filename = f"{result.message_id}_detection.jpg"
            vis_path = vis_dir / vis_filename
            
            # For now, just copy the image (in full implementation, draw boxes)
            # In a real implementation, you would use YOLO's plotting functionality
            cv2.imwrite(str(vis_path), img)
            
            # Save detection metadata
            meta_path = vis_dir / f"{result.message_id}_metadata.json"
            with open(meta_path, 'w') as f:
                json.dump({
                    'message_id': result.message_id,
                    'detected_objects': result.detected_objects,
                    'confidence_scores': result.confidence_scores,
                    'image_category': result.image_category,
                    'processed_at': result.processed_at.isoformat()
                }, f, indent=2, default=str)
            
            logger.debug(f"Saved visualization to {vis_path}")
            
        except Exception as e:
            logger.warning(f"Failed to save visualization: {e}")
    
    def create_db_engine(self):
        """Create database engine"""
        connection_string = (
            f"postgresql://{self.db_config['user']}:{self.db_config['password']}"
            f"@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
        )
        return create_engine(connection_string)
    
    def save_results_to_csv(self, results: List[DetectionResult]):
        """Save results to CSV file"""
        if not results:
            logger.warning("No results to save")
            return
        
        # Convert to DataFrame
        data = []
        for result in results:
            data.append({
                'message_id': result.message_id,
                'channel_name': result.channel_name,
                'image_path': result.image_path,
                'detected_objects': ','.join(result.detected_objects),
                'confidence_scores': ','.join(map(str, result.confidence_scores)),
                'detection_count': result.detection_count,
                'image_category': result.image_category,
                'processing_time': result.processing_time,
                'model_version': result.model_version,
                'processed_at': result.processed_at
            })
        
        df = pd.DataFrame(data)
        
        # Save to CSV
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_path = self.results_dir / f"detection_results_{timestamp}.csv"
        df.to_csv(csv_path, index=False)
        
        logger.info(f"Saved {len(results)} results to {csv_path}")
        return csv_path
    
    def load_results_to_postgres(self, results: List[DetectionResult]):
        """Load detection results to PostgreSQL"""
        if not results:
            logger.warning("No results to load to database")
            return 0
        
        try:
            engine = self.create_db_engine()
            
            with engine.begin() as conn:
                # Create table if not exists
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS raw.image_detections (
                    id SERIAL PRIMARY KEY,
                    message_id INTEGER NOT NULL,
                    channel_name VARCHAR(255) NOT NULL,
                    image_path VARCHAR(500),
                    detected_objects TEXT[],
                    confidence_scores FLOAT[],
                    detection_count INTEGER,
                    image_category VARCHAR(50),
                    processing_time FLOAT,
                    model_version VARCHAR(50),
                    processed_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(message_id, channel_name)
                );
                """
                conn.execute(text(create_table_sql))
                
                # Insert results
                inserted_count = 0
                for result in results:
                    try:
                        insert_sql = text("""
                        INSERT INTO raw.image_detections 
                        (message_id, channel_name, image_path, detected_objects, 
                         confidence_scores, detection_count, image_category,
                         processing_time, model_version, processed_at)
                        VALUES (:message_id, :channel_name, :image_path, 
                                :detected_objects, :confidence_scores, :detection_count,
                                :image_category, :processing_time, :model_version, :processed_at)
                        ON CONFLICT (message_id, channel_name) 
                        DO UPDATE SET
                            detected_objects = EXCLUDED.detected_objects,
                            confidence_scores = EXCLUDED.confidence_scores,
                            image_category = EXCLUDED.image_category,
                            processing_time = EXCLUDED.processing_time,
                            processed_at = EXCLUDED.processed_at
                        """)
                        
                        conn.execute(insert_sql, {
                            'message_id': result.message_id,
                            'channel_name': result.channel_name,
                            'image_path': result.image_path,
                            'detected_objects': result.detected_objects,
                            'confidence_scores': result.confidence_scores,
                            'detection_count': result.detection_count,
                            'image_category': result.image_category,
                            'processing_time': result.processing_time,
                            'model_version': result.model_version,
                            'processed_at': result.processed_at
                        })
                        inserted_count += 1
                        
                    except Exception as e:
                        logger.error(f"Failed to insert result for message {result.message_id}: {e}")
                        continue
                
                logger.info(f"Loaded {inserted_count} detection results to database")
                return inserted_count
                
        except Exception as e:
            logger.error(f"Error loading results to database: {e}")
            return 0
    
    def generate_analysis_report(self, results: List[DetectionResult]):
        """Generate analysis report from detection results"""
        if not results:
            logger.warning("No results for analysis report")
            return None
        
        try:
            # Convert to DataFrame for analysis
            data = []
            for result in results:
                data.append({
                    'channel_name': result.channel_name,
                    'image_category': result.image_category,
                    'detection_count': result.detection_count,
                    'has_person': 'person' in result.detected_objects,
                    'has_product': any(obj in ['bottle', 'cup', 'bowl'] for obj in result.detected_objects),
                    'processing_time': result.processing_time
                })
            
            df = pd.DataFrame(data)
            
            # Analysis calculations
            report = {
                'timestamp': datetime.now().isoformat(),
                'total_images_processed': len(results),
                'image_category_distribution': df['image_category'].value_counts().to_dict(),
                'detection_statistics': {
                    'avg_detections_per_image': df['detection_count'].mean(),
                    'max_detections': df['detection_count'].max(),
                    'min_detections': df['detection_count'].min()
                },
                'channel_analysis': {},
                'category_insights': {}
            }
            
            # Channel-level analysis
            for channel in df['channel_name'].unique():
                channel_data = df[df['channel_name'] == channel]
                report['channel_analysis'][channel] = {
                    'image_count': len(channel_data),
                    'category_distribution': channel_data['image_category'].value_counts().to_dict(),
                    'person_percentage': (channel_data['has_person'].sum() / len(channel_data)) * 100,
                    'product_percentage': (channel_data['has_product'].sum() / len(channel_data)) * 100
                }
            
            # Category insights
            for category in df['image_category'].unique():
                category_data = df[df['image_category'] == category]
                report['category_insights'][category] = {
                    'count': len(category_data),
                    'channels': category_data['channel_name'].value_counts().to_dict(),
                    'avg_detections': category_data['detection_count'].mean()
                }
            
            # Save report
            report_path = self.results_dir / f"analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            
            # Generate visualizations
            self.generate_visualizations(df, report_path.parent)
            
            logger.info(f"Generated analysis report: {report_path}")
            return report_path
            
        except Exception as e:
            logger.error(f"Error generating analysis report: {e}")
            return None
    
    def generate_visualizations(self, df: pd.DataFrame, output_dir: Path):
        """Generate visualization charts"""
        try:
            # Set style
            plt.style.use('seaborn-v0_8-darkgrid')
            
            # 1. Image Category Distribution
            plt.figure(figsize=(10, 6))
            category_counts = df['image_category'].value_counts()
            colors = plt.cm.Set3(np.arange(len(category_counts)))
            plt.pie(category_counts.values, labels=category_counts.index, autopct='%1.1f%%', colors=colors)
            plt.title('Distribution of Image Categories')
            plt.savefig(output_dir / 'category_distribution.png', dpi=150, bbox_inches='tight')
            plt.close()
            
            # 2. Detections per Channel
            plt.figure(figsize=(12, 6))
            channel_detections = df.groupby('channel_name')['detection_count'].mean().sort_values()
            channel_detections.plot(kind='bar', color='skyblue')
            plt.title('Average Detections per Image by Channel')
            plt.xlabel('Channel')
            plt.ylabel('Average Detections')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig(output_dir / 'detections_by_channel.png', dpi=150)
            plt.close()
            
            # 3. Category Distribution by Channel
            plt.figure(figsize=(14, 8))
            pivot = pd.crosstab(df['channel_name'], df['image_category'], normalize='index') * 100
            pivot.plot(kind='bar', stacked=True, colormap='Set2', figsize=(14, 8))
            plt.title('Image Category Distribution by Channel (%)')
            plt.xlabel('Channel')
            plt.ylabel('Percentage')
            plt.legend(title='Category', bbox_to_anchor=(1.05, 1), loc='upper left')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig(output_dir / 'category_by_channel.png', dpi=150)
            plt.close()
            
            logger.info("Generated visualization charts")
            
        except Exception as e:
            logger.warning(f"Could not generate all visualizations: {e}")
    
    def run(self, batch_size: int = 100):
        """Main execution method"""
        logger.info("Starting YOLO object detection pipeline")
        
        # Load model
        if not self.load_model():
            logger.error("Failed to load YOLO model")
            return
        
        # Find images to process
        images = self.find_images()
        
        if not images:
            logger.info("No images to process")
            return
        
        logger.info(f"Processing {len(images)} images")
        
        # Process in batches
        all_results = []
        for i in range(0, len(images), batch_size):
            batch = images[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(images)-1)//batch_size + 1}")
            
            batch_results = []
            for image_info in batch:
                result = self.process_image(image_info)
                if result:
                    batch_results.append(result)
            
            # Save batch results
            if batch_results:
                self.save_results_to_csv(batch_results)
                self.load_results_to_postgres(batch_results)
                all_results.extend(batch_results)
            
            logger.info(f"Processed {len(batch_results)} images in this batch")
        
        # Generate analysis report
        if all_results:
            report_path = self.generate_analysis_report(all_results)
            
            # Print summary
            self.print_summary(all_results, report_path)
        
        logger.info("YOLO detection pipeline completed")
    
    def print_summary(self, results: List[DetectionResult], report_path: Optional[Path] = None):
        """Print execution summary"""
        print("\n" + "="*60)
        print("YOLO DETECTION PIPELINE - EXECUTION SUMMARY")
        print("="*60)
        
        if not results:
            print("✗ No images processed")
            return
        
        df = pd.DataFrame([{
            'category': r.image_category,
            'detections': r.detection_count,
            'channel': r.channel_name
        } for r in results])
        
        print(f"✓ Total Images Processed: {len(results)}")
        print(f"✓ Image Categories:")
        for category, count in df['category'].value_counts().items():
            percentage = (count / len(results)) * 100
            print(f"    - {category}: {count} ({percentage:.1f}%)")
        
        print(f"✓ Average Detections per Image: {df['detections'].mean():.2f}")
        print(f"✓ Unique Channels: {df['channel'].nunique()}")
        
        if report_path:
            print(f"✓ Analysis Report: {report_path}")
        
        print("="*60)


def main():
    """Main function to run YOLO detector"""
    # Create logs directory
    Path("logs").mkdir(exist_ok=True)
    
    # Initialize and run detector
    detector = YOLODetector(model_path='models/yolov8n.pt')
    detector.run(batch_size=50)


if __name__ == "__main__":
    main()