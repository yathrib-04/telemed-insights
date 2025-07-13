#!/usr/bin/env python3
"""
Object Detection Script for Ethiopian Medical Data Platform
Uses YOLO to detect medical products in Telegram images
"""

import os
import json
import hashlib
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Optional
import psycopg2
from ultralytics import YOLO
from PIL import Image
import requests
from dotenv import load_dotenv
from loguru import logger

# Load environment variables
load_dotenv()

class MedicalObjectDetector:
    """YOLO-based object detector for medical products"""
    
    def __init__(self):
        self.model_path = os.getenv('YOLO_MODEL_PATH', 'models/yolo_model.pt')
        self.confidence_threshold = float(os.getenv('CONFIDENCE_THRESHOLD', 0.5))
        
        # Database connection
        self.db_params = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'password'),
            'database': os.getenv('DB_NAME', 'ethiopian_medical_data')
        }
        
        # Initialize YOLO model
        self.model = self._load_model()
        
        # Medical product classes
        self.medical_classes = [
            'pill', 'tablet', 'capsule', 'medicine', 'drug', 
            'bottle', 'syringe', 'cream', 'ointment', 'injection'
        ]
    
    def _load_model(self) -> YOLO:
        """Load YOLO model"""
        try:
            if os.path.exists(self.model_path):
                logger.info(f"Loading existing model from {self.model_path}")
                return YOLO(self.model_path)
            else:
                logger.info("Loading pre-trained YOLO model")
                # Use a pre-trained model and fine-tune for medical products
                model = YOLO('yolov8n.pt')  # Start with nano model
                return model
        except Exception as e:
            logger.error(f"Error loading YOLO model: {e}")
            raise
    
    def _download_image(self, image_url: str, local_path: str) -> bool:
        """Download image from URL"""
        try:
            response = requests.get(image_url, timeout=30)
            response.raise_for_status()
            
            with open(local_path, 'wb') as f:
                f.write(response.content)
            
            return True
        except Exception as e:
            logger.error(f"Error downloading image {image_url}: {e}")
            return False
    
    def _calculate_image_hash(self, image_path: str) -> str:
        """Calculate SHA-256 hash of image file"""
        try:
            with open(image_path, 'rb') as f:
                return hashlib.sha256(f.read()).hexdigest()
        except Exception as e:
            logger.error(f"Error calculating image hash: {e}")
            return ""
    
    def detect_objects(self, image_path: str) -> List[Dict[str, Any]]:
        """Detect objects in image using YOLO"""
        try:
            # Run inference
            results = self.model(image_path, conf=self.confidence_threshold)
            
            detections = []
            for result in results:
                boxes = result.boxes
                if boxes is not None:
                    for box in boxes:
                        # Get box coordinates
                        x1, y1, x2, y2 = box.xyxy[0].cpu().numpy()
                        
                        # Get confidence and class
                        confidence = float(box.conf[0].cpu().numpy())
                        class_id = int(box.cls[0].cpu().numpy())
                        class_name = result.names[class_id]
                        
                        detection = {
                            'class': class_name,
                            'confidence': confidence,
                            'bbox': {
                                'x1': float(x1),
                                'y1': float(y1),
                                'x2': float(x2),
                                'y2': float(y2)
                            },
                            'area': float((x2 - x1) * (y2 - y1))
                        }
                        
                        detections.append(detection)
            
            return detections
            
        except Exception as e:
            logger.error(f"Error detecting objects in {image_path}: {e}")
            return []
    
    def is_medical_product(self, detections: List[Dict[str, Any]]) -> bool:
        """Check if any detected objects are medical products"""
        for detection in detections:
            if detection['class'].lower() in self.medical_classes:
                return True
        return False
    
    async def process_telegram_images(self, limit: int = 100):
        """Process images from telegram_messages table"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            
            # Get images that haven't been processed yet
            query = """
                SELECT DISTINCT 
                    tm.message_id,
                    tm.channel_username,
                    tm.media_url
                FROM raw.telegram_messages tm
                LEFT JOIN raw.telegram_images ti 
                    ON tm.message_id = ti.message_id 
                    AND tm.channel_username = ti.channel_username
                WHERE tm.has_media = TRUE 
                  AND tm.media_type = 'photo'
                  AND tm.media_url IS NOT NULL
                  AND ti.id IS NULL
                LIMIT %s
            """
            
            cursor.execute(query, (limit,))
            images = cursor.fetchall()
            
            logger.info(f"Found {len(images)} images to process")
            
            # Create images directory
            images_dir = Path('data/processed/images')
            images_dir.mkdir(parents=True, exist_ok=True)
            
            processed_count = 0
            for message_id, channel_username, media_url in images:
                try:
                    # Create local file path
                    image_filename = f"{channel_username}_{message_id}.jpg"
                    local_path = images_dir / image_filename
                    
                    # Download image
                    if not self._download_image(media_url, str(local_path)):
                        continue
                    
                    # Calculate image hash
                    image_hash = self._calculate_image_hash(str(local_path))
                    if not image_hash:
                        continue
                    
                    # Detect objects
                    detections = self.detect_objects(str(local_path))
                    
                    # Check if medical products detected
                    has_medical_objects = self.is_medical_product(detections)
                    
                    # Save results to database
                    cursor.execute("""
                        INSERT INTO raw.telegram_images 
                        (message_id, channel_username, image_url, local_path, image_hash, object_detection_results)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (message_id, channel_username) DO NOTHING
                    """, (
                        message_id,
                        channel_username,
                        media_url,
                        str(local_path),
                        image_hash,
                        json.dumps(detections)
                    ))
                    
                    processed_count += 1
                    
                    if processed_count % 10 == 0:
                        logger.info(f"Processed {processed_count} images")
                    
                except Exception as e:
                    logger.error(f"Error processing image {message_id}: {e}")
                    continue
            
            conn.commit()
            logger.success(f"Successfully processed {processed_count} images")
            
        except Exception as e:
            logger.error(f"Error in process_telegram_images: {e}")
            if 'conn' in locals():
                conn.rollback()
        finally:
            if 'conn' in locals():
                conn.close()
    
    async def get_detection_statistics(self) -> Dict[str, Any]:
        """Get statistics about object detection results"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            
            query = """
                SELECT 
                    COUNT(*) as total_images,
                    COUNT(CASE WHEN has_medical_objects THEN 1 END) as medical_images,
                    AVG(detected_objects_count) as avg_objects_per_image,
                    AVG(avg_confidence_score) as avg_confidence,
                    COUNT(DISTINCT channel_username) as channels_with_images
                FROM stg_telegram_images
                WHERE detected_objects_count > 0
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            
            stats = {
                'total_images': result[0],
                'medical_images': result[1],
                'avg_objects_per_image': float(result[2]) if result[2] else 0.0,
                'avg_confidence': float(result[3]) if result[3] else 0.0,
                'channels_with_images': result[4],
                'medical_percentage': round((result[1] / result[0]) * 100, 2) if result[0] > 0 else 0
            }
            
            conn.close()
            return stats
            
        except Exception as e:
            logger.error(f"Error getting detection statistics: {e}")
            return {}

async def main():
    """Main function to run object detection"""
    detector = MedicalObjectDetector()
    
    # Process images
    await detector.process_telegram_images(limit=50)
    
    # Get statistics
    stats = await detector.get_detection_statistics()
    logger.info("Object Detection Statistics:")
    for key, value in stats.items():
        logger.info(f"  {key}: {value}")

if __name__ == "__main__":
    asyncio.run(main()) 