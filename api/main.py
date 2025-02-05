import cv2
import os
import psycopg2
import torch
from datetime import datetime
from psycopg2.extras import Json  # Required for JSONB serialization

# Load YOLOv5 model
model = torch.hub.load('ultralytics/yolov5', 'yolov5s')

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="telegram_scrapped",
    user="postgres",
    password="root",
    host="localhost"
)
cursor = conn.cursor()

# Fix 1: Update the image_id column type to VARCHAR
cursor.execute('''
    CREATE TABLE IF NOT EXISTS detections (
        id SERIAL PRIMARY KEY,
        image_id VARCHAR(255),  -- Changed from INTEGER to VARCHAR
        class VARCHAR(50),
        confidence FLOAT,
        bbox JSONB,
        detected_at TIMESTAMP
    )
''')

# Process images recursively
image_dir = "images"
for root, _, files in os.walk(image_dir):
    for file_name in files:
        image_path = os.path.join(root, file_name)
        
        # Skip non-image files
        if not file_name.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp', '.gif')):
            print(f"Skipping non-image file: {image_path}")
            continue
        
        # Load and process the image
        img = cv2.imread(image_path)
        if img is None:
            print(f"Failed to load image: {image_path}")
            continue
        
        results = model(img)
        detections = results.pandas().xyxy[0]
        
        for _, det in detections.iterrows():
            # Fix 2: Use Json() wrapper for the dictionary
            cursor.execute('''
                INSERT INTO detections (image_id, class, confidence, bbox, detected_at)
                VALUES (%s, %s, %s, %s, %s)
            ''', (
                file_name.split('.')[0],  # This is now VARCHAR
                det['name'], 
                det['confidence'], 
                Json({'x1': det['xmin'], 'y1': det['ymin'], 'x2': det['xmax'], 'y2': det['ymax']}),  # Wrap with Json()
                datetime.now()
            ))
        conn.commit()

# Close the database connection
cursor.close()
conn.close()