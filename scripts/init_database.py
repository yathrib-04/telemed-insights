#!/usr/bin/env python3
"""
Database initialization script for Ethiopian Medical Data Platform
"""

import os
import sys
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv
from loguru import logger

# Load environment variables
load_dotenv()

def init_database():
    """Initialize the database with required schemas and tables"""
    
    # Database connection parameters
    db_params = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'password'),
        'database': os.getenv('DB_NAME', 'ethiopian_medical_data')
    }
    
    try:
        # Connect to PostgreSQL
        logger.info("Connecting to PostgreSQL database...")
        conn = psycopg2.connect(**db_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Create raw schema
        logger.info("Creating raw schema...")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        
        # Create telegram_messages table
        logger.info("Creating telegram_messages table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw.telegram_messages (
                id SERIAL PRIMARY KEY,
                message_id BIGINT,
                channel_username VARCHAR(255),
                channel_title VARCHAR(500),
                message_text TEXT,
                message_date TIMESTAMP,
                has_media BOOLEAN DEFAULT FALSE,
                media_type VARCHAR(50),
                media_url TEXT,
                views INTEGER,
                forwards INTEGER,
                replies INTEGER,
                raw_data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Create telegram_images table
        logger.info("Creating telegram_images table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw.telegram_images (
                id SERIAL PRIMARY KEY,
                message_id BIGINT,
                channel_username VARCHAR(255),
                image_url TEXT,
                local_path TEXT,
                image_hash VARCHAR(64),
                object_detection_results JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Create indexes
        logger.info("Creating indexes...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_telegram_messages_channel ON raw.telegram_messages(channel_username);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_telegram_messages_date ON raw.telegram_messages(message_date);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_telegram_messages_has_media ON raw.telegram_messages(has_media);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_telegram_images_channel ON raw.telegram_images(channel_username);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_telegram_images_hash ON raw.telegram_images(image_hash);")
        
        logger.success("Database initialization completed successfully!")
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    init_database() 