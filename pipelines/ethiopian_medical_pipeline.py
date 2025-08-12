#!/usr/bin/env python3
"""
Dagster Pipeline for Ethiopian Medical Data Platform
Orchestrates the entire data workflow from scraping to analytics
"""

import os
import asyncio
from pathlib import Path
from typing import Dict, Any
from dagster import (
    job, op, graph, 
    Config, In, Out, 
    get_dagster_logger, 
    schedule, 
    ScheduleDefinition,
    DefaultScheduleStatus
)
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration for the pipeline
class PipelineConfig(Config):
    telegram_channels: list = ["lobelia4cosmetics", "tikvahpharma", "chemed_telegram"]
    scrape_limit: int = 100
    days_back: int = 7
    dbt_project_dir: str = "dbt"
    data_lake_path: str = "data/raw/telegram_messages"

@op
def scrape_telegram_data(context, config: PipelineConfig) -> Dict[str, Any]:
    """Scrape data from Telegram channels"""
    logger = get_dagster_logger()
    logger.info("Starting Telegram data scraping...")
    
    try:
        # Import and run the scraper
        import sys
        sys.path.append('scripts')
        from telegram_scraper import TelegramScraper
        
        # Run scraper asynchronously
        async def run_scraper():
            scraper = TelegramScraper()
            await scraper.start()
            await scraper.scrape_all_channels(
                limit=config.scrape_limit, 
                days_back=config.days_back
            )
            await scraper.stop()
        
        # Run the async function
        asyncio.run(run_scraper())
        
        logger.info("Telegram data scraping completed successfully")
        return {
            "status": "success",
            "channels_scraped": len(config.telegram_channels),
            "data_lake_path": config.data_lake_path
        }
        
    except Exception as e:
        logger.error(f"Telegram scraping failed: {e}")
        raise

@op
def load_raw_to_postgres(context, config: PipelineConfig) -> Dict[str, Any]:
    """Load raw JSON data from data lake to PostgreSQL"""
    logger = get_dagster_logger()
    logger.info("Starting data loading to PostgreSQL...")
    
    try:
        # Import and run the database initialization
        import sys
        sys.path.append('scripts')
        from init_database import init_database
        
        # Initialize database if needed
        init_database()
        
        logger.info("Data loaded to PostgreSQL successfully")
        return {
            "status": "success",
            "database": "postgresql",
            "schema": "raw"
        }
        
    except Exception as e:
        logger.error(f"Data loading failed: {e}")
        raise

@op
def run_dbt_transformations(context, config: PipelineConfig) -> Dict[str, Any]:
    """Run DBT transformations to create data marts"""
    logger = get_dagster_logger()
    logger.info("Starting DBT transformations...")
    
    try:
        import subprocess
        import os
        
        # Change to DBT project directory
        os.chdir(config.dbt_project_dir)
        
        # Install DBT dependencies
        logger.info("Installing DBT dependencies...")
        subprocess.run(["dbt", "deps"], check=True)
        
        # Run DBT models
        logger.info("Running DBT models...")
        subprocess.run(["dbt", "run"], check=True)
        
        # Run DBT tests
        logger.info("Running DBT tests...")
        subprocess.run(["dbt", "test"], check=True)
        
        # Generate DBT documentation
        logger.info("Generating DBT documentation...")
        subprocess.run(["dbt", "docs", "generate"], check=True)
        
        logger.info("DBT transformations completed successfully")
        return {
            "status": "success",
            "dbt_project": config.dbt_project_dir,
            "models_run": "all",
            "tests_passed": True
        }
        
    except subprocess.CalledProcessError as e:
        logger.error(f"DBT transformation failed: {e}")
        raise
    except Exception as e:
        logger.error(f"DBT transformation failed: {e}")
        raise

@op
def run_yolo_enrichment(context, config: PipelineConfig) -> Dict[str, Any]:
    """Run YOLO object detection on images for data enrichment"""
    logger = get_dagster_logger()
    logger.info("Starting YOLO image enrichment...")
    
    try:
        # Import and run the object detection
        import sys
        sys.path.append('scripts')
        from object_detection import MedicalObjectDetector
        
        # Run object detection asynchronously
        async def run_object_detection():
            detector = MedicalObjectDetector()
            await detector.process_telegram_images(limit=50)
            stats = await detector.get_detection_statistics()
            return stats
        
        # Run the async function
        stats = asyncio.run(run_object_detection())
        
        logger.info("YOLO enrichment completed successfully")
        return {
            "status": "success",
            "images_processed": stats.get('total_images', 0),
            "medical_images_detected": stats.get('medical_images', 0),
            "avg_confidence": stats.get('avg_confidence', 0.0)
        }
        
    except Exception as e:
        logger.error(f"YOLO enrichment failed: {e}")
        raise

@op
def validate_data_quality(context, config: PipelineConfig) -> Dict[str, Any]:
    """Validate data quality and run additional checks"""
    logger = get_dagster_logger()
    logger.info("Starting data quality validation...")
    
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
        
        # Database connection
        db_params = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'password'),
            'database': os.getenv('DB_NAME', 'ethiopian_medical_data')
        }
        
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Run data quality checks
        quality_checks = {}
        
        # Check message count
        cursor.execute("SELECT COUNT(*) as count FROM fct_messages")
        quality_checks['total_messages'] = cursor.fetchone()['count']
        
        # Check medical content percentage
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN is_medical_related THEN 1 END) as medical
            FROM fct_messages
        """)
        result = cursor.fetchone()
        if result['total'] > 0:
            quality_checks['medical_content_percentage'] = round(
                (result['medical'] / result['total']) * 100, 2
            )
        
        # Check image content
        cursor.execute("""
            SELECT COUNT(*) as count 
            FROM fct_messages 
            WHERE has_image = TRUE
        """)
        quality_checks['messages_with_images'] = cursor.fetchone()['count']
        
        conn.close()
        
        logger.info("Data quality validation completed successfully")
        return {
            "status": "success",
            "quality_metrics": quality_checks
        }
        
    except Exception as e:
        logger.error(f"Data quality validation failed: {e}")
        raise

@op
def start_fastapi_server(context, config: PipelineConfig) -> Dict[str, Any]:
    """Start the FastAPI server for analytics"""
    logger = get_dagster_logger()
    logger.info("Starting FastAPI server...")
    
    try:
        # This would typically start the server in a separate process
        # For now, we'll just indicate it's ready
        logger.info("FastAPI server is ready to start")
        return {
            "status": "success",
            "api_server": "ready",
            "endpoint": "http://localhost:8000/docs"
        }
        
    except Exception as e:
        logger.error(f"FastAPI server startup failed: {e}")
        raise

# Define the pipeline graph
@graph
def ethiopian_medical_pipeline():
    """Main pipeline for Ethiopian medical data processing"""
    
    # Run the pipeline steps
    scrape_result = scrape_telegram_data()
    load_result = load_raw_to_postgres()
    dbt_result = run_dbt_transformations()
    yolo_result = run_yolo_enrichment()
    quality_result = validate_data_quality()
    api_result = start_fastapi_server()
    
    # Return results
    return {
        "scraping": scrape_result,
        "loading": load_result,
        "transformation": dbt_result,
        "enrichment": yolo_result,
        "quality": quality_result,
        "api": api_result
    }

# Create the job
@job
def ethiopian_medical_job():
    """Dagster job for Ethiopian medical data platform"""
    return ethiopian_medical_pipeline()

# Define schedules
@schedule(
    cron_schedule="0 2 * * *",  # Daily at 2 AM
    job=ethiopian_medical_job,
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING
)
def daily_data_pipeline():
    """Daily scheduled run of the data pipeline"""
    return {}

@schedule(
    cron_schedule="0 */6 * * *",  # Every 6 hours
    job=ethiopian_medical_job,
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.STOPPED
)
def frequent_data_pipeline():
    """Frequent data pipeline runs for near real-time updates"""
    return {}

# Pipeline configuration
pipeline_config = PipelineConfig(
    telegram_channels=["lobelia4cosmetics", "tikvahpharma", "chemed_telegram"],
    scrape_limit=100,
    days_back=7,
    dbt_project_dir="dbt",
    data_lake_path="data/raw/telegram_messages"
)

if __name__ == "__main__":
    # This allows running the pipeline directly
    from dagster import execute_in_process
    
    result = execute_in_process(
        ethiopian_medical_job,
        run_config={"ops": {"ethiopian_medical_pipeline": {"config": pipeline_config.dict()}}}
    )
    
    print(f"Pipeline execution result: {result.success}")
