#!/usr/bin/env python3
"""
FastAPI application for Ethiopian Medical Data Platform
Provides analytical endpoints for medical business insights
"""

import os
import psycopg2
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from loguru import logger

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI(
    title="Ethiopian Medical Data Platform API",
    description="Analytical API for Ethiopian medical business insights from Telegram data",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection
def get_db_connection():
    """Get database connection"""
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', 'password'),
        database=os.getenv('DB_NAME', 'ethiopian_medical_data')
    )

# Pydantic models for API responses
class ChannelInsight(BaseModel):
    channel_username: str
    channel_title: str
    total_messages: int
    medical_messages: int
    medical_percentage: float
    avg_engagement_score: float
    messages_with_images: int
    image_percentage: float
    medical_focus_level: str
    visual_content_level: str

class ProductInsight(BaseModel):
    product_name: str
    mention_count: int
    channels_mentioned: int
    avg_engagement: float
    first_mention_date: str
    last_mention_date: str

class TrendData(BaseModel):
    date: str
    message_count: int
    medical_message_count: int
    image_message_count: int
    avg_engagement_score: float

class ImageAnalysis(BaseModel):
    channel_username: str
    total_images: int
    medical_images: int
    medical_percentage: float
    avg_objects_detected: float
    avg_confidence_score: float
    top_detected_classes: List[str]

# API Endpoints

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Ethiopian Medical Data Platform API",
        "version": "1.0.0",
        "description": "Analytical API for Ethiopian medical business insights",
        "endpoints": {
            "channels": "/api/v1/channels",
            "products": "/api/v1/products",
            "trends": "/api/v1/trends",
            "images": "/api/v1/images",
            "health": "/health"
        }
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

@app.get("/api/v1/channels", response_model=List[ChannelInsight])
async def get_channel_insights(
    limit: int = Query(10, description="Number of channels to return"),
    sort_by: str = Query("total_messages", description="Sort by: total_messages, medical_messages, avg_engagement_score")
):
    """Get insights about Telegram channels"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Build query based on sort parameter
        sort_mapping = {
            "total_messages": "total_messages DESC",
            "medical_messages": "medical_messages DESC",
            "avg_engagement_score": "avg_engagement_score DESC",
            "medical_percentage": "medical_content_percentage DESC",
            "image_percentage": "image_content_percentage DESC"
        }
        
        sort_clause = sort_mapping.get(sort_by, "total_messages DESC")
        
        query = f"""
            SELECT 
                channel_username,
                channel_title,
                total_messages,
                medical_messages,
                medical_content_percentage,
                avg_engagement_score,
                messages_with_images,
                image_content_percentage,
                medical_focus_level,
                visual_content_level
            FROM dim_channels
            ORDER BY {sort_clause}
            LIMIT %s
        """
        
        cursor.execute(query, (limit,))
        results = cursor.fetchall()
        
        channels = []
        for row in results:
            channels.append(ChannelInsight(
                channel_username=row[0],
                channel_title=row[1] or "Unknown",
                total_messages=row[2],
                medical_messages=row[3],
                medical_percentage=float(row[4]) if row[4] else 0.0,
                avg_engagement_score=float(row[5]) if row[5] else 0.0,
                messages_with_images=row[6],
                image_percentage=float(row[7]) if row[7] else 0.0,
                medical_focus_level=row[8],
                visual_content_level=row[9]
            ))
        
        conn.close()
        return channels
        
    except Exception as e:
        logger.error(f"Error fetching channel insights: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch channel insights")

@app.get("/api/v1/products", response_model=List[ProductInsight])
async def get_product_insights(
    limit: int = Query(10, description="Number of products to return"),
    min_mentions: int = Query(1, description="Minimum number of mentions")
):
    """Get insights about medical products mentioned across channels"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Extract product mentions from message text
        query = """
            WITH product_mentions AS (
                SELECT 
                    LOWER(unnest(regexp_matches(message_text, '\b(medicine|drug|pill|tablet|capsule|syrup|cream|ointment|injection|vaccine)\b', 'gi'))) as product_name,
                    channel_username,
                    message_date,
                    engagement_score
                FROM fct_messages
                WHERE is_medical_related = TRUE
                  AND message_text IS NOT NULL
            ),
            product_stats AS (
                SELECT 
                    product_name,
                    COUNT(*) as mention_count,
                    COUNT(DISTINCT channel_username) as channels_mentioned,
                    AVG(engagement_score) as avg_engagement,
                    MIN(message_date) as first_mention_date,
                    MAX(message_date) as last_mention_date
                FROM product_mentions
                GROUP BY product_name
                HAVING COUNT(*) >= %s
            )
            SELECT 
                product_name,
                mention_count,
                channels_mentioned,
                avg_engagement,
                first_mention_date::text,
                last_mention_date::text
            FROM product_stats
            ORDER BY mention_count DESC
            LIMIT %s
        """
        
        cursor.execute(query, (min_mentions, limit))
        results = cursor.fetchall()
        
        products = []
        for row in results:
            products.append(ProductInsight(
                product_name=row[0].title(),
                mention_count=row[1],
                channels_mentioned=row[2],
                avg_engagement=float(row[3]) if row[3] else 0.0,
                first_mention_date=row[4],
                last_mention_date=row[5]
            ))
        
        conn.close()
        return products
        
    except Exception as e:
        logger.error(f"Error fetching product insights: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch product insights")

@app.get("/api/v1/trends", response_model=List[TrendData])
async def get_trends(
    days: int = Query(30, description="Number of days to analyze"),
    trend_type: str = Query("daily", description="Trend type: daily, weekly, monthly")
):
    """Get posting trends over time"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Build query based on trend type
        if trend_type == "weekly":
            group_by = "DATE_TRUNC('week', message_date_only)"
            date_format = "DATE_TRUNC('week', message_date_only)::text"
        elif trend_type == "monthly":
            group_by = "DATE_TRUNC('month', message_date_only)"
            date_format = "DATE_TRUNC('month', message_date_only)::text"
        else:  # daily
            group_by = "message_date_only"
            date_format = "message_date_only::text"
        
        query = f"""
            SELECT 
                {date_format} as date,
                COUNT(*) as message_count,
                COUNT(CASE WHEN is_medical_related THEN 1 END) as medical_message_count,
                COUNT(CASE WHEN has_image THEN 1 END) as image_message_count,
                AVG(engagement_score) as avg_engagement_score
            FROM fct_messages
            WHERE message_date_only >= CURRENT_DATE - INTERVAL '{days} days'
            GROUP BY {group_by}
            ORDER BY {group_by}
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        trends = []
        for row in results:
            trends.append(TrendData(
                date=row[0],
                message_count=row[1],
                medical_message_count=row[2],
                image_message_count=row[3],
                avg_engagement_score=float(row[4]) if row[4] else 0.0
            ))
        
        conn.close()
        return trends
        
    except Exception as e:
        logger.error(f"Error fetching trends: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch trends")

@app.get("/api/v1/images", response_model=List[ImageAnalysis])
async def get_image_analysis(
    limit: int = Query(10, description="Number of channels to return")
):
    """Get image analysis insights across channels"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT 
                channel_username,
                COUNT(*) as total_images,
                COUNT(CASE WHEN has_medical_objects THEN 1 END) as medical_images,
                ROUND(
                    (COUNT(CASE WHEN has_medical_objects THEN 1 END)::FLOAT / COUNT(*)) * 100, 2
                ) as medical_percentage,
                AVG(detected_objects_count) as avg_objects_detected,
                AVG(avg_confidence_score) as avg_confidence_score,
                array_agg(DISTINCT detected_classes) FILTER (WHERE detected_classes IS NOT NULL) as top_detected_classes
            FROM fct_messages
            WHERE has_image = TRUE
              AND detected_objects_count IS NOT NULL
            GROUP BY channel_username
            ORDER BY total_images DESC
            LIMIT %s
        """
        
        cursor.execute(query, (limit,))
        results = cursor.fetchall()
        
        images = []
        for row in results:
            # Flatten the detected classes array
            detected_classes = []
            if row[6]:  # top_detected_classes
                for class_array in row[6]:
                    if class_array:
                        detected_classes.extend(class_array)
            
            images.append(ImageAnalysis(
                channel_username=row[0],
                total_images=row[1],
                medical_images=row[2],
                medical_percentage=float(row[3]) if row[3] else 0.0,
                avg_objects_detected=float(row[4]) if row[4] else 0.0,
                avg_confidence_score=float(row[5]) if row[5] else 0.0,
                top_detected_classes=list(set(detected_classes))[:10]  # Top 10 unique classes
            ))
        
        conn.close()
        return images
        
    except Exception as e:
        logger.error(f"Error fetching image analysis: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch image analysis")

@app.get("/api/v1/summary")
async def get_summary():
    """Get overall platform summary statistics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT 
                COUNT(DISTINCT channel_username) as total_channels,
                COUNT(*) as total_messages,
                COUNT(CASE WHEN is_medical_related THEN 1 END) as medical_messages,
                COUNT(CASE WHEN has_image THEN 1 END) as messages_with_images,
                COUNT(CASE WHEN has_medical_objects THEN 1 END) as messages_with_medical_objects,
                AVG(engagement_score) as avg_engagement_score,
                MIN(message_date) as earliest_message,
                MAX(message_date) as latest_message
            FROM fct_messages
        """
        
        cursor.execute(query)
        result = cursor.fetchone()
        
        summary = {
            "total_channels": result[0],
            "total_messages": result[1],
            "medical_messages": result[2],
            "messages_with_images": result[3],
            "messages_with_medical_objects": result[4],
            "avg_engagement_score": float(result[5]) if result[5] else 0.0,
            "earliest_message": result[6].isoformat() if result[6] else None,
            "latest_message": result[7].isoformat() if result[7] else None,
            "medical_content_percentage": round((result[2] / result[1]) * 100, 2) if result[1] > 0 else 0,
            "image_content_percentage": round((result[3] / result[1]) * 100, 2) if result[1] > 0 else 0
        }
        
        conn.close()
        return summary
        
    except Exception as e:
        logger.error(f"Error fetching summary: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch summary")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 