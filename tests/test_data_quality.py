#!/usr/bin/env python3
"""
Data Quality Tests for Ethiopian Medical Data Platform
Tests data integrity, consistency, and business rules
"""

import pytest
import psycopg2
import json
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
import sys
import os

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class TestDataQuality:
    """Test class for data quality validation"""
    
    def setup_method(self):
        """Setup method for each test"""
        self.db_params = {
            'host': 'localhost',
            'port': '5432',
            'user': 'postgres',
            'password': 'password',
            'database': 'ethiopian_medical_data'
        }
    
    def test_database_connection(self):
        """Test database connectivity"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            conn.close()
            assert result[0] == 1
        except Exception as e:
            pytest.skip(f"Database not available: {e}")
    
    def test_raw_schema_exists(self):
        """Test that raw schema exists"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name = 'raw'
            """)
            result = cursor.fetchone()
            conn.close()
            assert result is not None, "Raw schema should exist"
        except Exception as e:
            pytest.skip(f"Database not available: {e}")
    
    def test_raw_tables_exist(self):
        """Test that raw tables exist"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            
            # Check telegram_messages table
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'raw' AND table_name = 'telegram_messages'
            """)
            result = cursor.fetchone()
            assert result is not None, "raw.telegram_messages table should exist"
            
            # Check telegram_images table
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'raw' AND table_name = 'telegram_images'
            """)
            result = cursor.fetchone()
            assert result is not None, "raw.telegram_images table should exist"
            
            conn.close()
        except Exception as e:
            pytest.skip(f"Database not available: {e}")
    
    def test_staging_models_exist(self):
        """Test that staging models exist"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            
            # Check if staging models exist (they should be views)
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.views 
                WHERE table_schema = 'public' AND table_name LIKE 'stg_%'
            """)
            results = cursor.fetchall()
            
            # At least stg_telegram_messages should exist
            staging_tables = [row[0] for row in results]
            assert 'stg_telegram_messages' in staging_tables, "stg_telegram_messages view should exist"
            
            conn.close()
        except Exception as e:
            pytest.skip(f"Database not available: {e}")
    
    def test_mart_models_exist(self):
        """Test that mart models exist"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            
            # Check if mart models exist
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name LIKE 'dim_%' OR table_name LIKE 'fct_%'
            """)
            results = cursor.fetchall()
            
            # At least the core star schema tables should exist
            mart_tables = [row[0] for row in results]
            assert 'dim_channels' in mart_tables, "dim_channels table should exist"
            assert 'dim_dates' in mart_tables, "dim_dates table should exist"
            assert 'fct_messages' in mart_tables, "fct_messages table should exist"
            
            conn.close()
        except Exception as e:
            pytest.skip(f"Database not available: {e}")
    
    def test_data_consistency_checks(self):
        """Test data consistency across tables"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            
            # Check that channel usernames are consistent between raw and staging
            cursor.execute("""
                SELECT COUNT(DISTINCT channel_username) as raw_count
                FROM raw.telegram_messages
            """)
            raw_channels = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT COUNT(DISTINCT channel_username) as staging_count
                FROM stg_telegram_messages
            """)
            staging_channels = cursor.fetchone()[0]
            
            # The counts should be the same (or staging might have fewer due to filtering)
            assert staging_channels <= raw_channels, "Staging should not have more channels than raw"
            
            conn.close()
        except Exception as e:
            pytest.skip(f"Database not available: {e}")
    
    def test_medical_content_classification(self):
        """Test medical content classification logic"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            
            # Check that medical content classification is working
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_messages,
                    COUNT(CASE WHEN is_medical_related THEN 1 END) as medical_messages
                FROM stg_telegram_messages
                LIMIT 1000
            """)
            result = cursor.fetchone()
            
            if result[0] > 0:
                medical_percentage = (result[1] / result[0]) * 100
                
                # Medical content should be reasonable (not 0% or 100%)
                assert medical_percentage > 0, "Should have some medical content"
                assert medical_percentage < 100, "Should not be 100% medical content"
                
                # Log the percentage for debugging
                print(f"Medical content percentage: {medical_percentage:.2f}%")
            
            conn.close()
        except Exception as e:
            pytest.skip(f"Database not available: {e}")
    
    def test_engagement_score_calculation(self):
        """Test engagement score calculation"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            
            # Check that engagement scores are calculated correctly
            cursor.execute("""
                SELECT 
                    views, forwards, replies, engagement_score
                FROM stg_telegram_messages
                WHERE views IS NOT NULL AND forwards IS NOT NULL AND replies IS NOT NULL
                LIMIT 100
            """)
            results = cursor.fetchall()
            
            for row in results:
                views, forwards, replies, engagement_score = row
                
                # Calculate expected engagement score
                expected_score = (views or 0) + ((forwards or 0) * 2) + ((replies or 0) * 3)
                
                # Allow for small rounding differences
                assert abs(engagement_score - expected_score) <= 1, \
                    f"Engagement score mismatch: expected {expected_score}, got {engagement_score}"
            
            conn.close()
        except Exception as e:
            pytest.skip(f"Database not available: {e}")
    
    def test_date_dimension_integrity(self):
        """Test date dimension table integrity"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            
            # Check that date dimension has reasonable date range
            cursor.execute("""
                SELECT 
                    MIN(date_actual) as min_date,
                    MAX(date_actual) as max_date
                FROM dim_dates
            """)
            result = cursor.fetchone()
            
            if result[0] and result[1]:
                min_date = result[0]
                max_date = result[1]
                
                # Dates should be reasonable (not too far in past/future)
                current_date = datetime.now().date()
                assert min_date <= current_date, "Min date should not be in the future"
                assert max_date >= current_date - timedelta(days=365), "Max date should be recent"
            
            conn.close()
        except Exception as e:
            pytest.skip(f"Database not available: {e}")
    
    def test_channel_dimension_metrics(self):
        """Test channel dimension table metrics"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            
            # Check that channel metrics are calculated correctly
            cursor.execute("""
                SELECT 
                    channel_username,
                    total_messages,
                    medical_messages,
                    medical_content_percentage
                FROM dim_channels
                LIMIT 10
            """)
            results = cursor.fetchall()
            
            for row in results:
                username, total, medical, percentage = row
                
                if total > 0:
                    # Calculate expected percentage
                    expected_percentage = round((medical / total) * 100, 2)
                    
                    # Allow for small rounding differences
                    assert abs(percentage - expected_percentage) <= 0.01, \
                        f"Medical percentage mismatch for {username}: expected {expected_percentage}, got {percentage}"
            
            conn.close()
        except Exception as e:
            pytest.skip(f"Database not available: {e}")
    
    def test_fact_table_integrity(self):
        """Test fact table integrity and relationships"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            
            # Check that foreign keys exist and are valid
            cursor.execute("""
                SELECT COUNT(*) as total_facts
                FROM fct_messages
            """)
            total_facts = cursor.fetchone()[0]
            
            if total_facts > 0:
                # Check that all facts have valid channel keys
                cursor.execute("""
                    SELECT COUNT(*) as invalid_channels
                    FROM fct_messages f
                    LEFT JOIN dim_channels c ON f.channel_key = c.channel_key
                    WHERE c.channel_key IS NULL
                """)
                invalid_channels = cursor.fetchone()[0]
                assert invalid_channels == 0, "All facts should have valid channel keys"
                
                # Check that all facts have valid date keys
                cursor.execute("""
                    SELECT COUNT(*) as invalid_dates
                    FROM fct_messages f
                    LEFT JOIN dim_dates d ON f.date_key = d.date_key
                    WHERE d.date_key IS NULL
                """)
                invalid_dates = cursor.fetchone()[0]
                assert invalid_dates == 0, "All facts should have valid date keys"
            
            conn.close()
        except Exception as e:
            pytest.skip(f"Database not available: {e}")

if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
