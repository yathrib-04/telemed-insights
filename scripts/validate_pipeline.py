#!/usr/bin/env python3
"""
Pipeline Validation Script for Ethiopian Medical Data Platform
Validates the entire data pipeline end-to-end
"""

import os
import sys
import asyncio
import json
from pathlib import Path
from typing import Dict, List, Any, Tuple
import psycopg2
from dotenv import load_dotenv
from loguru import logger

# Load environment variables
load_dotenv()

class PipelineValidator:
    """Validates the entire data pipeline"""
    
    def __init__(self):
        self.db_params = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'password'),
            'database': os.getenv('DB_NAME', 'ethiopian_medical_data')
        }
        
        self.data_lake_path = Path('data/raw/telegram_messages')
        self.validation_results = {}
    
    def validate_environment(self) -> bool:
        """Validate environment setup"""
        logger.info("Validating environment setup...")
        
        try:
            # Check required environment variables
            required_vars = [
                'TELEGRAM_API_ID', 'TELEGRAM_API_HASH', 'TELEGRAM_BOT_TOKEN',
                'DB_USER', 'DB_PASSWORD'
            ]
            
            missing_vars = []
            for var in required_vars:
                if not os.getenv(var):
                    missing_vars.append(var)
            
            if missing_vars:
                logger.error(f"Missing environment variables: {missing_vars}")
                self.validation_results['environment'] = {
                    'status': 'failed',
                    'missing_vars': missing_vars
                }
                return False
            
            # Check data directories
            required_dirs = [
                'data/raw/telegram_messages',
                'data/processed',
                'logs',
                'models'
            ]
            
            missing_dirs = []
            for dir_path in required_dirs:
                if not Path(dir_path).exists():
                    missing_dirs.append(dir_path)
                    Path(dir_path).mkdir(parents=True, exist_ok=True)
                    logger.info(f"Created directory: {dir_path}")
            
            self.validation_results['environment'] = {
                'status': 'passed',
                'missing_vars': [],
                'missing_dirs': missing_dirs
            }
            
            logger.success("Environment validation passed")
            return True
            
        except Exception as e:
            logger.error(f"Environment validation failed: {e}")
            self.validation_results['environment'] = {
                'status': 'failed',
                'error': str(e)
            }
            return False
    
    def validate_database_connection(self) -> bool:
        """Validate database connectivity"""
        logger.info("Validating database connection...")
        
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            
            # Test basic connection
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            
            if result[0] != 1:
                raise Exception("Database connection test failed")
            
            # Check if database exists
            cursor.execute("SELECT current_database()")
            db_name = cursor.fetchone()[0]
            
            conn.close()
            
            self.validation_results['database'] = {
                'status': 'passed',
                'database_name': db_name,
                'connection': 'successful'
            }
            
            logger.success("Database validation passed")
            return True
            
        except Exception as e:
            logger.error(f"Database validation failed: {e}")
            self.validation_results['database'] = {
                'status': 'failed',
                'error': str(e)
            }
            return False
    
    def validate_database_schema(self) -> bool:
        """Validate database schema and tables"""
        logger.info("Validating database schema...")
        
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            
            # Check raw schema
            cursor.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name = 'raw'
            """)
            raw_schema = cursor.fetchone()
            
            if not raw_schema:
                # Create raw schema if it doesn't exist
                cursor.execute("CREATE SCHEMA IF NOT EXISTS raw")
                conn.commit()
                logger.info("Created raw schema")
            
            # Check required tables
            required_tables = {
                'raw.telegram_messages': [
                    'id', 'message_id', 'channel_username', 'message_text', 
                    'message_date', 'has_media', 'raw_data'
                ],
                'raw.telegram_images': [
                    'id', 'message_id', 'channel_username', 'image_url', 
                    'local_path', 'image_hash', 'object_detection_results'
                ]
            }
            
            missing_tables = []
            table_columns = {}
            
            for table, required_columns in required_tables.items():
                schema, table_name = table.split('.')
                
                # Check if table exists
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = %s
                """, (schema, table_name))
                
                if not cursor.fetchone():
                    missing_tables.append(table)
                    continue
                
                # Check columns
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = %s AND table_name = %s
                """, (schema, table_name))
                
                existing_columns = [row[0] for row in cursor.fetchall()]
                missing_columns = [col for col in required_columns if col not in existing_columns]
                
                table_columns[table] = {
                    'exists': True,
                    'missing_columns': missing_columns
                }
            
            conn.close()
            
            self.validation_results['database_schema'] = {
                'status': 'passed' if not missing_tables else 'partial',
                'missing_tables': missing_tables,
                'table_columns': table_columns
            }
            
            if missing_tables:
                logger.warning(f"Missing tables: {missing_tables}")
            else:
                logger.success("Database schema validation passed")
            
            return len(missing_tables) == 0
            
        except Exception as e:
            logger.error(f"Database schema validation failed: {e}")
            self.validation_results['database_schema'] = {
                'status': 'failed',
                'error': str(e)
            }
            return False
    
    def validate_data_lake(self) -> bool:
        """Validate data lake structure and content"""
        logger.info("Validating data lake...")
        
        try:
            # Check data lake directory structure
            if not self.data_lake_path.exists():
                self.data_lake_path.mkdir(parents=True, exist_ok=True)
                logger.info("Created data lake directory")
            
            # Check for existing data files
            data_files = list(self.data_lake_path.rglob("*.json"))
            
            # Check directory structure
            date_dirs = [d for d in self.data_lake_path.iterdir() if d.is_dir()]
            
            self.validation_results['data_lake'] = {
                'status': 'passed',
                'data_files_count': len(data_files),
                'date_directories': [str(d.name) for d in date_dirs],
                'total_size_mb': sum(f.stat().st_size for f in data_files) / (1024 * 1024)
            }
            
            logger.success(f"Data lake validation passed - {len(data_files)} files found")
            return True
            
        except Exception as e:
            logger.error(f"Data lake validation failed: {e}")
            self.validation_results['data_lake'] = {
                'status': 'failed',
                'error': str(e)
            }
            return False
    
    def validate_dbt_project(self) -> bool:
        """Validate DBT project setup"""
        logger.info("Validating DBT project...")
        
        try:
            dbt_path = Path('dbt')
            
            if not dbt_path.exists():
                logger.error("DBT project directory not found")
                self.validation_results['dbt_project'] = {
                    'status': 'failed',
                    'error': 'DBT project directory not found'
                }
                return False
            
            # Check required DBT files
            required_files = [
                'dbt_project.yml',
                'profiles.yml',
                'models/sources.yml'
            ]
            
            missing_files = []
            for file_path in required_files:
                if not (dbt_path / file_path).exists():
                    missing_files.append(file_path)
            
            # Check model files
            model_files = list((dbt_path / 'models').rglob("*.sql"))
            
            if missing_files:
                logger.warning(f"Missing DBT files: {missing_files}")
                self.validation_results['dbt_project'] = {
                    'status': 'partial',
                    'missing_files': missing_files,
                    'model_files_count': len(model_files)
                }
                return False
            
            self.validation_results['dbt_project'] = {
                'status': 'passed',
                'model_files_count': len(model_files),
                'model_files': [str(f.relative_to(dbt_path)) for f in model_files]
            }
            
            logger.success("DBT project validation passed")
            return True
            
        except Exception as e:
            logger.error(f"DBT project validation failed: {e}")
            self.validation_results['dbt_project'] = {
                'status': 'failed',
                'error': str(e)
            }
            return False
    
    def validate_api_endpoints(self) -> bool:
        """Validate FastAPI endpoints"""
        logger.info("Validating FastAPI endpoints...")
        
        try:
            api_path = Path('api/main.py')
            
            if not api_path.exists():
                logger.error("FastAPI main.py not found")
                self.validation_results['api_endpoints'] = {
                    'status': 'failed',
                    'error': 'FastAPI main.py not found'
                }
                return False
            
            # Check if required endpoints are defined
            with open(api_path, 'r') as f:
                content = f.read()
            
            required_endpoints = [
                '/api/v1/channels',
                '/api/v1/products', 
                '/api/v1/trends',
                '/api/v1/images',
                '/api/v1/summary'
            ]
            
            missing_endpoints = []
            for endpoint in required_endpoints:
                if endpoint not in content:
                    missing_endpoints.append(endpoint)
            
            if missing_endpoints:
                logger.warning(f"Missing API endpoints: {missing_endpoints}")
                self.validation_results['api_endpoints'] = {
                    'status': 'partial',
                    'missing_endpoints': missing_endpoints
                }
                return False
            
            self.validation_results['api_endpoints'] = {
                'status': 'passed',
                'endpoints_found': len(required_endpoints) - len(missing_endpoints)
            }
            
            logger.success("API endpoints validation passed")
            return True
            
        except Exception as e:
            logger.error(f"API endpoints validation failed: {e}")
            self.validation_results['api_endpoints'] = {
                'status': 'failed',
                'error': str(e)
            }
            return False
    
    def validate_object_detection(self) -> bool:
        """Validate YOLO object detection setup"""
        logger.info("Validating object detection setup...")
        
        try:
            # Check if object detection script exists
            od_script = Path('scripts/object_detection.py')
            
            if not od_script.exists():
                logger.error("Object detection script not found")
                self.validation_results['object_detection'] = {
                    'status': 'failed',
                    'error': 'Object detection script not found'
                }
                return False
            
            # Check if ultralytics is in requirements
            with open('requirements.txt', 'r') as f:
                requirements = f.read()
            
            if 'ultralytics' not in requirements:
                logger.warning("ultralytics not found in requirements.txt")
                self.validation_results['object_detection'] = {
                    'status': 'partial',
                    'warning': 'ultralytics not in requirements'
                }
                return False
            
            self.validation_results['object_detection'] = {
                'status': 'passed',
                'script_exists': True,
                'dependencies': 'ultralytics found'
            }
            
            logger.success("Object detection validation passed")
            return True
            
        except Exception as e:
            logger.error(f"Object detection validation failed: {e}")
            self.validation_results['object_detection'] = {
                'status': 'failed',
                'error': str(e)
            }
            return False
    
    def run_full_validation(self) -> Dict[str, Any]:
        """Run complete pipeline validation"""
        logger.info("Starting full pipeline validation...")
        
        validation_steps = [
            ('environment', self.validate_environment),
            ('database_connection', self.validate_database_connection),
            ('database_schema', self.validate_database_schema),
            ('data_lake', self.validate_data_lake),
            ('dbt_project', self.validate_dbt_project),
            ('api_endpoints', self.validate_api_endpoints),
            ('object_detection', self.validate_object_detection)
        ]
        
        overall_status = 'passed'
        failed_steps = []
        
        for step_name, validation_func in validation_steps:
            try:
                step_result = validation_func()
                if not step_result:
                    overall_status = 'failed'
                    failed_steps.append(step_name)
            except Exception as e:
                logger.error(f"Validation step {step_name} failed with exception: {e}")
                overall_status = 'failed'
                failed_steps.append(step_name)
        
        # Add overall results
        self.validation_results['overall'] = {
            'status': overall_status,
            'failed_steps': failed_steps,
            'total_steps': len(validation_steps),
            'passed_steps': len(validation_steps) - len(failed_steps)
        }
        
        # Log results
        if overall_status == 'passed':
            logger.success("🎉 All validation steps passed! Pipeline is ready.")
        else:
            logger.error(f"❌ Validation failed for steps: {failed_steps}")
        
        return self.validation_results
    
    def generate_validation_report(self) -> str:
        """Generate a human-readable validation report"""
        report = []
        report.append("=" * 60)
        report.append("ETHIOPIAN MEDICAL DATA PLATFORM - VALIDATION REPORT")
        report.append("=" * 60)
        report.append("")
        
        # Overall status
        overall = self.validation_results.get('overall', {})
        report.append(f"OVERALL STATUS: {overall.get('status', 'unknown').upper()}")
        report.append(f"Passed: {overall.get('passed_steps', 0)}/{overall.get('total_steps', 0)} steps")
        report.append("")
        
        # Individual step results
        for step_name, step_data in self.validation_results.items():
            if step_name == 'overall':
                continue
                
            status = step_data.get('status', 'unknown')
            status_icon = "✅" if status == 'passed' else "⚠️" if status == 'partial' else "❌"
            
            report.append(f"{status_icon} {step_name.upper().replace('_', ' ')}: {status}")
            
            if step_data.get('error'):
                report.append(f"   Error: {step_data['error']}")
            if step_data.get('warning'):
                report.append(f"   Warning: {step_data['warning']}")
            if step_data.get('missing_vars'):
                report.append(f"   Missing: {', '.join(step_data['missing_vars'])}")
        
        report.append("")
        report.append("=" * 60)
        
        return "\n".join(report)

async def main():
    """Main validation function"""
    validator = PipelineValidator()
    
    # Run full validation
    results = validator.run_full_validation()
    
    # Generate and display report
    report = validator.generate_validation_report()
    print(report)
    
    # Save results to file
    with open('validation_report.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    logger.info("Validation report saved to validation_report.json")
    
    # Return exit code based on validation results
    overall_status = results.get('overall', {}).get('status', 'unknown')
    if overall_status == 'passed':
        return 0
    else:
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
