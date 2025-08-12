# Ethiopian Medical Data Platform

A comprehensive data platform for generating insights about Ethiopian medical businesses using data scraped from public Telegram channels. This project implements a modern ELT (Extract, Load, Transform) framework with data enrichment using YOLO object detection.

## 🏗️ Architecture Overview

The platform follows a layered data architecture:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Telegram      │    │   Data Lake     │    │   PostgreSQL    │
│   Channels      │───▶│   (Raw JSON)    │───▶│   Data          │
│                 │    │                 │    │   Warehouse     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                       │
                                                       ▼
                                              ┌─────────────────┐
                                              │   DBT Models    │
                                              │   (Star Schema) │
                                              └─────────────────┘
                                                       │
                                                       ▼
                                              ┌─────────────────┐
                                              │   FastAPI       │
                                              │   Analytics API │
                                              └─────────────────┘
```

## 🚀 Features

- **Data Extraction**: Automated scraping from Ethiopian medical Telegram channels
- **Data Lake**: Raw data storage in partitioned JSON format
- **Data Warehouse**: PostgreSQL with dimensional star schema
- **Data Transformation**: DBT models for cleaning and modeling
- **Object Detection**: YOLO-based image analysis for medical products
- **Analytics API**: FastAPI endpoints for business insights
- **Containerization**: Docker and Docker Compose for easy deployment

## 📋 Business Questions Answered

1. **Top Medical Products**: What are the most frequently mentioned medical products across channels?
2. **Channel Analysis**: Which channels have the most medical content and visual media?
3. **Trend Analysis**: How do posting patterns and engagement vary over time?
4. **Image Insights**: Which channels have the most medical product images?
5. **Engagement Metrics**: Which content types generate the most engagement?

## 🛠️ Technology Stack

- **Data Extraction**: Telethon (Telegram API)
- **Database**: PostgreSQL 15
- **Data Transformation**: dbt (Data Build Tool)
- **API Framework**: FastAPI
- **Object Detection**: YOLO (Ultralytics)
- **Containerization**: Docker & Docker Compose
- **Logging**: Loguru
- **Environment**: python-dotenv


## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Telegram API credentials (API ID, API Hash, Bot Token)

### 1. Environment Setup

```bash
# Clone the repository
git clone <repository-url>
cd ethiopian-medical-data-platform

# Copy environment template
cp env.example .env

# Edit .env with your credentials
nano .env
```

### 2. Environment Variables

Create a `.env` file with the following variables:

```env
# Database Configuration
DB_USER=postgres
DB_PASSWORD=your_secure_password_here
DATABASE_URL=postgresql://postgres:your_secure_password_here@localhost:5432/ethiopian_medical_data

# Telegram API Configuration
TELEGRAM_API_ID=your_telegram_api_id_here
TELEGRAM_API_HASH=your_telegram_api_hash_here
TELEGRAM_BOT_TOKEN=your_telegram_bot_token_here

# Application Configuration
LOG_LEVEL=INFO
ENVIRONMENT=development

# YOLO Model Configuration
YOLO_MODEL_PATH=models/yolo_model.pt
CONFIDENCE_THRESHOLD=0.5

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
```

### 3. Start the Platform

```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f app
```

### 4. Initialize Data Pipeline

```bash
# Run database initialization
docker-compose exec app python scripts/init_database.py

# Run Telegram scraper
docker-compose exec app python scripts/telegram_scraper.py

# Run DBT transformations
docker-compose exec dbt dbt run
```

### 5. Access the API

- **API Documentation**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health
- **Channel Insights**: http://localhost:8000/api/v1/channels
- **Product Analysis**: http://localhost:8000/api/v1/products
- **Trends**: http://localhost:8000/api/v1/trends
- **Image Analysis**: http://localhost:8000/api/v1/images

## 📊 Data Model

### Star Schema Design

```
                    ┌─────────────────┐
                    │   dim_channels  │
                    │                 │
                    │ channel_key (PK)│
                    │ channel_username│
                    │ channel_title   │
                    │ medical_focus   │
                    │ visual_content  │
                    └─────────────────┘
                            │
                            │
┌─────────────────┐         │         ┌─────────────────┐
│   dim_dates     │         │         │   fct_messages  │
│                 │         │         │                 │
│ date_key (PK)   │─────────┼─────────│ message_key (PK)│
│ date_actual     │         │         │ channel_key (FK)│
│ year, month     │         │         │ date_key (FK)   │
│ day_of_week     │         │         │ message_text    │
│ season          │         │         │ engagement_score│
│ is_business_day │         │         │ is_medical_related│
└─────────────────┘         │         │ has_image       │
                            │         │ has_medical_objects│
                            │         └─────────────────┘
                            │
                            │
                    ┌─────────────────┐
                    │ stg_telegram_   │
                    │ images          │
                    │                 │
                    │ message_id (FK) │
                    │ image_hash      │
                    │ detected_objects│
                    │ confidence_score│
                    └─────────────────┘
```

### Key Tables

1. **dim_channels**: Channel information and metrics
2. **dim_dates**: Time dimension with various date attributes
3. **fct_messages**: Main fact table with message data
4. **stg_telegram_messages**: Cleaned message data
5. **stg_telegram_images**: Image analysis results

## 🔍 API Endpoints

### Channel Insights
```bash
GET /api/v1/channels?limit=10&sort_by=total_messages
```

### Product Analysis
```bash
GET /api/v1/products?limit=10&min_mentions=5
```

### Trend Analysis
```bash
GET /api/v1/trends?days=30&trend_type=daily
```

### Image Analysis
```bash
GET /api/v1/images?limit=10
```

### Platform Summary
```bash
GET /api/v1/summary
```

## 🧪 Data Quality & Testing

### DBT Tests

The platform includes comprehensive data quality tests:

1. **Built-in Tests**: Unique, not_null for primary keys
2. **Custom Tests**: 
   - Medical content consistency validation
   - Engagement score calculation verification
   - Data anomaly detection

### Running Tests

```bash
# Run all DBT tests
docker-compose exec dbt dbt test

# Run specific test
docker-compose exec dbt dbt test --select test_medical_content_consistency
```

## 🔄 Data Pipeline

### 1. Data Extraction
- Automated scraping from Telegram channels
- Rate limiting and error handling
- Incremental processing support

### 2. Data Storage
- Raw data stored in partitioned JSON files
- PostgreSQL for structured data warehouse
- Backup and recovery procedures

### 3. Data Transformation
- DBT models for data cleaning
- Star schema implementation
- Incremental model updates

### 4. Data Enrichment
- YOLO object detection on images
- Medical product identification
- Confidence scoring

### 5. Data Delivery
- FastAPI for analytical queries
- Real-time insights
- RESTful API design

## 📈 Monitoring & Logging

### Logs
- Application logs in `logs/` directory
- Structured logging with Loguru
- Error tracking and alerting

### Health Checks
- Database connectivity monitoring
- API endpoint health checks
- Service status monitoring

## 🔧 Development

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Set up database
python scripts/init_database.py

# Run scraper
python scripts/telegram_scraper.py

# Run DBT
cd dbt && dbt run

# Start API
uvicorn api.main:app --reload
```

### Adding New Channels

Edit `scripts/telegram_scraper.py` and add new channel usernames to the `channels` list:

```python
self.channels = [
    'lobelia4cosmetics',
    'tikvahpharma',
    'new_channel_username',  # Add new channels here
]
```

### Custom DBT Models

Create new models in the appropriate directory:
- `dbt/models/staging/` for data cleaning
- `dbt/models/marts/` for business logic

## 🚨 Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Check PostgreSQL service is running
   - Verify database credentials in `.env`
   - Ensure database exists

2. **Telegram API Errors**
   - Verify API credentials
   - Check rate limiting
   - Ensure channels are public

3. **DBT Model Errors**
   - Check source table exists
   - Verify SQL syntax
   - Review model dependencies

### Debug Commands

```bash
# Check service logs
docker-compose logs [service_name]

# Access database
docker-compose exec postgres psql -U postgres -d ethiopian_medical_data

# Run DBT with debug
docker-compose exec dbt dbt run --debug

# Check API health
curl http://localhost:8000/health
```

## 📝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🤝 Support

For support and questions:
- Create an issue in the repository
- Check the troubleshooting section
- Review the API documentation at `/docs`

## 🔮 Future Enhancements

- Real-time data streaming
- Advanced ML models for product classification
- Dashboard integration
- Multi-language support
- Advanced analytics and reporting
- Integration with external data sources 
