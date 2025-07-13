-- Initialize database for Ethiopian Medical Data Platform

-- Create raw schema for storing unprocessed data
CREATE SCHEMA IF NOT EXISTS raw;

-- Create tables for raw telegram data
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

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_telegram_messages_channel ON raw.telegram_messages(channel_username);
CREATE INDEX IF NOT EXISTS idx_telegram_messages_date ON raw.telegram_messages(message_date);
CREATE INDEX IF NOT EXISTS idx_telegram_messages_has_media ON raw.telegram_messages(has_media);

-- Create table for scraped images
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

-- Create indexes for images table
CREATE INDEX IF NOT EXISTS idx_telegram_images_channel ON raw.telegram_images(channel_username);
CREATE INDEX IF NOT EXISTS idx_telegram_images_hash ON raw.telegram_images(image_hash);

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA raw TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA raw TO postgres; 