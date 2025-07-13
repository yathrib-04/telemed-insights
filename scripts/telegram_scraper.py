#!/usr/bin/env python3
"""
Telegram Scraper for Ethiopian Medical Data Platform
Extracts data from public Telegram channels and stores in data lake
"""

import os
import json
import asyncio
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional

import psycopg2
from telethon import TelegramClient, events
from telethon.tl.types import Message, MessageMediaPhoto, MessageMediaDocument
from dotenv import load_dotenv
from loguru import logger
import aiofiles

# Load environment variables
load_dotenv()

class TelegramScraper:
    """Telegram scraper for Ethiopian medical channels"""
    
    def __init__(self):
        self.api_id = os.getenv('TELEGRAM_API_ID')
        self.api_hash = os.getenv('TELEGRAM_API_HASH')
        self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        
        if not all([self.api_id, self.api_hash]):
            raise ValueError("TELEGRAM_API_ID and TELEGRAM_API_HASH must be set")
        
        # Initialize Telegram client
        self.client = TelegramClient('ethiopian_medical_session', self.api_id, self.api_hash)
        
        # Database connection
        self.db_params = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'password'),
            'database': os.getenv('DB_NAME', 'ethiopian_medical_data')
        }
        
        # Target channels
        self.channels = [
            'lobelia4cosmetics',
            'tikvahpharma',
            'chemed_telegram',  # Placeholder for Chemed channel
            # Add more channels from https://et.tgstat.com/medicine
        ]
        
        # Data lake structure
        self.data_lake_path = Path('data/raw/telegram_messages')
        self.data_lake_path.mkdir(parents=True, exist_ok=True)
        
    async def start(self):
        """Start the Telegram client"""
        await self.client.start()
        logger.info("Telegram client started successfully")
    
    async def stop(self):
        """Stop the Telegram client"""
        await self.client.disconnect()
        logger.info("Telegram client disconnected")
    
    def _get_channel_data_path(self, channel_username: str, date: datetime) -> Path:
        """Get the file path for storing channel data"""
        date_str = date.strftime('%Y-%m-%d')
        return self.data_lake_path / date_str / f"{channel_username}.json"
    
    def _extract_message_data(self, message: Message, channel_username: str) -> Dict[str, Any]:
        """Extract relevant data from a Telegram message"""
        data = {
            'message_id': message.id,
            'channel_username': channel_username,
            'message_text': message.text or message.raw_text or '',
            'message_date': message.date.isoformat(),
            'has_media': message.media is not None,
            'media_type': None,
            'media_url': None,
            'views': getattr(message, 'views', None),
            'forwards': getattr(message, 'forwards', None),
            'replies': getattr(message.replies, 'replies', None) if message.replies else None,
            'raw_data': {
                'id': message.id,
                'date': message.date.isoformat(),
                'edit_date': message.edit_date.isoformat() if message.edit_date else None,
                'post_author': message.post_author,
                'fwd_from': str(message.fwd_from) if message.fwd_from else None,
                'via_bot': str(message.via_bot) if message.via_bot else None,
                'reply_to': str(message.reply_to) if message.reply_to else None,
                'media': str(message.media) if message.media else None,
                'reply_markup': str(message.reply_markup) if message.reply_markup else None,
                'entities': [str(entity) for entity in message.entities] if message.entities else None,
                'out': message.out,
                'mentioned': message.mentioned,
                'media_unread': message.media_unread,
                'silent': message.silent,
                'post': message.post,
                'from_scheduled': message.from_scheduled,
                'legacy': message.legacy,
                'edit_hide': message.edit_hide,
                'pinned': message.pinned,
                'noforwards': message.noforwards,
                'invert_media': message.invert_media,
                'offline': message.offline,
                'imported': message.imported,
                'reactions': str(message.reactions) if message.reactions else None,
                'restriction_reason': [str(reason) for reason in message.restriction_reason] if message.restriction_reason else None,
                'ttl_period': message.ttl_period
            }
        }
        
        # Handle media
        if message.media:
            if isinstance(message.media, MessageMediaPhoto):
                data['media_type'] = 'photo'
                # Note: Getting media URL requires additional API calls
            elif isinstance(message.media, MessageMediaDocument):
                data['media_type'] = 'document'
        
        return data
    
    async def _save_to_data_lake(self, channel_username: str, messages: List[Dict[str, Any]], date: datetime):
        """Save messages to data lake as JSON files"""
        file_path = self._get_channel_data_path(channel_username, date)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Load existing data if file exists
        existing_data = []
        if file_path.exists():
            async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                content = await f.read()
                if content.strip():
                    existing_data = json.loads(content)
        
        # Add new messages
        existing_data.extend(messages)
        
        # Save to file
        async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(existing_data, indent=2, ensure_ascii=False))
        
        logger.info(f"Saved {len(messages)} messages to {file_path}")
    
    async def _save_to_database(self, messages: List[Dict[str, Any]]):
        """Save messages to PostgreSQL database"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cursor = conn.cursor()
            
            for message in messages:
                cursor.execute("""
                    INSERT INTO raw.telegram_messages 
                    (message_id, channel_username, channel_title, message_text, message_date, 
                     has_media, media_type, media_url, views, forwards, replies, raw_data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (message_id, channel_username) DO NOTHING
                """, (
                    message['message_id'],
                    message['channel_username'],
                    message.get('channel_title'),
                    message['message_text'],
                    message['message_date'],
                    message['has_media'],
                    message['media_type'],
                    message['media_url'],
                    message['views'],
                    message['forwards'],
                    message['replies'],
                    json.dumps(message['raw_data'])
                ))
            
            conn.commit()
            logger.info(f"Saved {len(messages)} messages to database")
            
        except Exception as e:
            logger.error(f"Database error: {e}")
            if 'conn' in locals():
                conn.rollback()
        finally:
            if 'conn' in locals():
                conn.close()
    
    async def scrape_channel(self, channel_username: str, limit: int = 100, days_back: int = 7):
        """Scrape messages from a specific channel"""
        try:
            logger.info(f"Scraping channel: {channel_username}")
            
            # Get channel entity
            channel = await self.client.get_entity(channel_username)
            
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            messages = []
            async for message in self.client.iter_messages(channel, limit=limit):
                # Check if message is within date range
                if message.date < start_date:
                    break
                
                # Extract message data
                message_data = self._extract_message_data(message, channel_username)
                message_data['channel_title'] = getattr(channel, 'title', None)
                messages.append(message_data)
                
                # Log progress
                if len(messages) % 10 == 0:
                    logger.info(f"Scraped {len(messages)} messages from {channel_username}")
            
            # Save to data lake
            await self._save_to_data_lake(channel_username, messages, end_date)
            
            # Save to database
            await self._save_to_database(messages)
            
            logger.success(f"Successfully scraped {len(messages)} messages from {channel_username}")
            return messages
            
        except Exception as e:
            logger.error(f"Error scraping channel {channel_username}: {e}")
            return []
    
    async def scrape_all_channels(self, limit: int = 100, days_back: int = 7):
        """Scrape all target channels"""
        logger.info(f"Starting to scrape {len(self.channels)} channels")
        
        for channel in self.channels:
            try:
                await self.scrape_channel(channel, limit, days_back)
                # Add delay to avoid rate limiting
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Failed to scrape channel {channel}: {e}")
                continue
        
        logger.success("Completed scraping all channels")

async def main():
    """Main function to run the scraper"""
    scraper = TelegramScraper()
    
    try:
        await scraper.start()
        await scraper.scrape_all_channels(limit=50, days_back=3)
    finally:
        await scraper.stop()

if __name__ == "__main__":
    asyncio.run(main()) 