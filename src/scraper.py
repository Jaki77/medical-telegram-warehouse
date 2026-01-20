import os
import json
import logging
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import FloodWaitError, ChannelPrivateError
from telethon.tl.types import Message, Photo
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv('config/.env')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class TelegramScraper:
    def __init__(self, api_id: int, api_hash: str, phone_number: str):
        """
        Initialize Telegram scraper with API credentials
        
        Args:
            api_id: Telegram API ID
            api_hash: Telegram API Hash
            phone_number: Phone number associated with Telegram account
        """
        self.api_id = api_id
        self.api_hash = api_hash
        self.phone_number = phone_number
        self.client = None
        
        # Base directories
        self.base_dir = Path("data")
        self.raw_dir = self.base_dir / "raw"
        self.json_dir = self.raw_dir / "telegram_messages"
        self.images_dir = self.raw_dir / "images"
        
        # Create directories if they don't exist
        self.json_dir.mkdir(parents=True, exist_ok=True)
        self.images_dir.mkdir(parents=True, exist_ok=True)
        
        # Telegram channels to scrape (from environment or default)
        channels_env = os.getenv('TELEGRAM_CHANNELS', 'chemed,lobelia4cosmetics,tikvahpharma')
        self.channels = [channel.strip() for channel in channels_env.split(',')]
        
        # Additional channels from etggstat.com (you can expand this list)
        self.additional_channels = [
            "ethiopharma",
            "pharmacyethiopia",
            "medicalethiopia"
        ]
        
        # Combine all channels
        self.all_channels = self.channels + self.additional_channels
        
        logger.info(f"Initialized scraper for {len(self.all_channels)} channels")
    
    async def initialize_client(self):
        """Initialize Telegram client"""
        try:
            self.client = TelegramClient(
                'medical_scraper_session',
                self.api_id,
                self.api_hash
            )
            await self.client.start(phone=self.phone_number)
            logger.info("Telegram client initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Telegram client: {e}")
            return False
    
    def _sanitize_channel_name(self, channel_name: str) -> str:
        """Remove special characters from channel name for directory naming"""
        return ''.join(c for c in channel_name if c.isalnum() or c in ('_', '-')).rstrip()
    
    async def scrape_channel(self, channel_identifier: str, days_back: int = 7) -> List[Dict]:
        """
        Scrape messages from a Telegram channel
        
        Args:
            channel_identifier: Channel username or invite link
            days_back: Number of days to look back for messages
            
        Returns:
            List of message dictionaries
        """
        messages = []
        
        try:
            # Get channel entity
            channel = await self.client.get_entity(channel_identifier)
            channel_name = getattr(channel, 'title', channel_identifier)
            sanitized_name = self._sanitize_channel_name(channel_name)
            
            logger.info(f"Scraping channel: {channel_name}")
            
            # Calculate date limit
            date_limit = datetime.now() - timedelta(days=days_back)
            
            # Iterate through messages
            async for message in self.client.iter_messages(channel, limit=None, offset_date=date_limit):
                try:
                    message_data = await self._extract_message_data(message, channel_name)
                    messages.append(message_data)
                    
                    # Download image if present
                    if message_data['has_media'] and message_data.get('image_path'):
                        await self._download_image(message, message_data['image_path'])
                    
                except Exception as e:
                    logger.error(f"Error processing message {message.id}: {e}")
                    continue
            
            logger.info(f"Scraped {len(messages)} messages from {channel_name}")
            return messages
            
        except ChannelPrivateError:
            logger.warning(f"Channel {channel_identifier} is private or inaccessible")
            return []
        except FloodWaitError as e:
            logger.warning(f"Flood wait required: {e.seconds} seconds")
            await asyncio.sleep(e.seconds + 5)
            return await self.scrape_channel(channel_identifier, days_back)
        except Exception as e:
            logger.error(f"Error scraping channel {channel_identifier}: {e}")
            return []
    
    async def _extract_message_data(self, message: Message, channel_name: str) -> Dict:
        """Extract relevant data from a Telegram message"""
        
        # Initialize message data
        message_data = {
            'message_id': message.id,
            'channel_name': channel_name,
            'message_date': message.date.isoformat() if message.date else None,
            'message_text': message.text or '',
            'has_media': message.media is not None,
            'image_path': None,
            'views': message.views or 0,
            'forwards': message.forwards or 0,
            'scraped_at': datetime.now().isoformat()
        }
        
        # Check for image media
        if message.media and hasattr(message.media, 'photo'):
            # Create image path
            sanitized_channel = self._sanitize_channel_name(channel_name)
            image_dir = self.images_dir / sanitized_channel
            image_dir.mkdir(parents=True, exist_ok=True)
            
            image_filename = f"{message.id}.jpg"
            image_path = image_dir / image_filename
            message_data['image_path'] = str(image_path)
        
        return message_data
    
    async def _download_image(self, message: Message, image_path: str):
        """Download image from message if available"""
        try:
            if message.media and hasattr(message.media, 'photo'):
                await self.client.download_media(message.media, file=image_path)
                logger.debug(f"Downloaded image: {image_path}")
        except Exception as e:
            logger.error(f"Failed to download image for message {message.id}: {e}")
    
    def save_messages_json(self, messages: List[Dict], channel_name: str):
        """
        Save messages to JSON file with partitioned structure
        
        Args:
            messages: List of message dictionaries
            channel_name: Name of the channel
        """
        if not messages:
            logger.warning(f"No messages to save for channel {channel_name}")
            return
        
        # Group messages by date
        messages_by_date = {}
        for msg in messages:
            if msg.get('message_date'):
                date_str = msg['message_date'][:10]  # Extract YYYY-MM-DD
                if date_str not in messages_by_date:
                    messages_by_date[date_str] = []
                messages_by_date[date_str].append(msg)
        
        # Save messages for each date
        for date_str, date_messages in messages_by_date.items():
            sanitized_channel = self._sanitize_channel_name(channel_name)
            date_dir = self.json_dir / date_str
            date_dir.mkdir(parents=True, exist_ok=True)
            
            file_path = date_dir / f"{sanitized_channel}.json"
            
            # Save as JSON
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(date_messages, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Saved {len(date_messages)} messages to {file_path}")
    
    async def scrape_all_channels(self, days_back: int = 7):
        """Scrape all configured channels"""
        if not await self.initialize_client():
            logger.error("Failed to initialize Telegram client")
            return
        
        total_messages = 0
        
        try:
            for channel in self.all_channels:
                try:
                    logger.info(f"Starting scrape for: {channel}")
                    messages = await self.scrape_channel(channel, days_back)
                    
                    if messages:
                        channel_name = messages[0]['channel_name']
                        self.save_messages_json(messages, channel_name)
                        total_messages += len(messages)
                    
                    # Be nice to the API - add delay between channels
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    logger.error(f"Error processing channel {channel}: {e}")
                    continue
        
        finally:
            await self.client.disconnect()
            logger.info(f"Scraping completed. Total messages scraped: {total_messages}")
    
    def generate_summary_report(self):
        """Generate a summary report of scraped data"""
        summary = {
            "total_channels": len(self.all_channels),
            "scraped_channels": [],
            "total_messages": 0,
            "data_lake_structure": {
                "json_files": [],
                "image_folders": []
            }
        }
        
        # Count JSON files
        json_files = list(self.json_dir.rglob("*.json"))
        summary["json_file_count"] = len(json_files)
        summary["data_lake_structure"]["json_files"] = [str(f) for f in json_files[:10]]  # First 10
        
        # Count images
        image_files = list(self.images_dir.rglob("*.jpg"))
        summary["image_count"] = len(image_files)
        summary["data_lake_structure"]["image_folders"] = [
            str(f.parent) for f in image_files[:5]
        ]  # First 5 folders
        
        # Count messages in JSON files
        total_messages = 0
        for json_file in json_files[:20]:  # Check first 20 files for performance
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    total_messages += len(data)
                    
                    # Get channel names
                    if data:
                        channel = data[0].get('channel_name')
                        if channel and channel not in summary["scraped_channels"]:
                            summary["scraped_channels"].append(channel)
            except:
                continue
        
        summary["total_messages"] = total_messages
        
        # Save summary to logs
        summary_file = Path("logs") / "scraping_summary.json"
        summary_file.parent.mkdir(exist_ok=True)
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Summary report saved to {summary_file}")
        return summary


async def main():
    """Main function to run the scraper"""
    # Get credentials from environment
    api_id = int(os.getenv('API_ID', 0))
    api_hash = os.getenv('API_HASH', '')
    phone_number = os.getenv('PHONE_NONE', '')
    
    if not api_id or not api_hash or not phone_number:
        logger.error("Please set API_ID, API_HASH, and PHONE_NUMBER in config/.env")
        return
    
    # Create scraper instance
    scraper = TelegramScraper(api_id, api_hash, phone_number)
    
    # Scrape all channels (last 30 days by default)
    await scraper.scrape_all_channels(days_back=30)
    
    # Generate summary report
    summary = scraper.generate_summary_report()
    
    # Print summary
    print("\n" + "="*50)
    print("SCRAPING SUMMARY")
    print("="*50)
    print(f"Channels configured: {summary['total_channels']}")
    print(f"Channels successfully scraped: {len(summary['scraped_channels'])}")
    print(f"Total messages scraped: {summary['total_messages']}")
    print(f"JSON files created: {summary['json_file_count']}")
    print(f"Images downloaded: {summary['image_count']}")
    print("="*50)


if __name__ == "__main__":
    # Create logs directory
    Path("logs").mkdir(exist_ok=True)
    
    # Run the scraper
    asyncio.run(main())