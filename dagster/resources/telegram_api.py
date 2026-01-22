"""
Telegram API resources for Dagster pipeline
"""
import os
from typing import Dict, Any
from dagster import resource, Field, String

@resource(
    config_schema={
        "api_id": Field(String, description="Telegram API ID", is_required=True),
        "api_hash": Field(String, description="Telegram API Hash", is_required=True),
        "phone_number": Field(String, description="Phone number", is_required=True),
        "channels": Field(String, default_value="chemed,lobelia4cosmetics,tikvahpharma", is_required=False),
        "days_back": Field(int, default_value=30, is_required=False),
    }
)
def telegram_resource(context):
    """Resource for Telegram API operations"""
    
    class TelegramClient:
        def __init__(self, config: Dict[str, Any]):
            self.api_id = config["api_id"]
            self.api_hash = config["api_hash"]
            self.phone_number = config["phone_number"]
            self.channels = config["channels"].split(",")
            self.days_back = config["days_back"]
            self.context = context
            self.client = None
        
        def initialize(self):
            """Initialize Telegram client"""
            try:
                from telethon import TelegramClient
                
                self.client = TelegramClient(
                    'medical_scraper_session',
                    int(self.api_id),
                    self.api_hash
                )
                self.context.log.info("Telegram client initialized")
                return True
            except Exception as e:
                self.context.log.error(f"Failed to initialize Telegram client: {e}")
                return False
        
        def get_channels(self):
            """Get list of channels to scrape"""
            return [channel.strip() for channel in self.channels]
        
        def get_config(self) -> Dict[str, Any]:
            """Get configuration as dictionary"""
            return {
                "api_id": self.api_id,
                "api_hash": self.api_hash,
                "phone_number": self.phone_number,
                "channels": self.channels,
                "days_back": self.days_back
            }
    
    return TelegramClient(context.resource_config)