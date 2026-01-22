"""
Dagster operations for Telegram scraping (Task 1)
"""
import json
import asyncio
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

from dagster import op, Out, Output, Failure, RetryPolicy
from telethon import TelegramClient
from telethon.errors import FloodWaitError, ChannelPrivateError

@op(
    required_resource_keys={"telegram", "database"},
    description="Scrape data from Telegram channels",
    tags={"task": "scraping", "component": "telegram"},
    retry_policy=RetryPolicy(max_retries=3, delay=5)
)
def scrape_telegram_data(context) -> Output[Dict[str, Any]]:
    """
    Operation to scrape data from Telegram channels
    
    Returns:
        Dictionary with scraping results and metadata
    """
    telegram = context.resources.telegram
    db = context.resources.database
    
    # Initialize Telegram client
    if not telegram.initialize():
        raise Failure("Failed to initialize Telegram client")
    
    context.log.info(f"Starting Telegram scraping for {len(telegram.channels)} channels")
    
    # Create data directories
    data_dir = Path("data")
    raw_dir = data_dir / "raw"
    json_dir = raw_dir / "telegram_messages"
    images_dir = raw_dir / "images"
    
    json_dir.mkdir(parents=True, exist_ok=True)
    images_dir.mkdir(parents=True, exist_ok=True)
    
    results = {
        "timestamp": datetime.now().isoformat(),
        "channels_scraped": [],
        "total_messages": 0,
        "total_images": 0,
        "errors": []
    }
    
    async def run_scraping():
        """Async function to run scraping"""
        client = telegram.client
        await client.start(phone=telegram.phone_number)
        
        for channel_identifier in telegram.channels:
            try:
                context.log.info(f"Scraping channel: {channel_identifier}")
                
                # Get channel entity
                try:
                    channel = await client.get_entity(channel_identifier)
                    channel_name = getattr(channel, 'title', channel_identifier)
                except ChannelPrivateError:
                    context.log.warning(f"Channel {channel_identifier} is private, skipping")
                    results["errors"].append(f"Private channel: {channel_identifier}")
                    continue
                
                # Calculate date limit
                from datetime import timedelta
                date_limit = datetime.now() - timedelta(days=telegram.days_back)
                
                # Scrape messages
                messages = []
                async for message in client.iter_messages(
                    channel, 
                    limit=100,  # Limit for demo
                    offset_date=date_limit
                ):
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
                    
                    # Download image if present
                    if message.media and hasattr(message.media, 'photo'):
                        sanitized_name = ''.join(
                            c for c in channel_name if c.isalnum() or c in ('_', '-')
                        ).rstrip()
                        
                        image_dir = images_dir / sanitized_name
                        image_dir.mkdir(parents=True, exist_ok=True)
                        
                        image_filename = f"{message.id}.jpg"
                        image_path = image_dir / image_filename
                        
                        try:
                            await client.download_media(message.media, file=str(image_path))
                            message_data['image_path'] = str(image_path)
                            results["total_images"] += 1
                        except Exception as e:
                            context.log.warning(f"Failed to download image: {e}")
                    
                    messages.append(message_data)
                    results["total_messages"] += 1
                
                # Save messages to JSON
                if messages:
                    # Group by date
                    from collections import defaultdict
                    messages_by_date = defaultdict(list)
                    
                    for msg in messages:
                        if msg.get('message_date'):
                            date_str = msg['message_date'][:10]  # Extract YYYY-MM-DD
                            messages_by_date[date_str].append(msg)
                    
                    # Save for each date
                    for date_str, date_messages in messages_by_date.items():
                        sanitized_channel = ''.join(
                            c for c in channel_name if c.isalnum() or c in ('_', '-')
                        ).rstrip()
                        
                        date_dir = json_dir / date_str
                        date_dir.mkdir(parents=True, exist_ok=True)
                        
                        file_path = date_dir / f"{sanitized_channel}.json"
                        
                        with open(file_path, 'w', encoding='utf-8') as f:
                            json.dump(date_messages, f, ensure_ascii=False, indent=2)
                    
                    results["channels_scraped"].append({
                        "name": channel_name,
                        "messages": len(messages),
                        "images": sum(1 for m in messages if m['image_path'])
                    })
                    
                    context.log.info(f"Scraped {len(messages)} messages from {channel_name}")
                
                # Be nice to the API
                await asyncio.sleep(2)
                
            except FloodWaitError as e:
                context.log.warning(f"Flood wait required: {e.seconds} seconds")
                await asyncio.sleep(e.seconds + 5)
                continue
            except Exception as e:
                context.log.error(f"Error scraping channel {channel_identifier}: {e}")
                results["errors"].append(f"Error in {channel_identifier}: {str(e)}")
                continue
        
        await client.disconnect()
    
    # Run async scraping
    try:
        asyncio.run(run_scraping())
    except Exception as e:
        raise Failure(f"Scraping failed: {e}")
    
    # Log results
    context.log.info(f"Scraping completed: {results['total_messages']} messages, "
                    f"{results['total_images']} images")
    
    # Save scraping summary
    summary_path = Path("logs") / "scraping_summary.json"
    summary_path.parent.mkdir(exist_ok=True)
    
    with open(summary_path, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    # Update database with scraping metadata
    try:
        with db.get_session() as session:
            session.execute(
                "INSERT INTO pipeline.metadata (task_name, status, records_processed, timestamp) "
                "VALUES (:task, :status, :records, :timestamp)",
                {
                    "task": "telegram_scraping",
                    "status": "completed",
                    "records": results["total_messages"],
                    "timestamp": datetime.now()
                }
            )
    except Exception as e:
        context.log.warning(f"Failed to update database metadata: {e}")
    
    return Output(results, "scraping_results")


@op(
    required_resource_keys={"database"},
    description="Validate scraped data quality",
    tags={"task": "scraping", "component": "validation"}
)
def validate_scraped_data(context, scraping_results: Dict[str, Any]) -> Output[Dict[str, Any]]:
    """
    Validate the quality of scraped data
    
    Args:
        scraping_results: Results from scrape_telegram_data operation
    
    Returns:
        Validation results
    """
    context.log.info("Validating scraped data...")
    
    validation_results = {
        "timestamp": datetime.now().isoformat(),
        "checks": [],
        "passed": True,
        "summary": {}
    }
    
    # Check 1: Verify channels were scraped
    if len(scraping_results["channels_scraped"]) == 0:
        validation_results["checks"].append({
            "name": "channels_scraped",
            "passed": False,
            "message": "No channels were successfully scraped"
        })
        validation_results["passed"] = False
    else:
        validation_results["checks"].append({
            "name": "channels_scraped",
            "passed": True,
            "message": f"{len(scraping_results['channels_scraped'])} channels scraped"
        })
    
    # Check 2: Verify message count
    if scraping_results["total_messages"] == 0:
        validation_results["checks"].append({
            "name": "message_count",
            "passed": False,
            "message": "No messages were scraped"
        })
        validation_results["passed"] = False
    else:
        validation_results["checks"].append({
            "name": "message_count",
            "passed": True,
            "message": f"{scraping_results['total_messages']} messages scraped"
        })
    
    # Check 3: Check for errors
    if scraping_results["errors"]:
        validation_results["checks"].append({
            "name": "scraping_errors",
            "passed": False,
            "message": f"{len(scraping_results['errors'])} errors occurred during scraping",
            "errors": scraping_results["errors"][:5]  # Limit to first 5 errors
        })
        # Don't fail validation for errors, just warn
    else:
        validation_results["checks"].append({
            "name": "scraping_errors",
            "passed": True,
            "message": "No scraping errors"
        })
    
    # Check 4: Verify data directory structure
    data_dir = Path("data/raw/telegram_messages")
    if data_dir.exists():
        json_files = list(data_dir.rglob("*.json"))
        validation_results["checks"].append({
            "name": "data_files",
            "passed": len(json_files) > 0,
            "message": f"{len(json_files)} JSON files created",
            "files": [str(f.relative_to(Path("data"))) for f in json_files[:3]]
        })
        validation_results["summary"]["json_files"] = len(json_files)
    else:
        validation_results["checks"].append({
            "name": "data_files",
            "passed": False,
            "message": "Data directory not created"
        })
        validation_results["passed"] = False
    
    # Save validation results
    validation_path = Path("logs") / "scraping_validation.json"
    with open(validation_path, 'w') as f:
        json.dump(validation_results, f, indent=2, default=str)
    
    context.log.info(f"Validation {'passed' if validation_results['passed'] else 'failed'}")
    
    return Output(validation_results, "validation_results")