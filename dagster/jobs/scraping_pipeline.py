"""
Dagster job for Telegram scraping pipeline (Task 1)
"""
from dagster import job, In, Nothing
from dagster.ops.scraping_ops import scrape_telegram_data, validate_scraped_data

@job(
    description="Orchestrate Telegram data scraping pipeline",
    tags={"pipeline": "scraping", "task": "1"}
)
def scraping_pipeline():
    """
    Job to scrape data from Telegram channels
    
    This job:
    1. Scrapes data from configured Telegram channels
    2. Downloads images
    3. Saves data to JSON files
    4. Validates the scraped data
    """
    # Scrape Telegram data
    scraping_results = scrape_telegram_data()
    
    # Validate scraped data
    validate_scraped_data(scraping_results)