"""
Simple script to run the Telegram scraper
"""
import subprocess
import sys

def run_scraper():
    """Run the main scraper script"""
    try:
        # Run the scraper
        print("Starting Telegram scraper...")
        subprocess.run([sys.executable, "src/scraper.py"], check=True)
        print("Scraping completed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"Error running scraper: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
        sys.exit(0)

if __name__ == "__main__":
    run_scraper()