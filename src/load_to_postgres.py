import os
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, DateTime, Boolean, JSON
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# Load environment variables
load_dotenv('config/.env')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/load_to_postgres.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PostgresLoader:
    def __init__(self):
        """Initialize PostgreSQL loader with connection"""
        # Database configuration from environment
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'medical_warehouse'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'postgres123')
        }
        
        # Create connection string
        self.connection_string = (
            f"postgresql://{self.db_config['user']}:{self.db_config['password']}"
            f"@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
        )
        
        # Base directories
        self.data_dir = Path("data")
        self.raw_json_dir = self.data_dir / "raw" / "telegram_messages"
        
        # Ensure logs directory exists
        Path("logs").mkdir(exist_ok=True)
        
        logger.info(f"Initialized PostgresLoader for database: {self.db_config['database']}")
    
    def create_engine_connection(self):
        """Create SQLAlchemy engine"""
        try:
            engine = create_engine(self.connection_string)
            logger.info("Database engine created successfully")
            return engine
        except Exception as e:
            logger.error(f"Failed to create database engine: {e}")
            raise
    
    def create_raw_schema(self, engine):
        """Create raw schema and table if they don't exist"""
        try:
            with engine.connect() as conn:
                # Create raw schema
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
                logger.info("Created raw schema if it didn't exist")
                
                # Create raw.telegram_messages table
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS raw.telegram_messages (
                    id SERIAL PRIMARY KEY,
                    message_id BIGINT NOT NULL,
                    channel_name VARCHAR(255),
                    message_date TIMESTAMP,
                    message_text TEXT,
                    has_media BOOLEAN DEFAULT FALSE,
                    image_path VARCHAR(500),
                    views INTEGER DEFAULT 0,
                    forwards INTEGER DEFAULT 0,
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    raw_data JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(message_id, channel_name)
                );
                """
                conn.execute(text(create_table_sql))
                
                # Create indexes for better query performance
                index_sqls = [
                    "CREATE INDEX IF NOT EXISTS idx_message_date ON raw.telegram_messages(message_date);",
                    "CREATE INDEX IF NOT EXISTS idx_channel_name ON raw.telegram_messages(channel_name);",
                    "CREATE INDEX IF NOT EXISTS idx_has_media ON raw.telegram_messages(has_media);"
                ]
                
                for sql in index_sqls:
                    conn.execute(text(sql))
                
                conn.commit()
                logger.info("Created raw.telegram_messages table with indexes")
                
        except SQLAlchemyError as e:
            logger.error(f"Error creating raw schema: {e}")
            raise
    
    def find_json_files(self) -> List[Path]:
        """Find all JSON files in the data lake"""
        json_files = list(self.raw_json_dir.rglob("*.json"))
        logger.info(f"Found {len(json_files)} JSON files in data lake")
        return json_files
    
    def load_json_file(self, json_file: Path) -> List[Dict]:
        """Load and parse a JSON file"""
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Add source file information
            for record in data:
                record['source_file'] = str(json_file)
                record['load_batch'] = datetime.now().isoformat()
            
            logger.debug(f"Loaded {len(data)} records from {json_file}")
            return data
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in {json_file}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error loading {json_file}: {e}")
            return []
    
    def transform_record(self, record: Dict) -> Dict:
        """Transform record for database insertion"""
        try:
            # Extract message date
            message_date = None
            if record.get('message_date'):
                try:
                    message_date = datetime.fromisoformat(record['message_date'].replace('Z', '+00:00'))
                except:
                    # Try different date formats
                    for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d']:
                        try:
                            message_date = datetime.strptime(record['message_date'], fmt)
                            break
                        except:
                            continue
            
            # Extract scraped_at
            scraped_at = None
            if record.get('scraped_at'):
                try:
                    scraped_at = datetime.fromisoformat(record['scraped_at'].replace('Z', '+00:00'))
                except:
                    scraped_at = datetime.now()
            
            # Clean text
            message_text = record.get('message_text', '')
            if message_text:
                message_text = message_text.replace('\x00', '')  # Remove null bytes
            
            # Transform data for database
            transformed = {
                'message_id': int(record.get('message_id', 0)),
                'channel_name': record.get('channel_name', 'unknown'),
                'message_date': message_date,
                'message_text': message_text[:10000],  # Limit text length
                'has_media': bool(record.get('has_media', False)),
                'image_path': record.get('image_path'),
                'views': int(record.get('views', 0)),
                'forwards': int(record.get('forwards', 0)),
                'scraped_at': scraped_at or datetime.now(),
                'raw_data': json.dumps(record)  # Store complete raw data as JSONB
            }
            
            return transformed
            
        except Exception as e:
            logger.error(f"Error transforming record: {e}")
            # Return minimal safe record
            return {
                'message_id': 0,
                'channel_name': 'error',
                'message_date': datetime.now(),
                'message_text': 'ERROR IN TRANSFORMATION',
                'has_media': False,
                'views': 0,
                'forwards': 0,
                'scraped_at': datetime.now(),
                'raw_data': json.dumps({'error': str(e)})
            }
    
    def insert_records(self, engine, records: List[Dict]):
        """Insert records into PostgreSQL"""
        if not records:
            logger.warning("No records to insert")
            return 0
        
        try:
            # Create DataFrame for bulk insert
            df = pd.DataFrame(records)
            
            # Use SQLAlchemy to insert
            with engine.begin() as conn:  # Auto-commit/rollback
                # Use insert with conflict handling (update on duplicate)
                for index, row in df.iterrows():
                    insert_sql = text("""
                    INSERT INTO raw.telegram_messages 
                    (message_id, channel_name, message_date, message_text, has_media, 
                     image_path, views, forwards, scraped_at, raw_data)
                    VALUES (:message_id, :channel_name, :message_date, :message_text, :has_media,
                            :image_path, :views, :forwards, :scraped_at, :raw_data::jsonb)
                    ON CONFLICT (message_id, channel_name) 
                    DO UPDATE SET
                        message_date = EXCLUDED.message_date,
                        message_text = EXCLUDED.message_text,
                        views = EXCLUDED.views,
                        forwards = EXCLUDED.forwards,
                        updated_at = CURRENT_TIMESTAMP,
                        raw_data = EXCLUDED.raw_data
                    """)
                    
                    conn.execute(insert_sql, {
                        'message_id': int(row['message_id']),
                        'channel_name': str(row['channel_name']),
                        'message_date': row['message_date'],
                        'message_text': str(row['message_text']),
                        'has_media': bool(row['has_media']),
                        'image_path': row.get('image_path'),
                        'views': int(row['views']),
                        'forwards': int(row['forwards']),
                        'scraped_at': row['scraped_at'],
                        'raw_data': row['raw_data']
                    })
                
                inserted_count = len(df)
                logger.info(f"Inserted/updated {inserted_count} records")
                return inserted_count
                
        except Exception as e:
            logger.error(f"Error inserting records: {e}")
            # Fallback: try individual inserts
            success_count = 0
            for record in records:
                try:
                    with engine.begin() as conn:
                        insert_sql = text("""
                        INSERT INTO raw.telegram_messages 
                        (message_id, channel_name, message_date, message_text, has_media, 
                         image_path, views, forwards, scraped_at, raw_data)
                        VALUES (:message_id, :channel_name, :message_date, :message_text, :has_media,
                                :image_path, :views, :forwards, :scraped_at, :raw_data::jsonb)
                        ON CONFLICT (message_id, channel_name) DO NOTHING
                        """)
                        
                        conn.execute(insert_sql, record)
                        success_count += 1
                        
                except Exception as single_error:
                    logger.error(f"Failed to insert record {record.get('message_id')}: {single_error}")
            
            logger.info(f"Fallback insert: {success_count}/{len(records)} records inserted")
            return success_count
    
    def validate_load(self, engine) -> Dict:
        """Validate the loaded data"""
        try:
            with engine.connect() as conn:
                # Get counts
                total_records = conn.execute(
                    text("SELECT COUNT(*) FROM raw.telegram_messages")
                ).scalar()
                
                channels = conn.execute(
                    text("SELECT COUNT(DISTINCT channel_name) FROM raw.telegram_messages")
                ).scalar()
                
                dates = conn.execute(
                    text("SELECT COUNT(DISTINCT DATE(message_date)) FROM raw.telegram_messages")
                ).scalar()
                
                with_media = conn.execute(
                    text("SELECT COUNT(*) FROM raw.telegram_messages WHERE has_media = TRUE")
                ).scalar()
                
                validation_results = {
                    'total_records': total_records or 0,
                    'unique_channels': channels or 0,
                    'unique_dates': dates or 0,
                    'messages_with_media': with_media or 0,
                    'validation_timestamp': datetime.now().isoformat()
                }
                
                logger.info(f"Validation results: {validation_results}")
                return validation_results
                
        except Exception as e:
            logger.error(f"Error during validation: {e}")
            return {'error': str(e)}
    
    def run(self):
        """Main execution method"""
        logger.info("Starting PostgreSQL data load process")
        
        try:
            # Create engine
            engine = self.create_engine_connection()
            
            # Create raw schema and table
            self.create_raw_schema(engine)
            
            # Find and process JSON files
            json_files = self.find_json_files()
            total_loaded = 0
            
            for json_file in json_files:
                logger.info(f"Processing: {json_file}")
                
                # Load JSON data
                raw_records = self.load_json_file(json_file)
                
                if not raw_records:
                    continue
                
                # Transform records
                transformed_records = [self.transform_record(r) for r in raw_records]
                
                # Insert into PostgreSQL
                inserted = self.insert_records(engine, transformed_records)
                total_loaded += inserted
                
                logger.info(f"Loaded {inserted} records from {json_file}")
            
            # Validate the load
            validation = self.validate_load(engine)
            
            # Save validation results
            validation_file = Path("logs") / "postgres_load_validation.json"
            with open(validation_file, 'w', encoding='utf-8') as f:
                json.dump(validation, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Total records loaded: {total_loaded}")
            logger.info(f"Validation saved to: {validation_file}")
            
            return {
                'success': True,
                'total_loaded': total_loaded,
                'validation': validation
            }
            
        except Exception as e:
            logger.error(f"Failed to run PostgreSQL loader: {e}")
            return {
                'success': False,
                'error': str(e)
            }


def main():
    """Main function to run the PostgreSQL loader"""
    loader = PostgresLoader()
    result = loader.run()
    
    # Print summary
    print("\n" + "="*50)
    print("POSTGRES LOAD SUMMARY")
    print("="*50)
    if result['success']:
        print(f"✓ Successfully loaded {result['total_loaded']} records")
        print(f"✓ Validation results:")
        for key, value in result['validation'].items():
            print(f"  - {key}: {value}")
    else:
        print(f"✗ Failed: {result['error']}")
    print("="*50)


if __name__ == "__main__":
    main()