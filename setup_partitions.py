"""Script to set up table partitions for market data."""

import os
from dotenv import load_dotenv
import psycopg2
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_db_config():
    """Get database configuration from environment variables."""
    load_dotenv()
    
    return {
        'host': os.getenv('PGHOST'),
        'database': os.getenv('PGDATABASE'),
        'user': os.getenv('PGUSER'),
        'password': os.getenv('PGPASSWORD'),
        'port': os.getenv('PGPORT'),
        'sslmode': os.getenv('PGSSLMODE')
    }

def setup_partitions():
    """Set up partitions for the bars table."""
    try:
        db_config = get_db_config()
        conn = psycopg2.connect(**db_config)
        
        with conn:
            with conn.cursor() as cur:
                # First, modify the timeframe column to support longer values
                logger.info("Modifying timeframe column length...")
                cur.execute(
                    "ALTER TABLE bars ALTER COLUMN timeframe TYPE VARCHAR(10)"
                )
                
                # Create partitions for each timeframe
                timeframes = ['5m', '15m', '30m', '60m', 'daily', 'weekly']
                
                for timeframe in timeframes:
                    logger.info(f"Creating partition for {timeframe} data...")
                    cur.execute(
                        f"""
                        CREATE TABLE IF NOT EXISTS bars_{timeframe} 
                        PARTITION OF bars 
                        FOR VALUES IN ('{timeframe}')
                        """
                    )
                
                # Create indexes on each partition
                for timeframe in timeframes:
                    logger.info(f"Creating indexes for {timeframe} partition...")
                    cur.execute(
                        f"""
                        CREATE INDEX IF NOT EXISTS idx_bars_{timeframe}_instrument_time 
                        ON bars_{timeframe} (instrument_id, timestamp)
                        """
                    )
                
                logger.info("All partitions and indexes created successfully")
        
    except Exception as e:
        logger.error(f"Error setting up partitions: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    setup_partitions() 