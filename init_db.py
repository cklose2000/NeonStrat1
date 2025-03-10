"""Database initialization script."""

import os
import psycopg2
from dotenv import load_dotenv
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

def init_database():
    """Initialize the database with the required schema."""
    try:
        # Get database configuration
        db_config = get_db_config()
        
        # Connect to database
        conn = psycopg2.connect(**db_config)
        
        # Drop existing tables
        with conn:
            cursor = conn.cursor()
            try:
                logger.info("Dropping existing tables...")
                cursor.execute("""
                    DROP TABLE IF EXISTS 
                        audit_trails,
                        system_logs,
                        performance_metrics,
                        positions,
                        portfolio_snapshots,
                        trades,
                        orders,
                        backtest_sessions,
                        parameter_sets,
                        strategy_parameters,
                        strategies,
                        bars_5m,
                        bars_15m,
                        bars_30m,
                        bars_60m,
                        bars_daily,
                        bars_weekly,
                        bars,
                        tick_data,
                        instruments
                    CASCADE
                """)
                logger.info("Existing tables dropped successfully.")
            finally:
                cursor.close()
        
        # Read schema file
        with open('schema.sql', 'r') as f:
            schema_sql = f.read()
        
        # Execute schema creation
        with conn:
            cursor = conn.cursor()
            try:
                logger.info("Creating database schema...")
                cursor.execute(schema_sql)
                logger.info("Database schema created successfully.")
            finally:
                cursor.close()
        
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    init_database() 