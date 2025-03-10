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

def test_connection():
    """Test connection to NeonDB."""
    load_dotenv()
    
    # Get connection parameters from environment
    db_params = {
        'host': os.getenv('PGHOST'),
        'database': os.getenv('PGDATABASE'),
        'user': os.getenv('PGUSER'),
        'password': os.getenv('PGPASSWORD'),
        'port': os.getenv('PGPORT'),
        'sslmode': os.getenv('PGSSLMODE')
    }
    
    try:
        logger.info("Attempting to connect to NeonDB...")
        conn = psycopg2.connect(**db_params)
        
        # Test the connection by executing a simple query
        with conn.cursor() as cur:
            cur.execute('SELECT version();')
            version = cur.fetchone()
            logger.info(f"Successfully connected to PostgreSQL. Version: {version[0]}")
        
        conn.close()
        logger.info("Database connection test completed successfully.")
        return True
        
    except Exception as e:
        logger.error(f"Error connecting to the database: {e}")
        return False

if __name__ == "__main__":
    test_connection() 