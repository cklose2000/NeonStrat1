"""Script to load SPY market data into the database."""

import os
from dotenv import load_dotenv
import logging
from src.data_loader import MarketDataLoader

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

def main():
    """Load all SPY market data files into the database."""
    try:
        # Create logs directory if it doesn't exist
        os.makedirs('logs', exist_ok=True)
        
        # Initialize data loader
        db_config = get_db_config()
        loader = MarketDataLoader(db_config)
        
        # Get all SPY data files
        data_files = [
            f for f in os.listdir('data')
            if f.startswith('SPY_') and f.endswith('_data.csv')
        ]
        
        # Load each file
        for file_name in sorted(data_files):
            logger.info(f"Processing {file_name}...")
            file_path = os.path.join('data', file_name)
            
            try:
                loader.load_market_data(
                    file_path=file_path,
                    symbol='SPY',
                    exchange='NYSE'
                )
                logger.info(f"Successfully processed {file_name}")
                
            except Exception as e:
                logger.error(f"Error processing {file_name}: {e}")
                # Continue with next file
                continue
        
        logger.info("Market data loading completed")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise

if __name__ == "__main__":
    main() 