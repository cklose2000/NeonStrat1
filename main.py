import os
from dotenv import load_dotenv
import logging
from datetime import datetime, timedelta
from src.data_ingestion import DataIngestionModule
from src.strategy import MovingAverageCrossover, RSIStrategy, BollingerBandsStrategy
from src.backtest import BacktestSimulator

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
    """Main function to demonstrate the backtesting system."""
    try:
        # Get database configuration
        db_config = get_db_config()
        
        # Initialize modules
        data_ingestion = DataIngestionModule(db_config)
        simulator = BacktestSimulator(db_config)
        
        # Example: Run backtest for different strategies
        strategies = [
            (MovingAverageCrossover, {'short_window': 10, 'long_window': 50}),
            (RSIStrategy, {'rsi_period': 14, 'overbought_level': 70, 'oversold_level': 30}),
            (BollingerBandsStrategy, {'window': 20, 'num_std': 2})
        ]
        
        # Example market data (you would typically load this from a data source)
        import pandas as pd
        import numpy as np
        
        # Generate sample data
        dates = pd.date_range(start='2023-01-01', end='2023-12-31', freq='D')
        prices = np.random.random(len(dates)) * 10 + 100  # Random prices between 100 and 110
        volumes = np.random.randint(1000, 10000, len(dates))
        
        sample_data = pd.DataFrame({
            'symbol': ['AAPL'] * len(dates),
            'timestamp': dates,
            'price': prices,
            'volume': volumes
        })
        
        # Ingest sample data
        logger.info("Ingesting sample market data...")
        data_ingestion.ingest_tick_data(sample_data)
        
        # Aggregate to daily bars
        logger.info("Aggregating to daily bars...")
        data_ingestion.aggregate_to_bars('1d')
        
        # Run backtests for each strategy
        for strategy_class, parameters in strategies:
            logger.info(f"Running backtest for {strategy_class.__name__}...")
            
            # Create strategy in database (you would typically do this once)
            with simulator.connect_to_db() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO strategies (name, version, description)
                    VALUES (%s, %s, %s)
                    RETURNING strategy_id
                    """,
                    (strategy_class.__name__, '1.0', f"Example {strategy_class.__name__} strategy")
                )
                strategy_id = cursor.fetchone()[0]
                
                # Create parameter set
                cursor.execute(
                    """
                    INSERT INTO parameter_sets (strategy_id, name, parameters)
                    VALUES (%s, %s, %s)
                    RETURNING set_id
                    """,
                    (strategy_id, 'default', parameters)
                )
                parameter_set_id = cursor.fetchone()[0]
                
                # Get instrument ID (assuming it exists from data ingestion)
                cursor.execute(
                    "SELECT instrument_id FROM instruments WHERE symbol = %s",
                    ('AAPL',)
                )
                instrument_id = cursor.fetchone()[0]
            
            # Run backtest
            session_id = simulator.run_backtest(
                strategy_class=strategy_class,
                strategy_id=strategy_id,
                parameter_set_id=parameter_set_id,
                instrument_id=instrument_id,
                start_date='2023-01-01',
                end_date='2023-12-31',
                timeframe='1d',
                initial_capital=100000.0,
                commission_model={'type': 'percentage', 'percentage': 0.1},
                slippage_model={'type': 'percentage', 'percentage': 0.05},
                parameters=parameters
            )
            
            logger.info(f"Backtest completed. Session ID: {session_id}")
    
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise

if __name__ == "__main__":
    main() 