"""Market data loader module for the backtesting system.

This module handles the ingestion of market data while strictly adhering to:
- Database centricity
- Schema immutability
- Transactional integrity
- Performance consciousness
- Validation rigidity
"""

import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
from datetime import datetime
from typing import Dict, List, Optional, Union
import json

class MarketDataLoader:
    """Handles market data loading with strict adherence to system requirements."""
    
    def __init__(self, db_config: Dict[str, str]):
        """Initialize the data loader with database configuration.
        
        Args:
            db_config: Database connection parameters
        """
        self.db_config = db_config
        self.logger = logging.getLogger(__name__)
        
        # Configure logging for audit trail
        self.logger.setLevel(logging.INFO)
        handler = logging.FileHandler('logs/market_data_loader.log')
        handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        self.logger.addHandler(handler)
    
    def get_connection(self, isolation_level: Optional[str] = None) -> psycopg2.extensions.connection:
        """Get a database connection with specified isolation level.
        
        Args:
            isolation_level: PostgreSQL isolation level
            
        Returns:
            Database connection
            
        Raises:
            Exception: If connection fails
        """
        try:
            conn = psycopg2.connect(**self.db_config)
            if isolation_level:
                conn.set_session(isolation_level=isolation_level)
            return conn
        except Exception as e:
            self.logger.error(f"Database connection error: {e}")
            self._log_system_error("connection_error", str(e))
            raise
    
    def _log_system_error(self, error_type: str, error_message: str) -> None:
        """Log error to system_logs table.
        
        Args:
            error_type: Type of error
            error_message: Error message
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO system_logs (log_level, component, message, details)
                        VALUES (%s, %s, %s, %s)
                        """,
                        ('ERROR', 'market_data_loader', error_type, 
                         json.dumps({'message': error_message}))
                    )
        except Exception as e:
            self.logger.error(f"Failed to log error to system_logs: {e}")
    
    def _log_audit_trail(self, action: str, entity_type: str, 
                        entity_id: int, old_values: Optional[Dict] = None, 
                        new_values: Optional[Dict] = None) -> None:
        """Log audit trail entry.
        
        Args:
            action: Action performed
            entity_type: Type of entity modified
            entity_id: ID of the entity
            old_values: Previous values
            new_values: New values
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO audit_trails (
                            action, entity_type, entity_id, 
                            old_values, new_values
                        ) VALUES (%s, %s, %s, %s, %s)
                        """,
                        (action, entity_type, entity_id,
                         json.dumps(old_values) if old_values else None,
                         json.dumps(new_values) if new_values else None)
                    )
        except Exception as e:
            self.logger.error(f"Failed to log audit trail: {e}")
    
    def validate_market_data(self, data: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """Validate market data according to system requirements.
        
        Args:
            data: Market data DataFrame
            timeframe: Data timeframe
            
        Returns:
            Validated DataFrame
            
        Raises:
            ValueError: If validation fails
        """
        required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        
        # Check required columns
        missing_cols = [col for col in required_columns if col not in data.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Ensure timestamp index
        if not isinstance(data.index, pd.DatetimeIndex):
            try:
                data.index = pd.to_datetime(data.index)
            except:
                raise ValueError("Index must be convertible to datetime")
        
        # Validate data types and ranges
        if not all(data[col].dtype.kind in 'fc' for col in required_columns[:4]):
            raise ValueError("Price columns must be numeric")
        
        if not data['Volume'].dtype.kind in 'i':
            data['Volume'] = data['Volume'].astype(int)
        
        # Validate price relationships
        invalid_bars = (
            (data['High'] < data['Low']) |
            (data['Open'] > data['High']) |
            (data['Open'] < data['Low']) |
            (data['Close'] > data['High']) |
            (data['Close'] < data['Low'])
        )
        if invalid_bars.any():
            raise ValueError(f"Found {invalid_bars.sum()} bars with invalid OHLC relationships")
        
        # Validate volume
        if (data['Volume'] < 0).any():
            raise ValueError("Found negative volume values")
        
        return data
    
    def load_market_data(self, file_path: str, symbol: str, 
                        exchange: str = "DEFAULT") -> None:
        """Load market data from CSV file into the database.
        
        Args:
            file_path: Path to CSV file
            symbol: Instrument symbol
            exchange: Exchange name
            
        Raises:
            Exception: If data loading fails
        """
        try:
            # Extract timeframe from filename
            timeframe = file_path.split('_')[1].replace('min', 'm').replace('data.csv', '')
            
            # Read and validate data
            data = pd.read_csv(file_path, index_col=0)
            data = self.validate_market_data(data, timeframe)
            
            # Use SERIALIZABLE isolation for critical data ingestion
            with self.get_connection(isolation_level='SERIALIZABLE') as conn:
                with conn.cursor() as cur:
                    # Get or create instrument
                    cur.execute(
                        """
                        INSERT INTO instruments (
                            symbol, exchange, instrument_type, 
                            tick_size, lot_size, trading_hours
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (symbol, exchange) 
                        DO UPDATE SET 
                            tick_size = EXCLUDED.tick_size,
                            lot_size = EXCLUDED.lot_size
                        RETURNING instrument_id
                        """,
                        (
                            symbol, exchange, 'stock', 0.01, 100,
                            json.dumps({
                                'timezone': 'America/New_York',
                                'sessions': [{'start': '09:30', 'end': '16:00'}]
                            })
                        )
                    )
                    instrument_id = cur.fetchone()[0]
                    
                    # Prepare bar data for bulk insertion
                    bars_data = []
                    for timestamp, row in data.iterrows():
                        bars_data.append((
                            instrument_id,
                            pd.to_datetime(timestamp),
                            timeframe,
                            float(row['Open']),
                            float(row['High']),
                            float(row['Low']),
                            float(row['Close']),
                            int(row['Volume']),
                            None,  # vwap
                            None   # trades
                        ))
                    
                    # Bulk insert using execute_values
                    execute_values(
                        cur,
                        """
                        INSERT INTO bars (
                            instrument_id, timestamp, timeframe,
                            open, high, low, close, volume,
                            vwap, trades
                        ) VALUES %s
                        ON CONFLICT (bar_id, timestamp, timeframe) DO NOTHING
                        """,
                        bars_data
                    )
                    
                    # Log successful ingestion
                    self._log_audit_trail(
                        'insert', 'bars', instrument_id,
                        new_values={'timeframe': timeframe, 'count': len(bars_data)}
                    )
                    
                    self.logger.info(
                        f"Successfully loaded {len(bars_data)} bars for {symbol} "
                        f"({timeframe}) into database"
                    )
        
        except Exception as e:
            self.logger.error(f"Error loading market data from {file_path}: {e}")
            self._log_system_error("data_ingestion_error", str(e))
            raise 