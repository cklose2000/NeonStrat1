import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from datetime import datetime
import logging
from typing import Dict, List, Optional, Union
import json

class DataIngestionModule:
    """Module for ingesting and processing market data into the database."""
    
    def __init__(self, db_config: Dict[str, str]):
        """Initialize data ingestion module with database configuration.
        
        Args:
            db_config: Dictionary containing database connection parameters
        """
        self.db_config = db_config
        self.logger = logging.getLogger(__name__)
        
    def connect_to_db(self) -> psycopg2.extensions.connection:
        """Establish connection to PostgreSQL database.
        
        Returns:
            psycopg2.extensions.connection: Database connection object
            
        Raises:
            Exception: If connection fails
        """
        try:
            conn = psycopg2.connect(**self.db_config)
            return conn
        except Exception as e:
            self.logger.error(f"Database connection error: {e}")
            raise
    
    def validate_market_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Validate market data for required fields and data types.
        
        Args:
            data: DataFrame containing market data
            
        Returns:
            pd.DataFrame: Validated and cleaned market data
            
        Raises:
            ValueError: If data validation fails
        """
        required_fields = ['symbol', 'timestamp', 'price', 'volume']
        
        # Check for required fields
        for field in required_fields:
            if field not in data.columns:
                raise ValueError(f"Required field '{field}' missing from market data")
        
        # Validate data types
        if not pd.api.types.is_datetime64_dtype(data['timestamp']):
            try:
                data['timestamp'] = pd.to_datetime(data['timestamp'])
            except:
                raise ValueError("Timestamp field could not be converted to datetime")
        
        # Ensure numeric price and volume
        for field in ['price', 'volume']:
            if not pd.api.types.is_numeric_dtype(data[field]):
                raise ValueError(f"Field '{field}' must be numeric")
        
        # Remove rows with null values in critical fields
        data = data.dropna(subset=required_fields)
        
        return data
    
    def get_instrument_id(self, conn: psycopg2.extensions.connection, 
                         symbol: str, exchange: str = "DEFAULT") -> int:
        """Get instrument ID from database, create if not exists.
        
        Args:
            conn: Database connection
            symbol: Instrument symbol
            exchange: Exchange name (default: "DEFAULT")
            
        Returns:
            int: Instrument ID
        """
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT instrument_id FROM instruments WHERE symbol = %s AND exchange = %s",
                (symbol, exchange)
            )
            result = cursor.fetchone()
            
            if result:
                return result[0]
            else:
                cursor.execute(
                    """
                    INSERT INTO instruments (symbol, exchange, instrument_type, tick_size, lot_size)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING instrument_id
                    """,
                    (symbol, exchange, 'stock', 0.01, 1)  # Default values
                )
                return cursor.fetchone()[0]
        finally:
            cursor.close()
    
    def ingest_tick_data(self, data: pd.DataFrame, source: str = "external_feed") -> None:
        """Ingest tick data into the database.
        
        Args:
            data: DataFrame containing tick data
            source: Data source identifier (default: "external_feed")
            
        Raises:
            Exception: If data ingestion fails
        """
        data = self.validate_market_data(data)
        
        conn = self.connect_to_db()
        try:
            with conn:
                # Get instrument IDs for all symbols
                symbols = data['symbol'].unique()
                instrument_map = {}
                for symbol in symbols:
                    instrument_map[symbol] = self.get_instrument_id(conn, symbol)
                
                # Map symbols to instrument IDs
                data['instrument_id'] = data['symbol'].map(instrument_map)
                
                # Prepare data for bulk insert
                tick_data = []
                for _, row in data.iterrows():
                    tick_data.append((
                        row['instrument_id'],
                        row['timestamp'],
                        row['price'],
                        row['volume'],
                        row.get('bid_price'),
                        row.get('ask_price'),
                        row.get('bid_size'),
                        row.get('ask_size'),
                        row.get('trade_id'),
                        row.get('trade_condition', []),
                        source
                    ))
                
                # Bulk insert
                cursor = conn.cursor()
                try:
                    execute_values(
                        cursor,
                        """
                        INSERT INTO tick_data (
                            instrument_id, timestamp, price, volume,
                            bid_price, ask_price, bid_size, ask_size,
                            trade_id, trade_condition, source
                        ) VALUES %s
                        """,
                        tick_data
                    )
                    self.logger.info(f"Inserted {len(tick_data)} tick records")
                finally:
                    cursor.close()
        except Exception as e:
            self.logger.error(f"Error ingesting tick data: {e}")
            raise
        finally:
            conn.close()
    
    def aggregate_to_bars(self, timeframe: str = '1m') -> None:
        """Aggregate tick data to OHLCV bars.
        
        Args:
            timeframe: Bar timeframe (default: '1m')
            
        Raises:
            Exception: If aggregation fails
        """
        conn = self.connect_to_db()
        try:
            with conn:
                cursor = conn.cursor()
                try:
                    # Find the most recent bar timestamp
                    cursor.execute(
                        f"SELECT MAX(timestamp) FROM bars WHERE timeframe = %s",
                        (timeframe,)
                    )
                    last_timestamp = cursor.fetchone()[0]
                    
                    # Build query based on timeframe
                    interval_sql = {
                        '1m': "1 minute",
                        '5m': "5 minutes",
                        '15m': "15 minutes",
                        '1h': "1 hour",
                        '1d': "1 day"
                    }.get(timeframe, "1 minute")
                    
                    # Aggregate ticks to bars
                    cursor.execute(
                        f"""
                        INSERT INTO bars (
                            instrument_id, timestamp, timeframe,
                            open, high, low, close, volume, vwap, trades
                        )
                        SELECT 
                            instrument_id,
                            date_trunc(%s, timestamp) as bar_time,
                            %s as timeframe,
                            first_value(price) OVER (PARTITION BY instrument_id, date_trunc(%s, timestamp) ORDER BY timestamp) as open,
                            max(price) OVER (PARTITION BY instrument_id, date_trunc(%s, timestamp)) as high,
                            min(price) OVER (PARTITION BY instrument_id, date_trunc(%s, timestamp)) as low,
                            last_value(price) OVER (PARTITION BY instrument_id, date_trunc(%s, timestamp) ORDER BY timestamp) as close,
                            sum(volume) OVER (PARTITION BY instrument_id, date_trunc(%s, timestamp)) as volume,
                            sum(price * volume) OVER (PARTITION BY instrument_id, date_trunc(%s, timestamp)) / 
                                nullif(sum(volume) OVER (PARTITION BY instrument_id, date_trunc(%s, timestamp)), 0) as vwap,
                            count(*) OVER (PARTITION BY instrument_id, date_trunc(%s, timestamp)) as trades
                        FROM tick_data
                        WHERE timestamp > %s
                        GROUP BY instrument_id, date_trunc(%s, timestamp), price, volume
                        ORDER BY instrument_id, bar_time
                        """,
                        ('minute', timeframe, 'minute', 'minute', 'minute', 'minute', 
                         'minute', 'minute', 'minute', 'minute', last_timestamp or '1970-01-01', 'minute')
                    )
                    
                    self.logger.info(f"Aggregated tick data to {timeframe} bars")
                finally:
                    cursor.close()
        except Exception as e:
            self.logger.error(f"Error aggregating to bars: {e}")
            raise
        finally:
            conn.close() 