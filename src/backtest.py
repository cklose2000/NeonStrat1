import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime
import json
import logging
from typing import Dict, Any, Optional, Type
from .strategy import Strategy

class BacktestSimulator:
    """Backtesting engine to simulate strategy execution."""
    
    def __init__(self, db_config: Dict[str, str]):
        """Initialize backtesting engine with database configuration.
        
        Args:
            db_config: Dictionary containing database connection parameters
        """
        self.db_config = db_config
        self.logger = logging.getLogger(__name__)
    
    def connect_to_db(self) -> psycopg2.extensions.connection:
        """Connect to PostgreSQL database.
        
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
    
    def load_market_data(self, instrument_id: int, start_date: str, 
                        end_date: str, timeframe: str = '1d') -> pd.DataFrame:
        """Load market data for backtesting.
        
        Args:
            instrument_id: ID of the instrument
            start_date: Start date for data
            end_date: End date for data
            timeframe: Bar timeframe (default: '1d')
            
        Returns:
            pd.DataFrame: Market data for backtesting
            
        Raises:
            Exception: If data loading fails
        """
        conn = self.connect_to_db()
        try:
            query = """
                SELECT timestamp, open, high, low, close, volume, vwap
                FROM bars
                WHERE instrument_id = %s 
                AND timeframe = %s
                AND timestamp BETWEEN %s AND %s
                ORDER BY timestamp
            """
            
            df = pd.read_sql_query(
                query, 
                conn, 
                params=(instrument_id, timeframe, start_date, end_date)
            )
            
            return df
        except Exception as e:
            self.logger.error(f"Error loading market data: {e}")
            raise
        finally:
            conn.close()
    
    def create_backtest_session(self, strategy_id: int, parameter_set_id: int, 
                              instrument_id: int, start_date: str, end_date: str, 
                              timeframe: str, initial_capital: float,
                              commission_model: Optional[Dict] = None, 
                              slippage_model: Optional[Dict] = None) -> int:
        """Create a new backtest session in the database.
        
        Args:
            strategy_id: ID of the strategy
            parameter_set_id: ID of the parameter set
            instrument_id: ID of the instrument
            start_date: Start date for backtest
            end_date: End date for backtest
            timeframe: Bar timeframe
            initial_capital: Initial capital for backtest
            commission_model: Commission model configuration
            slippage_model: Slippage model configuration
            
        Returns:
            int: Session ID
            
        Raises:
            Exception: If session creation fails
        """
        conn = self.connect_to_db()
        try:
            with conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(
                        """
                        INSERT INTO backtest_sessions (
                            strategy_id, parameter_set_id, instrument_id,
                            start_date, end_date, timeframe, initial_capital,
                            commission_model, slippage_model, status, created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING session_id
                        """,
                        (
                            strategy_id, parameter_set_id, instrument_id,
                            start_date, end_date, timeframe, initial_capital,
                            json.dumps(commission_model) if commission_model else None,
                            json.dumps(slippage_model) if slippage_model else None,
                            'running', datetime.now()
                        )
                    )
                    session_id = cursor.fetchone()[0]
                    return session_id
                finally:
                    cursor.close()
        except Exception as e:
            self.logger.error(f"Error creating backtest session: {e}")
            raise
        finally:
            conn.close()
    
    def record_order(self, session_id: int, timestamp: datetime, instrument_id: int, 
                    order_type: str, side: str, quantity: int, price: Optional[float] = None, 
                    time_in_force: str = 'day') -> int:
        """Record a simulated order in the database.
        
        Args:
            session_id: ID of the backtest session
            timestamp: Order timestamp
            instrument_id: ID of the instrument
            order_type: Type of order
            side: Order side (buy/sell)
            quantity: Order quantity
            price: Order price
            time_in_force: Time in force for the order
            
        Returns:
            int: Order ID
            
        Raises:
            Exception: If order recording fails
        """
        conn = self.connect_to_db()
        try:
            with conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(
                        """
                        INSERT INTO orders (
                            session_id, timestamp, instrument_id, order_type,
                            side, quantity, price, status, time_in_force, submit_time
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING order_id
                        """,
                        (
                            session_id, timestamp, instrument_id, order_type,
                            side, quantity, price, 'pending', time_in_force, 
                            datetime.now()
                        )
                    )
                    order_id = cursor.fetchone()[0]
                    return order_id
                finally:
                    cursor.close()
        except Exception as e:
            self.logger.error(f"Error recording order: {e}")
            raise
        finally:
            conn.close()
    
    def execute_order(self, order_id: int, price: float, quantity: int, 
                     commission: float = 0.0, slippage: float = 0.0) -> int:
        """Execute a simulated order and record the trade.
        
        Args:
            order_id: ID of the order
            price: Execution price
            quantity: Execution quantity
            commission: Commission amount
            slippage: Slippage amount
            
        Returns:
            int: Trade ID
            
        Raises:
            Exception: If order execution fails
        """
        conn = self.connect_to_db()
        try:
            with conn:
                cursor = conn.cursor()
                try:
                    # Get order details
                    cursor.execute(
                        "SELECT session_id FROM orders WHERE order_id = %s",
                        (order_id,)
                    )
                    session_id = cursor.fetchone()[0]
                    
                    # Update order status
                    cursor.execute(
                        """
                        UPDATE orders 
                        SET status = %s, filled_quantity = %s, 
                            average_fill_price = %s, execution_time = %s
                        WHERE order_id = %s
                        """,
                        ('filled', quantity, price, datetime.now(), order_id)
                    )
                    
                    # Record trade
                    cursor.execute(
                        """
                        INSERT INTO trades (
                            order_id, timestamp, price, quantity, 
                            commission, slippage
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        RETURNING trade_id
                        """,
                        (order_id, datetime.now(), price, quantity, commission, slippage)
                    )
                    trade_id = cursor.fetchone()[0]
                    
                    return trade_id
                finally:
                    cursor.close()
        except Exception as e:
            self.logger.error(f"Error executing order: {e}")
            raise
        finally:
            conn.close()
    
    def apply_slippage(self, price: float, side: str, 
                      slippage_model: Optional[Dict] = None) -> float:
        """Apply slippage to execution price.
        
        Args:
            price: Original price
            side: Order side (buy/sell)
            slippage_model: Slippage model configuration
            
        Returns:
            float: Price with slippage applied
        """
        if not slippage_model:
            return price
        
        model_type = slippage_model.get('type', 'fixed')
        
        if model_type == 'fixed':
            # Fixed slippage in price units
            slippage_amount = slippage_model.get('amount', 0.0)
            return price + (slippage_amount if side == 'buy' else -slippage_amount)
        
        elif model_type == 'percentage':
            # Percentage slippage
            slippage_percent = slippage_model.get('percentage', 0.0) / 100.0
            return price * (1 + slippage_percent if side == 'buy' else 1 - slippage_percent)
        
        return price
    
    def calculate_commission(self, price: float, quantity: int, 
                           commission_model: Optional[Dict] = None) -> float:
        """Calculate commission for a trade.
        
        Args:
            price: Execution price
            quantity: Trade quantity
            commission_model: Commission model configuration
            
        Returns:
            float: Commission amount
        """
        if not commission_model:
            return 0.0
        
        model_type = commission_model.get('type', 'fixed')
        
        if model_type == 'fixed':
            # Fixed commission per trade
            return commission_model.get('amount', 0.0)
        
        elif model_type == 'per_share':
            # Per-share commission
            return quantity * commission_model.get('amount', 0.0)
        
        elif model_type == 'percentage':
            # Percentage of trade value
            return price * quantity * commission_model.get('percentage', 0.0) / 100.0
        
        return 0.0
    
    def run_backtest(self, strategy_class: Type[Strategy], strategy_id: int, 
                    parameter_set_id: int, instrument_id: int, start_date: str, 
                    end_date: str, timeframe: str = '1d', initial_capital: float = 100000.0, 
                    commission_model: Optional[Dict] = None, slippage_model: Optional[Dict] = None, 
                    parameters: Optional[Dict] = None) -> int:
        """Run a full backtest simulation.
        
        Args:
            strategy_class: Strategy class to use
            strategy_id: ID of the strategy
            parameter_set_id: ID of the parameter set
            instrument_id: ID of the instrument
            start_date: Start date for backtest
            end_date: End date for backtest
            timeframe: Bar timeframe
            initial_capital: Initial capital
            commission_model: Commission model configuration
            slippage_model: Slippage model configuration
            parameters: Strategy parameters
            
        Returns:
            int: Session ID
            
        Raises:
            Exception: If backtest fails
        """
        # Create backtest session
        session_id = self.create_backtest_session(
            strategy_id, parameter_set_id, instrument_id,
            start_date, end_date, timeframe, initial_capital,
            commission_model, slippage_model
        )
        
        try:
            # Load market data
            data = self.load_market_data(instrument_id, start_date, end_date, timeframe)
            if data.empty:
                raise ValueError("No market data available for the specified period")
            
            # Initialize strategy
            strategy = strategy_class()
            strategy.initialize(parameters or {})
            
            # Generate trading signals
            signals = strategy.on_bar(data)
            
            # Process signals and simulate executions
            position = 0
            cash = initial_capital
            equity = initial_capital
            
            for _, signal in signals.iterrows():
                # Record order
                order_id = self.record_order(
                    session_id, signal['timestamp'], instrument_id,
                    'market', signal['side'], signal['quantity'], None
                )
                
                # Apply slippage to execution price
                execution_price = self.apply_slippage(
                    signal['close'], signal['side'], slippage_model
                )
                
                # Calculate commission
                commission = self.calculate_commission(
                    execution_price, signal['quantity'], commission_model
                )
                
                # Execute order
                self.execute_order(
                    order_id, execution_price, signal['quantity'], 
                    commission, abs(execution_price - signal['close'])
                )
                
                # Update position and cash
                old_position = position
                if signal['side'] == 'buy':
                    position += signal['quantity']
                    cash -= (execution_price * signal['quantity'] + commission)
                else:
                    position -= signal['quantity']
                    cash += (execution_price * signal['quantity'] - commission)
                
                # Record portfolio snapshot
                equity = cash + (position * execution_price)
                
                # Calculate P&L
                if old_position != 0 and position != old_position:
                    # Realized P&L calculation would go here
                    pass
            
            # Update backtest session status
            conn = self.connect_to_db()
            try:
                with conn:
                    cursor = conn.cursor()
                    try:
                        cursor.execute(
                            """
                            UPDATE backtest_sessions 
                            SET status = %s, completed_at = %s
                            WHERE session_id = %s
                            """,
                            ('completed', datetime.now(), session_id)
                        )
                    finally:
                        cursor.close()
            finally:
                conn.close()
            
            return session_id
            
        except Exception as e:
            self.logger.error(f"Error running backtest: {e}")
            
            # Update session status to 'failed'
            conn = self.connect_to_db()
            try:
                with conn:
                    cursor = conn.cursor()
                    try:
                        cursor.execute(
                            """
                            UPDATE backtest_sessions 
                            SET status = %s, completed_at = %s
                            WHERE session_id = %s
                            """,
                            ('failed', datetime.now(), session_id)
                        )
                    finally:
                        cursor.close()
            finally:
                conn.close()
            
            raise 