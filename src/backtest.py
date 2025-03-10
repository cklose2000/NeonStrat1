import logging
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Type, Optional, List, Dict, Any
import psycopg2
from src.strategy import Strategy
from src.models import Signal
import json
import os
from src.strategy import (
    ShortTermMACrossover, MediumTermMACrossover, LongTermMACrossover,
    CustomRSIStrategy, MACDCrossoverStrategy, BollingerBandsBreakoutStrategy,
    ATRBreakoutStrategy, VWAPCrossoverStrategy, OBVTrendFollowingStrategy,
    CandlestickPatternStrategy, ATRBreakoutWithVolumeConfirmation,
    DualTimeframeMACrossover, MeanReversionWithStatisticalBoundaries,
    MomentumDivergenceStrategy, SupportResistanceBreakoutWithOrderFlow
)

logger = logging.getLogger(__name__)

class Backtest:
    """
    Backtesting engine for trading strategies.
    """
    
    def __init__(self):
        """Initialize backtesting engine."""
        self.logger = logging.getLogger(__name__)
        self.db_url = os.getenv('DATABASE_URL')
        if not self.db_url:
            raise ValueError("DATABASE_URL environment variable is not set")
    
    def connect_to_db(self) -> psycopg2.extensions.connection:
        """Connect to PostgreSQL database.
        
        Returns:
            psycopg2.extensions.connection: Database connection object
            
        Raises:
            Exception: If connection fails
        """
        try:
            conn = psycopg2.connect(self.db_url)
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
    
    def record_order(self, session_id: int, timestamp: datetime, direction: int,
                    size: int, price: float, commission: float, reason: str, instrument_id: int) -> int:
        """
        Record an executed order in the database.
        
        Args:
            session_id: ID of the backtest session
            timestamp: Timestamp of the order
            direction: Order direction (1 for buy, -1 for sell)
            size: Order size
            price: Execution price
            commission: Commission paid
            reason: Reason for the order
            instrument_id: ID of the instrument being traded
            
        Returns:
            ID of the created order record
        """
        conn = self.connect_to_db()
        cursor = conn.cursor()
        
        try:
            query = """
            INSERT INTO orders (
                session_id,
                timestamp,
                instrument_id,
                side,
                quantity,
                price,
                order_type,
                status,
                time_in_force,
                submit_time,
                execution_time,
                filled_quantity,
                average_fill_price,
                reason
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING order_id
            """
            
            # Convert direction to side
            side = 'buy' if direction == 1 else 'sell'
            
            cursor.execute(
                query,
                (
                    session_id,
                    timestamp,
                    instrument_id,
                    side,
                    size,
                    price,
                    'market',  # order_type
                    'filled',  # status
                    'day',     # time_in_force
                    timestamp, # submit_time
                    timestamp, # execution_time
                    size,     # filled_quantity
                    price,    # average_fill_price
                    reason    # reason
                )
            )
            
            order_id = cursor.fetchone()[0]
            conn.commit()
            
            return order_id
            
        finally:
            cursor.close()
            conn.close()
    
    def execute_order(self, order_id: int, timestamp: datetime, price: float, quantity: int, 
                     commission: float = 0.0, slippage: float = 0.0) -> int:
        """Execute a simulated order and record the trade.
        
        Args:
            order_id: ID of the order
            timestamp: Execution timestamp from market data
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
                        ('filled', quantity, price, timestamp, order_id)  # Use market data timestamp
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
                        (order_id, timestamp, price, quantity, commission, slippage)  # Use market data timestamp
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
    
    def calculate_commission(self, quantity: int, price: float, commission_model: Optional[Dict] = None) -> float:
        """
        Calculate commission for a trade.
        
        Args:
            quantity: Number of shares
            price: Price per share
            commission_model: Dictionary containing commission model parameters
            
        Returns:
            Commission amount
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
            return price * quantity * commission_model.get('percentage', 0.0)
        
        return 0.0
    
    def calculate_slippage(self, quantity: int, price: float, slippage_model: Optional[Dict] = None) -> float:
        """
        Calculate slippage for a trade.
        
        Args:
            quantity: Number of shares
            price: Price per share
            slippage_model: Dictionary containing slippage model parameters
            
        Returns:
            Slippage amount as a percentage
        """
        if not slippage_model:
            return 0.0
        
        model_type = slippage_model.get('type', 'fixed')
        
        if model_type == 'fixed':
            # Fixed slippage per share
            return slippage_model.get('amount', 0.0)
        
        elif model_type == 'percentage':
            # Percentage of price
            return slippage_model.get('percentage', 0.0) / 100.0
        
        return 0.0
    
    def run_backtest(self, strategy_class: Type[Strategy], strategy_id: int, 
                    parameter_set_id: int, instrument_id: int, start_date: str, 
                    end_date: str, timeframe: str = '1d', initial_capital: float = 100000.0, 
                    commission_model: Optional[Dict] = None, slippage_model: Optional[Dict] = None, 
                    parameters: Optional[Dict] = None) -> int:
        """Run a full backtest simulation."""
        try:
            # Connect to database
            conn = self.connect_to_db()
            cursor = conn.cursor()
            
            # Create a new backtest session
            session_id = self.create_backtest_session(
                strategy_id,
                parameter_set_id,
                instrument_id,
                start_date,
                end_date,
                timeframe,
                initial_capital,
                commission_model,
                slippage_model
            )
            
            # Initialize strategy
            strategy = strategy_class()
            strategy.initialize(parameters or {})
            print(f"Strategy initialized with parameters: {parameters}")
            
            # Load market data with market hours filter
            query = """
            WITH market_hours AS (
                SELECT 
                    timestamp,
                    timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' as est_time,
                    open, high, low, close, volume,
                    EXTRACT(HOUR FROM timestamp) as hour_utc,
                    EXTRACT(MINUTE FROM timestamp) as minute_utc,
                    EXTRACT(DOW FROM timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') as day_of_week
                FROM bars_5m 
                WHERE instrument_id = %s
                AND timestamp >= %s
                AND timestamp <= %s
            ),
            filtered_hours AS (
                SELECT *
                FROM market_hours
                WHERE (hour_utc > 4 OR (hour_utc = 4 AND minute_utc >= 30)) -- After 9:30 AM EST (4:30 AM UTC)
                AND (hour_utc < 11 OR (hour_utc = 11 AND minute_utc = 0))   -- Before or at 4:00 PM EST (11:00 AM UTC)
                AND day_of_week BETWEEN 1 AND 5  -- Monday to Friday
                ORDER BY timestamp
            ),
            daily_last_bars AS (
                SELECT *,
                    CASE WHEN LEAD(DATE(est_time), 1) OVER (ORDER BY timestamp) != DATE(est_time)
                         OR LEAD(timestamp, 1) OVER (ORDER BY timestamp) IS NULL
                         OR (hour_utc = 10 AND minute_utc >= 55) -- 3:55 PM EST (10:55 AM UTC) or later
                    THEN true ELSE false END as is_last_bar
                FROM filtered_hours
            )
            SELECT timestamp, open, high, low, close, volume, is_last_bar
            FROM daily_last_bars
            ORDER BY timestamp
            """
            
            market_data = pd.read_sql_query(
                query,
                conn,
                params=(instrument_id, start_date, end_date),
                parse_dates=['timestamp']
            )
            
            if market_data.empty:
                raise ValueError("No market data found for the specified period")
                
            self.logger.debug(f"Loaded {len(market_data)} bars of market data")
            print(f"Loaded {len(market_data)} bars of market data\n")
            
            # Print first few bars for debugging
            print("\nFirst 5 bars:")
            print(market_data.head())
            
            # Initialize tracking variables
            position = 0
            cash = initial_capital
            equity = initial_capital
            average_price = None
            realized_pnl = 0.0
            
            # Process each bar
            for i in range(len(market_data)):
                bar_data = market_data.iloc[i:i+1].copy()  # Create a copy to avoid SettingWithCopyWarning
                
                # Debug log
                if i % 100 == 0:
                    print(f"Processing bar {i}/{len(market_data)}: {bar_data.iloc[0]['timestamp']}")
                
                signals = strategy.on_bar(bar_data)
                
                if not signals.empty:  # If we have any signals
                    print(f"Generated {len(signals)} trading signals at {bar_data.iloc[0]['timestamp']}")
                    self.logger.debug(f"Generated {len(signals)} trading signals")
                    
                    # Process each signal
                    for signal in signals:
                        # Calculate commission
                        commission = self.calculate_commission(
                            signal.size,
                            signal.price,
                            commission_model
                        )
                        
                        # Calculate slippage
                        slippage = self.calculate_slippage(
                            signal.size,
                            signal.price,
                            slippage_model
                        )
                        
                        # Adjust price for slippage
                        execution_price = signal.price * (1 + signal.direction * slippage)
                        
                        # Record the order
                        order_id = self.record_order(
                            session_id,
                            signal.timestamp,
                            signal.direction,
                            signal.size,
                            execution_price,
                            commission,
                            signal.reason,
                            instrument_id
                        )
                        
                        # Execute the order and record the trade
                        trade_id = self.execute_order(
                            order_id,
                            signal.timestamp,
                            execution_price,
                            signal.size,
                            commission,
                            slippage
                        )
                        
                        # Update position and cash
                        old_position = position
                        position += signal.direction * signal.size
                        cash -= signal.direction * signal.size * execution_price
                        cash -= commission
                        
                        # Update average price and P&L
                        if position != 0:
                            if old_position == 0:
                                # New position
                                average_price = execution_price
                            elif position * old_position > 0:
                                # Adding to position
                                total_value = (old_position * average_price) + (signal.size * execution_price)
                                average_price = total_value / position
                            else:
                                # Reducing or closing position
                                realized_pnl += signal.direction * signal.size * (execution_price - average_price)
                                if position == 0:
                                    average_price = None
                        
                        # Calculate equity and unrealized P&L
                        market_value = position * execution_price
                        equity = cash + market_value
                        unrealized_pnl = position * (execution_price - (average_price or execution_price))
                        
                        # Update session results
                        self.update_session_results(session_id, equity)
                
                # Force close positions at the end of each trading day
                if bar_data.iloc[0]['is_last_bar'] and position != 0:
                    close_price = bar_data.iloc[0]['close']
                    size = -position  # This will close the position
                    
                    print(f"Closing position at end of day: {position} shares at {close_price}")
                    
                    # Calculate final commission and slippage
                    commission = self.calculate_commission(
                        abs(size),
                        close_price,
                        commission_model
                    )
                    
                    slippage = self.calculate_slippage(
                        abs(size),
                        close_price,
                        slippage_model
                    )
                    
                    # Adjust price for slippage
                    execution_price = close_price * (1 + (1 if size > 0 else -1) * slippage)
                    
                    # Record the final order
                    order_id = self.record_order(
                        session_id,
                        bar_data.iloc[0]['timestamp'],
                        1 if size > 0 else -1,
                        abs(size),
                        execution_price,
                        commission,
                        "DAY_END_CLOSE",
                        instrument_id
                    )
                    
                    # Execute the final order
                    trade_id = self.execute_order(
                        order_id,
                        bar_data.iloc[0]['timestamp'],
                        execution_price,
                        abs(size),
                        commission,
                        slippage
                    )
                    
                    # Update final cash and equity
                    cash -= size * execution_price
                    cash -= commission
                    equity = cash
                    position = 0
                    average_price = None
                    
                    # Update session results
                    self.update_session_results(session_id, equity)
            
            conn.commit()
            return session_id
            
        except Exception as e:
            self.logger.error(f"Error during backtest: {str(e)}")
            if 'conn' in locals():
                conn.rollback()
            raise
            
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()
    
    def update_session_results(self, session_id: int, end_equity: float) -> None:
        """
        Update backtest session with final results.
        
        Args:
            session_id: ID of the backtest session
            end_equity: Final equity value
        """
        conn = self.connect_to_db()
        cursor = conn.cursor()
        
        try:
            query = """
            UPDATE backtest_sessions
            SET final_equity = %s,
                status = 'completed',
                completed_at = NOW()
            WHERE session_id = %s
            """
            
            cursor.execute(query, (end_equity, session_id))
            conn.commit()
            
        finally:
            cursor.close()
            conn.close()
    
    def record_position(self, session_id: int, timestamp: datetime, quantity: int,
                       cash: float, equity: float, instrument_id: int, current_price: float,
                       average_price: float = None, unrealized_pnl: float = 0.0,
                       realized_pnl: float = 0.0) -> None:
        """
        Record a position update in the database.
        
        Args:
            session_id: ID of the backtest session
            timestamp: Timestamp of the position update
            quantity: Current position size
            cash: Current cash balance
            equity: Current total equity
            instrument_id: ID of the instrument
            current_price: Current market price
            average_price: Average entry price (optional)
            unrealized_pnl: Unrealized P&L (optional)
            realized_pnl: Realized P&L (optional)
        """
        conn = self.connect_to_db()
        cursor = conn.cursor()
        
        try:
            query = """
            INSERT INTO positions (
                session_id,
                timestamp,
                instrument_id,
                quantity,
                average_price,
                current_price,
                unrealized_pnl,
                realized_pnl
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(
                query,
                (
                    session_id,
                    timestamp,
                    instrument_id,
                    quantity,
                    average_price or current_price,
                    current_price,
                    unrealized_pnl,
                    realized_pnl
                )
            )
            
            conn.commit()
            
        finally:
            cursor.close()
            conn.close()

    def insert_strategies(self) -> Dict[str, int]:
        """Insert strategy definitions into the strategies table.
        
        Returns:
            Dict[str, int]: Dictionary mapping strategy names to their IDs
        """
        strategies = [
            ("Short-Term MA Crossover", "Moving average crossover strategy with short-term windows", "1.0.0"),
            ("Medium-Term MA Crossover", "Moving average crossover strategy with medium-term windows", "1.0.0"),
            ("Long-Term MA Crossover", "Moving average crossover strategy with long-term windows", "1.0.0"),
            ("Custom RSI", "RSI strategy with custom overbought and oversold levels", "1.0.0"),
            ("MACD Crossover", "MACD crossover strategy with customizable parameters", "1.0.0"),
            ("Bollinger Bands Breakout", "Bollinger Bands breakout strategy", "1.0.0"),
            ("ATR Breakout", "ATR-based breakout strategy", "1.0.0"),
            ("VWAP Crossover", "Volume-weighted average price crossover strategy", "1.0.0"),
            ("OBV Trend Following", "On-balance volume trend following strategy", "1.0.0"),
            ("Candlestick Pattern", "Candlestick pattern recognition strategy", "1.0.0")
        ]
        
        strategy_ids = {}
        conn = self.connect_to_db()
        try:
            with conn:
                cursor = conn.cursor()
                for name, description, version in strategies:
                    try:
                        cursor.execute(
                            """
                            INSERT INTO strategies (name, description, version, status)
                            VALUES (%s, %s, %s, 'active')
                            ON CONFLICT (name, version) DO UPDATE 
                            SET description = EXCLUDED.description,
                                status = 'active'
                            RETURNING strategy_id
                            """,
                            (name, description, version)
                        )
                        strategy_id = cursor.fetchone()[0]
                        strategy_ids[name] = strategy_id
                        print(f"Strategy '{name}' inserted/updated with ID: {strategy_id}")
                    except Exception as e:
                        print(f"Error inserting strategy {name}: {e}")
                conn.commit()
        finally:
            conn.close()
        
        return strategy_ids

    def insert_parameter_sets(self, strategy_ids: Dict[str, int]) -> Dict[str, int]:
        """Insert parameter sets for each strategy.
        
        Args:
            strategy_ids: Dictionary mapping strategy names to their IDs
            
        Returns:
            Dict[str, int]: Dictionary mapping strategy names to their parameter set IDs
        """
        parameter_sets = {
            "Short-Term MA Crossover": {'short_window': 5, 'long_window': 20},
            "Medium-Term MA Crossover": {'short_window': 10, 'long_window': 50},
            "Long-Term MA Crossover": {'short_window': 20, 'long_window': 100},
            "Custom RSI": {'rsi_period': 14, 'overbought_level': 75, 'oversold_level': 25},
            "MACD Crossover": {'fast_period': 12, 'slow_period': 26, 'signal_period': 9},
            "Bollinger Bands Breakout": {'window': 20, 'num_std': 2},
            "ATR Breakout": {'atr_period': 14, 'multiplier': 2},
            "VWAP Crossover": {'short_window': 5, 'long_window': 20},
            "OBV Trend Following": {'obv_period': 20},
            "Candlestick Pattern": {'pattern': 'engulfing'}
        }
        
        param_set_ids = {}
        conn = self.connect_to_db()
        try:
            with conn:
                cursor = conn.cursor()
                for strategy_name, params in parameter_sets.items():
                    try:
                        # First, check if a parameter set already exists
                        cursor.execute(
                            """
                            SELECT set_id 
                            FROM parameter_sets 
                            WHERE strategy_id = %s 
                            AND name = %s
                            """,
                            (strategy_ids[strategy_name], f"Default_{strategy_name}")
                        )
                        result = cursor.fetchone()
                        
                        if result:
                            # Update existing parameter set
                            set_id = result[0]
                            cursor.execute(
                                """
                                UPDATE parameter_sets 
                                SET parameters = %s
                                WHERE set_id = %s
                                """,
                                (json.dumps(params), set_id)
                            )
                        else:
                            # Insert new parameter set
                            cursor.execute(
                                """
                                INSERT INTO parameter_sets (strategy_id, name, parameters)
                                VALUES (%s, %s, %s)
                                RETURNING set_id
                                """,
                                (strategy_ids[strategy_name], f"Default_{strategy_name}", json.dumps(params))
                            )
                            set_id = cursor.fetchone()[0]
                        
                        param_set_ids[strategy_name] = set_id
                        print(f"Parameter set for '{strategy_name}' inserted/updated with ID: {set_id}")
                    except Exception as e:
                        print(f"Error inserting parameter set for {strategy_name}: {e}")
                conn.commit()
        finally:
            conn.close()
        
        return param_set_ids

    def register_strategy(self, strategy_name: str, description: str, version: str, author: str) -> int:
        """Register a new strategy in the database, or return the existing strategy ID if it already exists."""
        conn = self.connect_to_db()
        try:
            with conn.cursor() as cur:
                # Check if the strategy already exists
                cur.execute(
                    """
                    SELECT strategy_id FROM strategies WHERE name = %s AND version = %s
                    """,
                    (strategy_name, version)
                )
                result = cur.fetchone()
                if result:
                    return result[0]

                # Insert new strategy if it does not exist
                cur.execute(
                    """
                    INSERT INTO strategies (name, description, version, author)
                    VALUES (%s, %s, %s, %s)
                    RETURNING strategy_id;
                    """,
                    (strategy_name, description, version, author)
                )
                strategy_id = cur.fetchone()[0]
                conn.commit()
                return strategy_id
        except Exception as e:
            self.logger.error(f"Error registering strategy: {e}")
            raise
        finally:
            conn.close()

    def setup_backtest_sessions(self):
        """Set up backtest sessions for the new strategies."""
        strategies = [
            ('ATR Breakout with Volume Confirmation', 'ATR breakout strategy with volume confirmation', '1.0', 'Your Name'),
            ('Dual Timeframe Moving Average', 'Dual timeframe moving average strategy', '1.0', 'Your Name'),
            ('Mean Reversion with Statistical Boundaries', 'Mean reversion strategy using statistical boundaries', '1.0', 'Your Name'),
            ('Momentum Divergence', 'Momentum divergence strategy', '1.0', 'Your Name'),
            ('Support/Resistance Breakout with Order Flow', 'Support/resistance breakout strategy with order flow confirmation', '1.0', 'Your Name')
        ]

        for name, description, version, author in strategies:
            strategy_id = self.register_strategy(name, description, version, author)
            # Assuming parameter_set_id and instrument_id are predefined or retrieved from the database
            parameter_set_id = 1  # Placeholder
            instrument_id = 1  # Placeholder
            self.create_backtest_session(strategy_id, parameter_set_id, instrument_id, '2023-01-01', '2023-12-31', '1d', 100000.0)

    def run_backtests(self):
        """Run backtests for all registered strategies."""
        # Register strategies to ensure they are in the database
        self.setup_backtest_sessions()

        # Load market data and execute backtests for each strategy
        strategies = [
            (ATRBreakoutWithVolumeConfirmation, 'ATR Breakout with Volume Confirmation'),
            (DualTimeframeMACrossover, 'Dual Timeframe Moving Average'),
            (MeanReversionWithStatisticalBoundaries, 'Mean Reversion with Statistical Boundaries'),
            (MomentumDivergenceStrategy, 'Momentum Divergence'),
            (SupportResistanceBreakoutWithOrderFlow, 'Support/Resistance Breakout with Order Flow')
        ]

        # Assuming parameter_set_id and instrument_id are predefined or retrieved from the database
        parameter_set_id = 1  # Placeholder
        instrument_id = 1  # Placeholder
        start_date = '2025-01-30'
        end_date = '2025-02-28'
        timeframe = '5m'
        initial_capital = 100000.0

        # Define commission and slippage models
        commission_model = {
            'type': 'percentage',
            'percentage': 0.001  # 0.1% commission
        }
        
        slippage_model = {
            'type': 'percentage',
            'percentage': 0.0001  # 0.01% slippage
        }

        for strategy_class, strategy_name in strategies:
            print(f"\nBacktesting {strategy_name} strategy...")
            try:
                # Retrieve strategy ID from the database
                strategy_id = self.get_strategy_id(strategy_name)

                # Run backtest
                session_id = self.run_backtest(
                    strategy_class=strategy_class,
                    strategy_id=strategy_id,
                    parameter_set_id=parameter_set_id,
                    instrument_id=instrument_id,
                    start_date=start_date,
                    end_date=end_date,
                    timeframe=timeframe,
                    initial_capital=initial_capital,
                    commission_model=commission_model,
                    slippage_model=slippage_model
                )
                print(f"Completed backtest for {strategy_name}. Session ID: {session_id}")
            except Exception as e:
                print(f"Error running backtest for {strategy_name}: {e}")

    def get_strategy_id(self, strategy_name: str) -> int:
        """Retrieve the strategy ID from the database by name."""
        conn = self.connect_to_db()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT strategy_id FROM strategies WHERE name = %s
                    """,
                    (strategy_name,)
                )
                result = cur.fetchone()
                if result:
                    return result[0]
                else:
                    raise ValueError(f"Strategy {strategy_name} not found in the database.")
        except Exception as e:
            self.logger.error(f"Error retrieving strategy ID: {e}")
            raise
        finally:
            conn.close()

# Example usage
if __name__ == "__main__":
    simulator = Backtest()
    simulator.run_backtests() 