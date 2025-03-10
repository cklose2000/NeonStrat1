import logging
from datetime import datetime, timedelta
from src.backtest import Backtest
from src.strategy import InvertedLongTermMACrossover
from src.db_connection import get_db_connection
import json

def run_inverted_ltma_backtest():
    """Run a backtest for the Inverted Long-Term MA Crossover strategy."""
    try:
        # Connect to the database
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Insert the strategy into the database if it doesn't exist
        cursor.execute("""
            INSERT INTO strategies (name, description, version, status)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (name, version) DO UPDATE 
            SET description = EXCLUDED.description, status = EXCLUDED.status
            RETURNING strategy_id
        """, (
            'Inverted Long-Term MA Crossover', 
            'Improved strategy with reduced frequency and optimized position sizing', 
            '1.1.0', 
            'active'
        ))
        strategy_id = cursor.fetchone()[0]
        conn.commit()
        
        # Insert the parameter set into the database
        parameters = {
            'short_window': 20,
            'long_window': 50,
            'base_position_size': 100,
            'min_crossover_threshold': 0.01,
            'atr_period': 14,
            'atr_threshold': 0.01,
            'signal_strength_factor': 2.0,
            'max_position_size': 200
        }
        
        cursor.execute("""
            INSERT INTO parameter_sets (strategy_id, name, parameters)
            VALUES (%s, %s, %s)
            ON CONFLICT (strategy_id, name) DO UPDATE 
            SET parameters = EXCLUDED.parameters
            RETURNING set_id
        """, (
            strategy_id, 
            'improved_reduced_frequency', 
            json.dumps(parameters)
        ))
        parameter_set_id = cursor.fetchone()[0]
        conn.commit()
        
        # Get the instrument ID for SPY
        cursor.execute("SELECT instrument_id FROM instruments WHERE symbol = 'SPY'")
        instrument_id = cursor.fetchone()[0]
        
        # Create a new backtest session
        start_date = '2025-01-30'
        end_date = '2025-02-28'
        
        # Initialize the backtest
        backtest = Backtest()
        
        # Run the backtest
        session_id = backtest.run_backtest(
            strategy_class=InvertedLongTermMACrossover,
            strategy_id=strategy_id,
            parameter_set_id=parameter_set_id,
            instrument_id=instrument_id,
            start_date=start_date,
            end_date=end_date,
            timeframe='5m',
            initial_capital=100000.0,
            commission_model={'type': 'percentage', 'percentage': 0.0006},
            slippage_model={'type': 'percentage', 'percentage': 0.0001},
            parameters=parameters
        )
        
        # Close the database connection
        conn.close()
        
        return session_id
    
    except Exception as e:
        print(f"Error in backtest: {e}")
        if 'conn' in locals() and conn:
            conn.close()
        raise

def verify_day_trading_compliance(conn, session_id):
    """
    Verify that all trades comply with day trading rules:
    1. No positions held overnight
    2. No trades after 3:55 PM EST
    """
    cur = conn.cursor()
    
    query = """
    WITH trade_sequence AS (
        -- Convert timestamps to EST (UTC-5)
        SELECT 
            timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS est_timestamp,
            CASE WHEN side = 'buy' THEN 1 ELSE -1 END as direction,
            quantity as size
        FROM orders
        WHERE session_id = %s
        ORDER BY timestamp
    ),
    daily_positions AS (
        -- Calculate running position for each trade
        SELECT 
            est_timestamp,
            direction,
            size,
            SUM(direction * size) OVER (
                ORDER BY est_timestamp
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS position
        FROM trade_sequence
    ),
    eod_positions AS (
        -- Get last trade of each day and check position
        SELECT 
            DATE(est_timestamp) AS trade_date,
            MAX(est_timestamp::time) AS last_trade_time,
            position
        FROM daily_positions
        WHERE est_timestamp = (
            SELECT MAX(est_timestamp)
            FROM daily_positions dp2
            WHERE DATE(dp2.est_timestamp) = DATE(daily_positions.est_timestamp)
        )
        GROUP BY DATE(est_timestamp), position
    )
    SELECT 
        trade_date,
        last_trade_time,
        position as end_position
    FROM eod_positions
    WHERE position != 0  -- Non-zero position at end of day
    OR last_trade_time::text > '15:55:00'  -- Trade after 3:55 PM EST
    ORDER BY trade_date;
    """
    
    cur.execute(query, (session_id,))
    violations = cur.fetchall()
    
    if violations:
        print("\nDay Trading Rule Violations:")
        print("Date         Last Trade  End Position  Violation")
        print("-" * 60)
        
        for row in violations:
            trade_date, last_trade_time, end_position = row
            
            violations = []
            if end_position != 0:
                violations.append("Non-zero position at EOD")
            if last_trade_time.strftime('%H:%M:%S') > '15:55:00':
                violations.append(f"Last trade at {last_trade_time.strftime('%H:%M:%S')} EST")
                
            print(f"{trade_date.strftime('%Y-%m-%d')}  "
                  f"{last_trade_time.strftime('%H:%M:%S')}    "
                  f"{end_position:11d}  {', '.join(violations)}")
    else:
        print("\nNo day trading rule violations found.")
    
    cur.close()

def verify_performance(conn, session_id):
    """
    Verify the performance of a backtest session.
    
    Args:
        conn: Database connection
        session_id: ID of the backtest session to verify
    """
    cur = conn.cursor()
    
    try:
        # Calculate daily performance metrics
        query = """
        WITH trade_metrics AS (
            SELECT 
                DATE(o.timestamp) AS trade_date,
                COUNT(*) AS num_trades,
                SUM(CASE 
                    WHEN o.side = 'sell' THEN o.quantity * o.price
                    ELSE -o.quantity * o.price
                END) AS gross_pnl,
                SUM(t.commission + t.slippage) AS costs
            FROM orders o
            JOIN trades t ON o.order_id = t.order_id
            WHERE o.session_id = %s
            GROUP BY DATE(o.timestamp)
        )
        SELECT
            trade_date,
            num_trades,
            gross_pnl,
            costs,
            gross_pnl - costs AS net_pnl
        FROM trade_metrics
        ORDER BY trade_date;
        """
        
        cur.execute(query, (session_id,))
        daily_results = cur.fetchall()
        
        if not daily_results:
            print("No trades found for this session.")
            return
            
        print("\nDaily Performance Summary:")
        print("Date         Trades  Gross P&L    Costs    Net P&L")
        print("-" * 50)
        
        total_trades = 0
        total_gross_pnl = 0
        total_costs = 0
        total_net_pnl = 0
        
        for row in daily_results:
            trade_date, num_trades, gross_pnl, costs, net_pnl = row
            total_trades += num_trades
            total_gross_pnl += gross_pnl if gross_pnl else 0
            total_costs += costs if costs else 0
            total_net_pnl += net_pnl if net_pnl else 0
            
            print(f"{trade_date.strftime('%Y-%m-%d')}  {num_trades:6d}  "
                  f"{gross_pnl:10.2f}  {costs:7.2f}  {net_pnl:8.2f}")
                  
        print("-" * 50)
        print(f"Total      {total_trades:6d}  "
              f"{total_gross_pnl:10.2f}  {total_costs:7.2f}  {total_net_pnl:8.2f}")
              
        if total_trades > 0:
            avg_pnl_per_trade = total_net_pnl / total_trades
            print(f"\nAverage P&L per trade: ${avg_pnl_per_trade:.2f}")
            
        # Get final equity
        query = """
        SELECT final_equity
        FROM backtest_sessions
        WHERE session_id = %s
        """
        
        cur.execute(query, (session_id,))
        result = cur.fetchone()
        end_equity = result[0] if result else None
        
        print(f"\nFinal equity: ${end_equity:.2f}")
        
        # Verify day trading compliance
        verify_day_trading_compliance(conn, session_id)
        
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    run_inverted_ltma_backtest() 