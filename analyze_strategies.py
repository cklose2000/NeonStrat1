import pandas as pd
import numpy as np
from src.db_connection import get_db_connection

def analyze_strategies():
    conn = get_db_connection()
    
    # Query for strategy performance
    query = """
    WITH trade_pairs AS (
        SELECT 
            s.name AS strategy_name,
            bs.session_id,
            o.side,
            t.timestamp,
            t.price,
            t.quantity,
            t.commission,
            t.slippage,
            LAG(t.price) OVER (PARTITION BY bs.session_id ORDER BY t.timestamp) as prev_price,
            LAG(t.quantity) OVER (PARTITION BY bs.session_id ORDER BY t.timestamp) as prev_quantity,
            LAG(o.side) OVER (PARTITION BY bs.session_id ORDER BY t.timestamp) as prev_side
        FROM strategies s
        JOIN backtest_sessions bs ON s.strategy_id = bs.strategy_id
        JOIN orders o ON bs.session_id = o.session_id
        JOIN trades t ON o.order_id = t.order_id
        WHERE t.timestamp BETWEEN '2025-01-30' AND '2025-02-28'
    ),
    trade_metrics AS (
        SELECT 
            strategy_name,
            DATE(timestamp) as trade_date,
            COUNT(*) as num_trades,
            SUM(CASE 
                WHEN prev_side = 'buy' AND side = 'sell' 
                THEN (price - prev_price) * prev_quantity
                WHEN prev_side = 'sell' AND side = 'buy'
                THEN (prev_price - price) * prev_quantity
                ELSE 0 
            END) as gross_pnl,
            SUM(commission + slippage) as costs
        FROM trade_pairs
        WHERE prev_price IS NOT NULL
        GROUP BY strategy_name, DATE(timestamp)
    )
    SELECT 
        strategy_name,
        COUNT(DISTINCT trade_date) as trading_days,
        SUM(num_trades) as total_trades,
        CAST(AVG(num_trades) as NUMERIC(10,2)) as avg_trades_per_day,
        SUM(gross_pnl) as total_gross_pnl,
        SUM(costs) as total_costs,
        SUM(gross_pnl - costs) as net_pnl,
        CAST(AVG(gross_pnl - costs) as NUMERIC(10,2)) as avg_daily_pnl,
        CAST(STDDEV(gross_pnl - costs) as NUMERIC(10,2)) as daily_pnl_std,
        CAST(
            SUM(gross_pnl - costs) / NULLIF(STDDEV(gross_pnl - costs), 0) * SQRT(252) 
            as NUMERIC(10,2)
        ) as annualized_sharpe
    FROM trade_metrics
    GROUP BY strategy_name
    ORDER BY annualized_sharpe DESC NULLS LAST;
    """
    
    df = pd.read_sql_query(query, conn)
    
    print("\nStrategy Performance Analysis")
    print("=" * 100)
    for _, row in df.iterrows():
        print(f"\nStrategy: {row['strategy_name']}")
        print(f"Trading Days: {row['trading_days']}")
        print(f"Total Trades: {row['total_trades']}")
        print(f"Avg Trades/Day: {row['avg_trades_per_day']:.2f}")
        print(f"Total Gross P&L: ${row['total_gross_pnl']:,.2f}")
        print(f"Total Costs: ${row['total_costs']:,.2f}")
        print(f"Net P&L: ${row['net_pnl']:,.2f}")
        print(f"Avg Daily P&L: ${row['avg_daily_pnl']:,.2f}")
        print(f"Daily P&L Std: ${row['daily_pnl_std']:,.2f}")
        print(f"Annualized Sharpe: {row['annualized_sharpe']}")
    
    # Additional analysis for the best performing strategy
    best_strategy = df.iloc[0]['strategy_name']
    
    # Get detailed trade analysis for best strategy
    detail_query = """
    WITH trade_pairs AS (
        SELECT 
            t.timestamp,
            o.side,
            t.price,
            t.quantity,
            t.commission,
            t.slippage,
            LAG(t.price) OVER (ORDER BY t.timestamp) as prev_price,
            LAG(t.quantity) OVER (ORDER BY t.timestamp) as prev_quantity,
            LAG(o.side) OVER (ORDER BY t.timestamp) as prev_side
        FROM strategies s
        JOIN backtest_sessions bs ON s.strategy_id = bs.strategy_id
        JOIN orders o ON bs.session_id = o.session_id
        JOIN trades t ON o.order_id = t.order_id
        WHERE s.name = %s
        AND t.timestamp BETWEEN '2025-01-30' AND '2025-02-28'
        ORDER BY t.timestamp
    )
    SELECT 
        DATE_TRUNC('hour', timestamp) as hour,
        COUNT(*) as trades_in_hour,
        SUM(CASE 
            WHEN prev_side = 'buy' AND side = 'sell' 
            THEN (price - prev_price) * prev_quantity
            WHEN prev_side = 'sell' AND side = 'buy'
            THEN (prev_price - price) * prev_quantity
            ELSE 0 
        END) as gross_pnl,
        SUM(commission + slippage) as costs
    FROM trade_pairs
    WHERE prev_price IS NOT NULL
    GROUP BY DATE_TRUNC('hour', timestamp)
    ORDER BY trades_in_hour DESC
    LIMIT 5;
    """
    
    print(f"\nBest Strategy ({best_strategy}) - Top Trading Hours:")
    print("=" * 100)
    detail_df = pd.read_sql_query(detail_query, conn, params=(best_strategy,))
    for _, row in detail_df.iterrows():
        print(f"\nHour: {row['hour']}")
        print(f"Trades: {row['trades_in_hour']}")
        print(f"Gross P&L: ${row['gross_pnl']:,.2f}")
        print(f"Costs: ${row['costs']:,.2f}")
        print(f"Net P&L: ${row['gross_pnl'] - row['costs']:,.2f}")
    
    conn.close()

if __name__ == "__main__":
    analyze_strategies() 