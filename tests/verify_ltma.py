from src.db_connection import get_db_connection

def verify_ltma_performance():
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Daily performance query
    daily_query = """
    WITH trade_sequence AS (
        SELECT 
            t.timestamp,
            DATE(t.timestamp) as trade_date,
            t.price,
            t.quantity,
            o.side,
            t.commission,
            t.slippage,
            ROW_NUMBER() OVER (PARTITION BY bs.session_id ORDER BY t.timestamp) as trade_seq
        FROM strategies s
        JOIN backtest_sessions bs ON s.strategy_id = bs.strategy_id
        JOIN orders o ON bs.session_id = o.session_id
        JOIN trades t ON o.order_id = t.order_id
        WHERE s.name = 'Long-Term MA Crossover'
        AND t.timestamp BETWEEN '2025-01-30' AND '2025-02-28'
        ORDER BY t.timestamp
    ),
    trade_pairs AS (
        SELECT 
            t1.trade_date,
            t1.timestamp as entry_time,
            t1.price as entry_price,
            t1.side as entry_side,
            t1.quantity,
            t2.timestamp as exit_time,
            t2.price as exit_price,
            t1.commission + t1.slippage + COALESCE(t2.commission, 0) + COALESCE(t2.slippage, 0) as total_costs
        FROM trade_sequence t1
        LEFT JOIN trade_sequence t2 ON t1.trade_seq + 1 = t2.trade_seq
        WHERE t1.trade_seq % 2 = 1  -- Get entry trades
    )
    SELECT 
        trade_date,
        COUNT(*) as num_trades,
        SUM(CASE 
            WHEN entry_side = 'buy' THEN (COALESCE(exit_price, entry_price) - entry_price) * quantity
            ELSE (entry_price - COALESCE(exit_price, entry_price)) * quantity
        END) as gross_pnl,
        SUM(total_costs) as costs,
        string_agg(
            entry_side || ' ' || 
            quantity::text || ' @ $' || 
            ROUND(entry_price::numeric, 2)::text || ' -> $' || 
            ROUND(COALESCE(exit_price, entry_price)::numeric, 2)::text || 
            CASE 
                WHEN exit_price IS NULL THEN ' (OPEN)'
                ELSE ' (P&L: $' || 
                    ROUND(
                        CASE 
                            WHEN entry_side = 'buy' THEN (exit_price - entry_price) * quantity
                            ELSE (entry_price - exit_price) * quantity
                        END::numeric, 
                        2
                    )::text || 
                    ')'
            END,
            E'\n'
        ) as trade_details
    FROM trade_pairs
    GROUP BY trade_date
    ORDER BY trade_date;
    """
    
    print("\nLong-Term MA Crossover - Daily Performance Verification")
    print("=" * 100)
    
    cur.execute(daily_query)
    rows = cur.fetchall()
    
    total_gross_pnl = 0
    total_costs = 0
    total_trades = 0
    
    for row in rows:
        trade_date = row[0]
        num_trades = row[1]
        gross_pnl = row[2] if row[2] is not None else 0
        costs = row[3] if row[3] is not None else 0
        trade_details = row[4]
        
        total_gross_pnl += gross_pnl
        total_costs += costs
        total_trades += num_trades
        
        print(f"\nDate: {trade_date}")
        print(f"Number of Trades: {num_trades}")
        print(f"Gross P&L: ${gross_pnl:,.2f}")
        print(f"Costs: ${costs:,.2f}")
        print(f"Net P&L: ${(gross_pnl - costs):,.2f}")
        print("\nTrade Details:")
        print(trade_details)
    
    print("\nSummary")
    print("=" * 100)
    print(f"Total Trading Days: {len(rows)}")
    print(f"Total Trades: {total_trades}")
    print(f"Total Gross P&L: ${total_gross_pnl:,.2f}")
    print(f"Total Costs: ${total_costs:,.2f}")
    print(f"Total Net P&L: ${(total_gross_pnl - total_costs):,.2f}")
    print(f"Average P&L per Trade: ${((total_gross_pnl - total_costs) / total_trades if total_trades > 0 else 0):,.2f}")
    
    # Verify open positions
    open_positions_query = """
    WITH trade_sequence AS (
        SELECT 
            t.timestamp,
            t.price,
            t.quantity,
            o.side,
            ROW_NUMBER() OVER (PARTITION BY bs.session_id ORDER BY t.timestamp) as trade_seq
        FROM strategies s
        JOIN backtest_sessions bs ON s.strategy_id = bs.strategy_id
        JOIN orders o ON bs.session_id = o.session_id
        JOIN trades t ON o.order_id = t.order_id
        WHERE s.name = 'Long-Term MA Crossover'
        AND t.timestamp BETWEEN '2025-01-30' AND '2025-02-28'
        ORDER BY t.timestamp
    )
    SELECT 
        SUM(CASE WHEN trade_seq % 2 = 1 THEN
            CASE WHEN side = 'buy' THEN quantity ELSE -quantity END
        ELSE 0 END) as net_position
    FROM trade_sequence;
    """
    
    cur.execute(open_positions_query)
    open_position = cur.fetchone()[0]
    print(f"\nEnd of Period Position: {open_position if open_position else 0}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    verify_ltma_performance() 