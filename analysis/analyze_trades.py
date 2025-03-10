from src.db_connection import get_db_connection

def analyze_trades():
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get trading activity by strategy
    query = """
    SELECT 
        s.name,
        COUNT(DISTINCT DATE(t.timestamp)) as trading_days,
        COUNT(*) as total_trades,
        SUM(CASE WHEN o.side = 'buy' THEN -1 ELSE 1 END * t.quantity * t.price) as gross_pnl,
        SUM(t.commission + t.slippage) as total_costs
    FROM strategies s
    JOIN backtest_sessions bs ON s.strategy_id = bs.strategy_id
    JOIN orders o ON bs.session_id = o.session_id
    JOIN trades t ON o.order_id = t.order_id
    WHERE t.timestamp BETWEEN '2025-01-30' AND '2025-02-28'
    GROUP BY s.name
    ORDER BY gross_pnl DESC;
    """
    
    cur.execute(query)
    rows = cur.fetchall()
    
    print("\nStrategy Trading Activity and Performance")
    print("=" * 80)
    
    for row in rows:
        print(f"\nStrategy: {row[0]}")
        print(f"Trading Days: {row[1]}")
        print(f"Total Trades: {row[2]}")
        print(f"Gross P&L: ${row[3]:,.2f}")
        print(f"Total Costs: ${row[4]:,.2f}")
        print(f"Net P&L: ${(row[3] - row[4]):,.2f}")
        print(f"Average P&L per Trade: ${((row[3] - row[4]) / row[2]):,.2f}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    analyze_trades() 