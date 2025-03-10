from src.db_connection import get_db_connection

def check_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Check backtest_sessions table
    print("Checking backtest_sessions table...")
    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'backtest_sessions'")
    columns = cursor.fetchall()
    print("Columns in backtest_sessions table:")
    for col in columns:
        print(f"  - {col[0]}")
    
    # Get the latest backtest session ID
    cursor.execute("SELECT MAX(session_id) FROM backtest_sessions")
    latest_id = cursor.fetchone()[0]
    print(f"\nLatest backtest session ID: {latest_id}")
    
    # Get details of the latest session
    cursor.execute("SELECT * FROM backtest_sessions WHERE session_id = %s", (latest_id,))
    session = cursor.fetchone()
    print("\nLatest backtest session details:")
    for i, col in enumerate(cursor.description):
        print(f"  - {col.name}: {session[i]}")
    
    # Get strategy details
    cursor.execute("SELECT * FROM strategies WHERE strategy_id = %s", (session[1],))
    strategy = cursor.fetchone()
    print("\nStrategy details:")
    for i, col in enumerate(cursor.description):
        print(f"  - {col.name}: {strategy[i]}")
    
    # Check parameter_sets table schema
    print("\nChecking parameter_sets table schema...")
    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'parameter_sets'")
    columns = cursor.fetchall()
    if columns:
        print("Columns in parameter_sets table:")
        for col in columns:
            print(f"  - {col[0]}")
        
        # Get parameter set details
        cursor.execute("SELECT * FROM parameter_sets WHERE set_id = %s", (session[2],))
        params = cursor.fetchone()
        if params:
            print("\nParameter set details:")
            for i, col in enumerate(cursor.description):
                print(f"  - {col.name}: {params[i]}")
        else:
            print(f"\nNo parameter set found with ID {session[2]}")
    else:
        print("parameter_sets table not found or has no columns")
    
    # Check orders table
    print("\nChecking orders table...")
    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'orders'")
    columns = cursor.fetchall()
    print("Columns in orders table:")
    for col in columns:
        print(f"  - {col[0]}")
    
    # Check foreign key relationships for orders
    print("\nChecking foreign key relationships for orders...")
    cursor.execute("""
        SELECT
            tc.constraint_name,
            tc.table_name,
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM
            information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_name = 'orders';
    """)
    fk_relationships = cursor.fetchall()
    if fk_relationships:
        print("Foreign key relationships for orders table:")
        for rel in fk_relationships:
            print(f"  - {rel[0]}: {rel[1]}.{rel[2]} references {rel[3]}.{rel[4]}")
    else:
        print("No foreign key relationships found for orders table.")
    
    # Count orders for the latest session
    cursor.execute("SELECT COUNT(*) FROM orders WHERE session_id = %s", (latest_id,))
    count = cursor.fetchone()[0]
    print(f"\nNumber of orders for latest session (ID {latest_id}): {count}")
    
    # If there are no orders for the latest session, check the previous session
    if count == 0:
        print("\nNo orders found for the latest session.")
        
        # Get the previous session ID
        cursor.execute("SELECT MAX(session_id) FROM backtest_sessions WHERE session_id < %s", (latest_id,))
        prev_id = cursor.fetchone()[0]
        
        if prev_id:
            print(f"\nChecking previous session (ID {prev_id})...")
            
            # Get details of the previous session
            cursor.execute("SELECT * FROM backtest_sessions WHERE session_id = %s", (prev_id,))
            prev_session = cursor.fetchone()
            print("\nPrevious backtest session details:")
            for i, col in enumerate(cursor.description):
                print(f"  - {col.name}: {prev_session[i]}")
            
            # Count orders for the previous session
            cursor.execute("SELECT COUNT(*) FROM orders WHERE session_id = %s", (prev_id,))
            prev_count = cursor.fetchone()[0]
            print(f"\nNumber of orders for previous session (ID {prev_id}): {prev_count}")
            
            if prev_count > 0:
                # Show sample orders from the previous session
                cursor.execute("SELECT * FROM orders WHERE session_id = %s LIMIT 5", (prev_id,))
                orders = cursor.fetchall()
                print(f"\nSample orders from previous session (ID {prev_id}):")
                for order in orders:
                    print(order)
    
    conn.close()

if __name__ == "__main__":
    check_database() 