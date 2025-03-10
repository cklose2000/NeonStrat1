import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional

# Load environment variables from .env file
load_dotenv()

# Database connection parameters
DATABASE_URL = os.getenv('DATABASE_URL')

# Establish a connection to the PostgreSQL database
connection = psycopg2.connect(DATABASE_URL)

def get_db_connection():
    """Get a connection to the PostgreSQL database."""
    try:
        db_url = os.getenv('DATABASE_URL')
        if not db_url:
            raise ValueError("DATABASE_URL environment variable is not set")
        
        conn = psycopg2.connect(db_url)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {str(e)}")
        raise

def test_connection():
    """
    Test the database connection by executing a simple query.
    """
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Test query to check connection
                cur.execute('SELECT version();')
                version = cur.fetchone()
                print(f"PostgreSQL Version: {version['version']}")
        except Exception as e:
            print(f"Error executing query: {e}")
        finally:
            conn.close()
            print("Database connection closed.")

def list_tables():
    """
    List all tables in the database.
    """
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Query to get all tables in the database
                cur.execute("""
                    SELECT table_schema, table_name 
                    FROM information_schema.tables 
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                    ORDER BY table_schema, table_name;
                """)
                tables = cur.fetchall()
                
                if not tables:
                    print("No tables found in the database.")
                else:
                    print("\nExisting tables in the database:")
                    print("--------------------------------")
                    current_schema = None
                    for table in tables:
                        if current_schema != table['table_schema']:
                            current_schema = table['table_schema']
                            print(f"\nSchema: {current_schema}")
                        print(f"  → {table['table_name']}")
                    print("--------------------------------")
                
        except Exception as e:
            print(f"Error listing tables: {e}")
        finally:
            conn.close()
            print("\nDatabase connection closed.")

def drop_all_tables_public_schema():
    """
    Drops all tables in the public schema after confirmation.
    """
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get all tables in public schema
                cur.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name;
                """)
                tables = cur.fetchall()
                
                if not tables:
                    print("No tables found in the public schema.")
                    return
                
                print("\nThe following tables will be dropped from the public schema:")
                print("----------------------------------------------------")
                for table in tables:
                    print(f"  → {table['table_name']}")
                print("----------------------------------------------------")
                
                confirmation = input("\nWARNING: This will permanently delete all tables and their data in the public schema.\nType 'YES' to confirm: ")
                
                if confirmation == "YES":
                    # Drop all tables using CASCADE to handle dependencies
                    for table in tables:
                        table_name = table['table_name']
                        try:
                            print(f"Dropping table: {table_name}")
                            cur.execute(f'DROP TABLE IF EXISTS public."{table_name}" CASCADE;')
                            conn.commit()  # Commit after each table drop
                        except Exception as e:
                            print(f"Error dropping table {table_name}: {e}")
                            conn.rollback()  # Rollback on error
                            continue
                    
                    print("\nTable drop operations completed.")
                else:
                    print("\nOperation cancelled. No tables were dropped.")
                
        except Exception as e:
            conn.rollback()
            print(f"Error: {e}")
        finally:
            conn.close()
            print("\nDatabase connection closed.")

# Function to fetch market data efficiently
def fetch_market_data(instrument_id: int, start_date: str, end_date: str):
    query = """
    SELECT * FROM tick_data 
    WHERE instrument_id = %s 
    AND timestamp BETWEEN %s AND %s
    ORDER BY timestamp;
    """
    with connection.cursor() as cursor:
        cursor.execute(query, (instrument_id, start_date, end_date))
        data = cursor.fetchall()
    return data

def verify_spy_data_presence(instrument_id: int, start_date: str, end_date: str) -> bool:
    """Verify the presence of 5-minute SPY data in the database for the specified period.
    
    Args:
        instrument_id: ID of the instrument (e.g., SPY)
        start_date: Start date for the data check
        end_date: End date for the data check
        
    Returns:
        bool: True if data is present, False otherwise
    """
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                query = """
                SELECT COUNT(*) FROM tick_data 
                WHERE instrument_id = %s 
                AND timestamp BETWEEN %s AND %s
                """
                cursor.execute(query, (instrument_id, start_date, end_date))
                count = cursor.fetchone()[0]
                return count > 0
        except Exception as e:
            print(f"Error verifying data presence: {e}")
        finally:
            conn.close()
    return False

def list_non_empty_tables() -> None:
    """List all tables in the database that have more than 0 rows."""
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Query to get all tables with more than 0 rows
                cur.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    AND table_type = 'BASE TABLE'
                    AND table_name NOT IN ('pg_catalog', 'information_schema')
                """)
                tables = cur.fetchall()
                non_empty_tables = []
                for table in tables:
                    table_name = table['table_name']
                    cur.execute(f'SELECT COUNT(*) FROM {table_name}')
                    count = cur.fetchone()['count']
                    if count > 0:
                        non_empty_tables.append(table_name)

                if not non_empty_tables:
                    print("No non-empty tables found in the database.")
                else:
                    print("\nNon-empty tables in the database:")
                    print("--------------------------------")
                    for table_name in non_empty_tables:
                        print(f"  → {table_name}")
                    print("--------------------------------")
        except Exception as e:
            print(f"Error listing non-empty tables: {e}")
        finally:
            conn.close()

def get_spy_instrument_id() -> Optional[int]:
    """Get the instrument ID for SPY from the instruments table."""
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT instrument_id, symbol, exchange
                    FROM instruments 
                    WHERE symbol = 'SPY'
                """)
                result = cursor.fetchone()
                if result:
                    print(f"Found SPY instrument: ID={result[0]}, Exchange={result[2]}")
                    return result[0]
                else:
                    print("SPY instrument not found in database")
                    return None
        finally:
            conn.close()
    return None

def check_bars_5m_data(instrument_id: int, start_date: str, end_date: str) -> None:
    """Check for 5-minute bar data in the specified period."""
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # First check the count
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM bars_5m 
                    WHERE instrument_id = %s 
                    AND timestamp BETWEEN %s AND %s
                """, (instrument_id, start_date, end_date))
                count = cursor.fetchone()['count']
                print(f"\nFound {count} 5-minute bars for the specified period")

                if count > 0:
                    # Get sample of the data
                    cursor.execute("""
                        SELECT timestamp, open, high, low, close, volume
                        FROM bars_5m 
                        WHERE instrument_id = %s 
                        AND timestamp BETWEEN %s AND %s
                        ORDER BY timestamp
                        LIMIT 5
                    """, (instrument_id, start_date, end_date))
                    print("\nSample of available data:")
                    print("--------------------------------")
                    for row in cursor.fetchall():
                        print(f"Timestamp: {row['timestamp']}")
                        print(f"OHLCV: {row['open']}, {row['high']}, {row['low']}, {row['close']}, {row['volume']}")
                        print("--------------------------------")
        finally:
            conn.close()

def check_table_schema(table_name: str) -> None:
    """Check the schema of a specific table.
    
    Args:
        table_name: Name of the table to check
    """
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Get column information
                cursor.execute("""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_name = %s
                    ORDER BY ordinal_position;
                """, (table_name,))
                
                print(f"\nSchema for table '{table_name}':")
                print("--------------------------------")
                for col in cursor.fetchall():
                    print(f"Column: {col['column_name']}")
                    print(f"Type: {col['data_type']}")
                    print(f"Nullable: {col['is_nullable']}")
                    print(f"Default: {col['column_default']}")
                    print("--------------------------------")
                
                # Get constraints
                cursor.execute("""
                    SELECT c.conname as constraint_name,
                           c.contype as constraint_type,
                           pg_get_constraintdef(c.oid) as definition
                    FROM pg_constraint c
                    JOIN pg_namespace n ON n.oid = c.connamespace
                    WHERE conrelid = %s::regclass
                    AND n.nspname = 'public'
                """, (table_name,))
                
                print("\nConstraints:")
                print("--------------------------------")
                for constraint in cursor.fetchall():
                    print(f"Name: {constraint['constraint_name']}")
                    print(f"Type: {constraint['constraint_type']}")
                    print(f"Definition: {constraint['definition']}")
                    print("--------------------------------")
        finally:
            conn.close()

if __name__ == "__main__":
    list_tables()
    instrument_id = 42  # Assuming 42 is the instrument_id for SPY
    start_date = '2025-01-30'
    end_date = '2025-02-28'
    data_present = verify_spy_data_presence(instrument_id, start_date, end_date)
    print(f"Data present: {data_present}")
    list_non_empty_tables()
    
    # Get the correct instrument ID for SPY
    spy_id = get_spy_instrument_id()
    if spy_id is not None:
        # Check the bars_5m table
        check_bars_5m_data(spy_id, start_date, end_date)
    
    check_table_schema('strategies')
    check_table_schema('parameter_sets') 