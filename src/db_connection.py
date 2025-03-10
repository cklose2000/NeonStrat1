import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

def get_db_connection():
    """
    Create a connection to NeonDB using environment variables.
    Returns a database connection object.
    """
    # Load environment variables from .env file
    load_dotenv()
    
    try:
        # Create connection using environment variables
        connection = psycopg2.connect(
            host=os.getenv('PGHOST'),
            database=os.getenv('PGDATABASE'),
            user=os.getenv('PGUSER'),
            password=os.getenv('PGPASSWORD'),
            port=os.getenv('PGPORT'),
            sslmode=os.getenv('PGSSLMODE')
        )
        print("Successfully connected to NeonDB!")
        return connection
    except Exception as e:
        print(f"Error connecting to NeonDB: {e}")
        return None

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

if __name__ == "__main__":
    list_tables() 