from src.db_connection import get_db_connection
import pandas as pd

def check_data():
    conn = get_db_connection()
    query = """
    SELECT COUNT(*) as count 
    FROM bars 
    WHERE timestamp >= '2025-01-30'::date 
    AND timestamp <= '2025-02-28'::date
    """
    df = pd.read_sql(query, conn)
    print(f"Number of records: {df['count'].iloc[0]}")
    
    # Check some sample data
    sample_query = """
    SELECT * FROM bars 
    WHERE timestamp >= '2025-01-30'::date 
    AND timestamp <= '2025-02-28'::date
    ORDER BY timestamp
    LIMIT 5
    """
    df_sample = pd.read_sql(sample_query, conn)
    print("\nSample data:")
    print(df_sample)
    conn.close()

if __name__ == "__main__":
    check_data() 