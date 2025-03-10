from src.db_connection import get_db_connection
import pandas as pd

def check_tables():
    conn = get_db_connection()
    
    # Check strategies table schema
    schema_query = """
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns 
    WHERE table_name = 'strategies'
    ORDER BY ordinal_position;
    """
    df_schema = pd.read_sql(schema_query, conn)
    print("Strategies Table Schema:")
    print(df_schema)
    
    # Check parameter_sets table schema
    param_schema_query = """
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns 
    WHERE table_name = 'parameter_sets'
    ORDER BY ordinal_position;
    """
    df_param_schema = pd.read_sql(param_schema_query, conn)
    print("\nParameter Sets Table Schema:")
    print(df_param_schema)
    
    # Check constraints with more detail
    constraints_query = """
    SELECT 
        t.relname as table_name,
        c.conname as constraint_name,
        c.contype as constraint_type,
        pg_get_constraintdef(c.oid) as constraint_definition,
        CASE c.contype 
            WHEN 'p' THEN 'Primary Key'
            WHEN 'u' THEN 'Unique'
            WHEN 'f' THEN 'Foreign Key'
            WHEN 'c' THEN 'Check'
            ELSE c.contype::text
        END as constraint_type_desc
    FROM pg_constraint c
    JOIN pg_class t ON c.conrelid = t.oid
    WHERE t.relname IN ('strategies', 'parameter_sets')
    ORDER BY t.relname, c.contype;
    """
    df_constraints = pd.read_sql(constraints_query, conn)
    print("\nTable Constraints:")
    print(df_constraints)
    
    # Check indexes
    indexes_query = """
    SELECT 
        t.relname as table_name,
        i.relname as index_name,
        a.attname as column_name,
        ix.indisunique as is_unique
    FROM pg_class t
    JOIN pg_index ix ON t.oid = ix.indrelid
    JOIN pg_class i ON ix.indexrelid = i.oid
    JOIN pg_attribute a ON t.oid = a.attrelid
    WHERE a.attnum = ANY(ix.indkey)
    AND t.relname IN ('strategies', 'parameter_sets')
    ORDER BY t.relname, i.relname;
    """
    df_indexes = pd.read_sql(indexes_query, conn)
    print("\nTable Indexes:")
    print(df_indexes)
    
    conn.close()

if __name__ == "__main__":
    check_tables() 