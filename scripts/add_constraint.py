from src.db_connection import get_db_connection

def add_constraint():
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Add unique constraint on parameter_sets(strategy_id, name)
        cur.execute("""
            ALTER TABLE parameter_sets
            ADD CONSTRAINT parameter_sets_strategy_id_name_key
            UNIQUE (strategy_id, name);
        """)
        conn.commit()
        print("Successfully added unique constraint on parameter_sets(strategy_id, name)")
    except Exception as e:
        print(f"Error adding constraint: {str(e)}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    add_constraint() 