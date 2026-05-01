import sqlite3
import pandas as pd
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_to_db(df: pd.DataFrame, db_name: str = "stock_warehouse.db", table_name: str = "daily_stock_performance"):
    """
    Loads the transformed DataFrame into a relational SQLite database.
    """
    logging.info(f"Connecting to database: {db_name}...")
    
    # Create an output directory for the database file if it doesn't exist
    os.makedirs('data', exist_ok=True)
    db_path = os.path.join('data', db_name)
    
    try:
        # Establish a connection to the database
        conn = sqlite3.connect(db_path)
        
        logging.info(f"Loading {len(df)} records into table '{table_name}'...")
        
        # In a true enterprise environment, this step would use 'append' and complex SQL MERGE 
        # statements to avoid duplicates. Since we are pulling a fresh 30-day snapshot every time,
        # we will use 'replace' to refresh the table cleanly.
        df.to_sql(name=table_name, con=conn, if_exists='replace', index=False)
        
        logging.info("Database load completed successfully.")
        
    except Exception as e:
        logging.error(f"Database Load failed: {str(e)}")
        raise
        
    finally:
        # Always close the database connection to prevent memory leaks and database locks
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    print("This module handles database connections and is designed to be imported.")