import logging
from src.extract.api_client import fetch_stock_data
from src.transform.stock_cleaner import clean_and_transform, export_to_csv
from src.load.db_handler import load_to_db  # <-- NEW IMPORT

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline_execution.log"),
        logging.StreamHandler()
    ]
)

def run_stock_pipeline():
    logging.info("=== Starting Daily Stock ETL Pipeline ===")
    
    target_stocks = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'IVZ']
    
    try:
        # STEP 1: EXTRACT
        logging.info("Step 1: Extracting market data...")
        raw_data = fetch_stock_data(tickers=target_stocks, period="1mo")
        
        # STEP 2: TRANSFORM
        logging.info("Step 2: Cleaning data and applying business logic...")
        clean_data = clean_and_transform(df=raw_data, threshold=5.0)
        
        # STEP 3: LOAD (Export to CSV AND Load to SQL Database)
        logging.info("Step 3: Loading data to destinations...")
        
        # Keep the CSV for easy viewing
        export_to_csv(df=clean_data, filename="flagged_stock_report.csv")
        
        # Load into the SQL Database!
        load_to_db(df=clean_data)
        
        logging.info("=== Pipeline Completed Successfully ===")

    except Exception as e:
        logging.error(f"PIPELINE FAILED: {str(e)}")

if __name__ == "__main__":
    run_stock_pipeline()