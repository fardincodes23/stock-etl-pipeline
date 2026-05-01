import logging
import os
from src.extract.api_client import fetch_stock_data
from src.transform.stock_cleaner import clean_and_transform, export_to_csv

# Setup master pipeline logging
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
    
    # 1. Define target stocks (Apple, Google, Microsoft, Amazon, Invesco)
    target_stocks = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'IVZ']
    
    try:
        # STEP 1: EXTRACT
        # We are pulling "1mo" (1 month) of data so we can calculate daily % changes
        logging.info("Step 1: Extracting market data...")
        raw_data = fetch_stock_data(tickers=target_stocks, period="1mo")
        
        # STEP 2: TRANSFORM
        logging.info("Step 2: Cleaning data and applying business logic...")
        clean_data = clean_and_transform(df=raw_data, threshold=5.0)
        
        # STEP 3: LOAD (Export)
        logging.info("Step 3: Loading data to destination...")
        export_to_csv(df=clean_data, filename="flagged_stock_report.csv")
        
        logging.info("=== Pipeline Completed Successfully ===")

    except Exception as e:
        logging.error(f"PIPELINE FAILED: {str(e)}")

if __name__ == "__main__":
    # Ensure the root directory is set correctly for imports if running from command line
    run_stock_pipeline()