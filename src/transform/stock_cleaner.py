import pandas as pd
import logging
import os

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_and_transform(df: pd.DataFrame, threshold: float = 5.0) -> pd.DataFrame:
    """
    Cleans raw stock data, calculates daily % change, and flags unusual movements.
    """
    logging.info("Starting data transformation...")

    try:
        # 1. Sort data to ensure chronological order for calculations
        df = df.sort_values(by=['Ticker', 'Date'])

        # 2. Calculate daily percentage change based on the 'Close' price
        # groupby('Ticker') ensures Apple's price doesn't accidentally calculate against Amazon's
        df['Daily_Pct_Change'] = df.groupby('Ticker')['Close'].pct_change() * 100

        # 3. Flag unusual movements 
        # We use .abs() because a 5% crash is just as unusual as a 5% surge
        df['Unusual_Movement'] = df['Daily_Pct_Change'].abs() > threshold

        # 4. Clean up the data format
        # The first day won't have a previous day to compare to, resulting in NaN (Not a Number)
        df['Daily_Pct_Change'] = df['Daily_Pct_Change'].fillna(0).round(2)
        
        # Round prices to 2 decimal places for a cleaner report
        for col in ['Open', 'High', 'Low', 'Close']:
            if col in df.columns:
                df[col] = df[col].round(2)

        flagged_count = df['Unusual_Movement'].sum()
        logging.info(f"Transformation complete. Flagged {flagged_count} unusual movements.")
        
        return df

    except Exception as e:
        logging.error(f"Transformation failed: {str(e)}")
        raise

def export_to_csv(df: pd.DataFrame, filename: str = "daily_stock_report.csv"):
    """
    Exports the cleaned dataframe to a CSV file.
    """
    # Create an output folder if it doesn't exist
    os.makedirs('output', exist_ok=True)
    filepath = os.path.join('output', filename)
    
    # Save to CSV
    df.to_csv(filepath, index=False)
    logging.info(f"Report successfully exported to {filepath}")

if __name__ == "__main__":
    print("This module contains transformation logic and is designed to be imported by main.py.")