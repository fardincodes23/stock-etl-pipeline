import yfinance as yf
import pandas as pd
import logging

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_stock_data(tickers: list, period: str = "5d") -> pd.DataFrame:
    """
    Fetches historical market data from the Yahoo Finance API.
    """
    logging.info(f"Initiating API extraction for tickers: {tickers}")
    
    try:
        # yfinance allows downloading multiple tickers at once
        raw_data = yf.download(tickers, period=period, group_by='ticker', progress=False)
        
        # If we only pass one ticker, yf structures the dataframe differently. 
        # This standardizes it so it's always a clean, flat table.
        if len(tickers) == 1:
            raw_data = pd.concat({tickers[0]: raw_data}, names=['Ticker'])
        else:
            raw_data = raw_data.stack(level=0, future_stack=True).rename_axis(['Date', 'Ticker'])
            
        raw_data = raw_data.reset_index()
        logging.info(f"Successfully extracted {len(raw_data)} rows of market data.")
        
        return raw_data

    except Exception as e:
        logging.error(f"API Extraction failed: {str(e)}")
        raise

if __name__ == "__main__":
    # Quick local test to prove the API works
    target_stocks = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'IVZ']
    
    print("Testing API Connection...")
    df = fetch_stock_data(target_stocks, period="1d") # Just pulling the last 1 day for a quick test
    print(df.head(10))