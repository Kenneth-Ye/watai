import requests
import pandas as pd
import yaml
import time
import numpy as np
import os
from dotenv import load_dotenv

load_dotenv()

class AlphaVantageDataFetcher:
    def __init__(self, config_path='config.yaml'):
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        self.api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        if not self.api_key:
            raise ValueError("Please set ALPHA_VANTAGE_API_KEY environment variable")
        self.base_url = "https://www.alphavantage.co/query"
    
    def _api_call(self, params):
        """Make API call and handle rate limiting"""
        params['apikey'] = self.api_key
        response = requests.get(self.base_url, params=params)
        return response.json()
    
    def get_stock_data(self):
        """Fetch all required data for the configured stock"""
        symbol = self.config['symbol']
        print(f"Fetching data for {symbol}...")
        
        price_data = self._get_price_volume(symbol)
        
        price_data['volatility'] = price_data['close'].pct_change().rolling(20).std()
        
        rsi = self._get_rsi(symbol)
        macd = self._get_macd(symbol)
        sma = self._get_sma(symbol)
        
        combined_data = price_data.copy()
        if not rsi.empty:
            combined_data['rsi'] = rsi['RSI']
        if not macd.empty:
            combined_data['macd'] = macd['MACD']
            combined_data['macd_signal'] = macd['MACD_Signal']
        if not sma.empty:
            combined_data['sma_20'] = sma['SMA']
        
        return combined_data
    
    def _get_price_volume(self, symbol):
        """Get daily price and volume data"""
        params = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': symbol,
            'outputsize': 'compact'
        }
        
        data = self._api_call(params)
        ts_data = data['Time Series (Daily)']
        
        df = pd.DataFrame.from_dict(ts_data, orient='index', dtype=float)
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        
        # Rename columns
        df.columns = ['open', 'high', 'low', 'close', 'volume']
        return df
    
    def _get_rsi(self, symbol):
        """Get RSI indicator"""
        params = {
            'function': 'RSI',
            'symbol': symbol,
            'interval': 'daily',
            'time_period': 14,
            'series_type': 'close'
        }
        
        data = self._api_call(params)
        if 'Technical Analysis: RSI' in data:
            df = pd.DataFrame.from_dict(data['Technical Analysis: RSI'], orient='index', dtype=float)
            df.index = pd.to_datetime(df.index)
            return df.sort_index()
        return pd.DataFrame()
    
    def _get_macd(self, symbol):
        """Get MACD indicator"""
        params = {
            'function': 'MACD',
            'symbol': symbol,
            'interval': 'daily',
            'series_type': 'close'
        }
        
        data = self._api_call(params)
        if 'Technical Analysis: MACD' in data:
            df = pd.DataFrame.from_dict(data['Technical Analysis: MACD'], orient='index', dtype=float)
            df.index = pd.to_datetime(df.index)
            return df.sort_index()
        return pd.DataFrame()
    
    def _get_sma(self, symbol):
        """Get Simple Moving Average"""
        params = {
            'function': 'SMA',
            'symbol': symbol,
            'interval': 'daily',
            'time_period': 20,
            'series_type': 'close'
        }
        
        data = self._api_call(params)
        if 'Technical Analysis: SMA' in data:
            df = pd.DataFrame.from_dict(data['Technical Analysis: SMA'], orient='index', dtype=float)
            df.index = pd.to_datetime(df.index)
            return df.sort_index()
        return pd.DataFrame()

def main():
    fetcher = AlphaVantageDataFetcher()
    data = fetcher.get_stock_data()
    
    print(f"\nData fetched successfully!")
    print(f"Shape: {data.shape}")
    print(f"Date range: {data.index.min().date()} to {data.index.max().date()}")
    print(f"Columns: {list(data.columns)}")
    
    print(f"\nLatest data:")
    print(data.tail(3))
    
    symbol = fetcher.config['symbol']
    filename = f"{symbol}_stock_data.csv"
    data.to_csv(filename)
    print(f"\nData saved to {filename}")

if __name__ == "__main__":
    main()