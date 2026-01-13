import requests
import json
import asyncio
import aiohttp
import csv
import pandas as pd
import os
from dotenv import load_dotenv


# gets granual daily (high, low, close, volume)
DAILY_DATA_KEY = 'DAILY_DATA'
DAILY_DATA_ENDPOINT = 'https://api.tiingo.com/tiingo/daily/{}/prices?startDate=2024-1-01&resampleFreq=daily&token={}'

# gets P/E and marketCap data
FUNDAMENTALS_KEY = 'FUNDAMENTALS'
FUNDAMENTALS_ENDPOINT = 'https://api.tiingo.com/tiingo/fundamentals/{}/daily?startDate=2024-1-01&token={}'

ENDPOINT_DICT = {
    DAILY_DATA_KEY: DAILY_DATA_ENDPOINT,
    FUNDAMENTALS_KEY: FUNDAMENTALS_ENDPOINT
}

MISSING_TICKERS_SET = set()

async def fetch_data_for_ticker(ticker, api_token, endpoint_key, session):
    try: 
        print('fetching {} data for endpoint {}'.format(ticker, endpoint_key))
        endpoint = ENDPOINT_DICT[endpoint_key]
        request = endpoint.format(ticker, api_token)
        resp = await session.get(request)
        json_result =  await resp.json()
        return (ticker, json_result, endpoint_key)
    except Exception as e:
        print('Failed to fetch data for ticker {} due to error: {}'.format(ticker, e))
        return None

async def run_concurrent(tickers):
    tasks = []
    api_token = get_api_token()
    async with aiohttp.ClientSession() as session:
        for ticker in tickers:
            for endpoint_key in ENDPOINT_DICT.keys():
                tasks.append(fetch_data_for_ticker(ticker, api_token, endpoint_key, session))
        return await asyncio.gather(*tasks, return_exceptions=True)

def get_api_token():
    load_dotenv()
    api_key = os.getenv("API_KEY")
    return api_key

def filter_dfs(df_dict):
    columns_to_drop = ['high', 'low', 'open', 'adjHigh', 'adjLow', 'adjOpen', 'adjVolume', 'divCash']
    for df in df_dict.values():
        try:
            df.drop(columns_to_drop, axis=1, inplace=True)
        except Exception as e:
            ticker = df['ticker']
            print('Failed to filter columns for {} due to error: {}'.format(ticker, e))

def merge_daily_and_fundamental_data(response):
    df_dict = {}
    for ticker_dict in response:
        try:
            ticker = ticker_dict[0]
            ticker_data = ticker_dict[1]
            endpoint_key = ticker_dict[2]
            print('Processing {} for {}'.format(endpoint_key, ticker))
            # consolidate the daily and fundamentals into one df
            df = pd.DataFrame(ticker_data)
            df['ticker'] = ticker
            # shave off hour, mins, seconds from date column
            df["date"] = df["date"].str[:-14] 
            if ticker in df_dict:
                original_df = df_dict[ticker]
                df_merged = pd.merge(original_df, df, on=['date', 'ticker'])
                df_dict[ticker] = df_merged
            else:
                df_dict[ticker] = df
        except Exception as e:
            ticker = ticker_dict[0]
            print('unable to process {} due to error: {}'.format(ticker, e))
            MISSING_TICKERS_SET.add(ticker)

    return df_dict

def fetch_and_merged_data_sets():
    # Load meta data csv for active tickers
    df_fundamentals = pd.read_csv('fundamental_meta.csv', sep=',')
    tickers = df_fundamentals['ticker'].tolist()
    # Fetch daily + fundamental data
    response = asyncio.run(run_concurrent(tickers))
    df_dict = merge_daily_and_fundamental_data(response)
            
    # filter only the necessary fields
    filter_dfs(df_dict)

    # for ticker in tickers:
    for key, value in df_dict.items():
        try:
            ticker = key
            df = value
            file_name = './daily_data/{}.csv'.format(ticker)
            df.to_csv(file_name, index=False)
        except Exception as e:
            ticker = key
            print('unable to save {} due to error: {}'.format(ticker, e))
            MISSING_TICKERS_SET.add(ticker)
    
    print('MISSING TICKERS:{}'.format(MISSING_TICKERS_SET))


if __name__ == '__main__':
    fetch_and_merged_data_sets()
