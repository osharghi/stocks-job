import requests
import json
import asyncio
import aiohttp
import csv
import pandas as pd
import os
from dotenv import load_dotenv


# fundamental meta
META_ENDPOINT = "https://api.tiingo.com/tiingo/fundamentals/meta"

async def fetch_meta_data():
    payload = get_api_token()
    async with aiohttp.ClientSession() as session:
        async with session.get(META_ENDPOINT, params=payload) as response:
            json_data = await response.json()
            return json_data

def get_api_token():
    load_dotenv()
    api_key = os.getenv("API_KEY")
    payload = {'token': api_key}
    return payload
    
def get_fundamentals_meta_data():
    json_result = asyncio.run(fetch_meta_data())
    df = pd.DataFrame(json_result)
    filtered_df = df[df['isActive'] == True].copy()
    file_name = 'fundamental_meta.csv'
    filtered_df.to_csv(file_name, index=False)
    return filtered_df

if __name__ == '__main__':
    get_fundamentals_meta_data()