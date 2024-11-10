import requests
import pandas as pd
from sqlalchemy import create_engine
from requests.auth import HTTPBasicAuth

# Your API credentials
API_KEY = "nhmSXiLCyeSKKuYTOcH2b9C23"
API_KEY_SECRET = "G8YcrN9wftPd76dpRqbPHp0pSP0jjY5yrCvPObEXfMpk5FC5pm"

# MySQL RDS connection details
DB_HOST = "streetsmart-dw.c104ogauizib.us-east-2.rds.amazonaws.com"
DB_USER = "STREETSMART1"
DB_PASSWORD = "cF21cF43cF65!"
DB_NAME = "SANDBOX"
engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")

# Base URL for fetching tweets
API_URL = "https://api.twitter.com/2/tweets"  # Define the API base URL

# Function to get Bearer Token using API Key and API Secret
def get_bearer_token(api_key, api_key_secret):
    auth_url = 'https://api.twitter.com/oauth2/token'
    response = requests.post(
        auth_url,
        headers={'Content-Type': 'application/x-www-form-urlencoded'},
        data={'grant_type': 'client_credentials'},
        auth=HTTPBasicAuth(api_key, api_key_secret)
    )
    if response.status_code == 200:
        return response.json()['access_token']
    else:
        print(f"Failed to get token: {response.status_code}")
        return None

# Function to fetch data for specific tweets from Twitter API
def fetch_tweet_data(bearer_token):
    headers = {
        "Authorization": f"Bearer {bearer_token}"
    }
    tweet_ids = "1855029668455338017,1855372275635408949,1854622627886776805,1854237830295204077"  # Comma-separated Tweet IDs
    params = {
        "ids": tweet_ids,
        "tweet.fields": "created_at,public_metrics,text"  # Request creation date, engagement metrics, and text content
    }
    response = requests.get(API_URL, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json().get('data', [])
        return pd.DataFrame(data)  # Convert JSON response to DataFrame
    else:
        print(f"Error fetching tweets: {response.status_code}")
        return None

# Function to load data into MySQL
def load_data_to_mysql(data, table_name="twitter_data"):
    if data is not None and not data.empty:
        data.to_sql(table_name, con=engine, if_exists="append", index=False)
        print(f"Data loaded into {table_name}")
    else:
        print("No data to load")

# Main function
def main():
    bearer_token = get_bearer_token(API_KEY, API_KEY_SECRET)
    if bearer_token:
        tweets_data = fetch_tweet_data(bearer_token)  # Correct function name
        load_data_to_mysql(tweets_data)

if __name__ == "__main__":
    main()
