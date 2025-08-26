import time
from datetime import datetime, timezone, timedelta
import requests
import json
import os
from dotenv import load_dotenv
import duckdb

load_dotenv()

con = duckdb.connect("metrics.duckdb")

_INITIAL_RUN=False

con.execute(f"""
CREATE TABLE IF NOT EXISTS s1_metrics (
    Timestamp TIMESTAMPTZ,
    Query VARCHAR,
    Result INTEGER NULL
);
""")

def load_queries(filename='queries.json'):
    with open(filename, 'r', encoding='utf-8') as f:
        queries = json.load(f)
    return queries

queries = load_queries()
print(f'Loaded queries: {queries}')

def collect_metrics():
    current_time = datetime.now(timezone.utc)
    print(f'Collecting metrics @ {current_time}')

    for query in queries:
        data = {
            'Timestamp': f'{current_time}',
            'Query': query,
            'Result': get_count_query(query)
        }
        print(f'Writing: {data}')
        con.execute(f"""INSERT INTO s1_metrics (Timestamp, Query, Result) VALUES (?, ?, ?)""", (data['Timestamp'], data['Query'], data['Result']))

def get_count_query(params):
    url = f'https://{os.getenv("SENTINELONE_URL")}.sentinelone.net/web/api/v2.1/agents/count?{params}'
    headers = {
        'Accept': 'application/json',
        'Authorization': 'ApiToken ' + os.getenv('SENTINELONE_AUTH_TOKEN')
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        # print(response.json())
        return response.json()['data']['total']
    except requests.exceptions.RequestException as e:
        print(f"Error making GET request to {url}: {e}")
        return None

try:
    if _INITIAL_RUN:
        print('Performing initial startup collection.')
        collect_metrics()
    while True:
        now = datetime.now()
        # Calculate seconds to sleep until the next minute starts
        seconds_to_next_minute = 60 - now.second - now.microsecond / 1_000_000
        print(f'Waiting {seconds_to_next_minute} seconds to collect.')
        time.sleep(seconds_to_next_minute)

        temp_queries = load_queries()
        if temp_queries != queries:
            queries = temp_queries
            print(f'Loaded new queries: {queries}')
        
        # Gets weird if this takes more than 1 minute...
        collect_metrics()
except Exception as e:    
    print(f"Error: {e}")
finally:
    con.close()
