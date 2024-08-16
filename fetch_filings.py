import os

import sqlite3
import requests

from constant import DB_FILE

import pxx_util

year = 2018

app_util = pxx_util.AppUtil(year)
table_name = app_util.get_fillings_table_name()
# Connect SQLite database
conn = sqlite3.connect(DB_FILE)
conn.row_factory = sqlite3.Row
conn.execute(f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    url TEXT PRIMARY KEY,
    filename TEXT,
    file_date TEXT,
    cik TEXT,
    display_name TEXT,
    prop1 TEXT
);
""")
conn.commit()

# Prepare directories
os.makedirs(app_util.get_fillings_folder_path(), exist_ok=True)
os.makedirs(app_util.get_blocks_folder_path(), exist_ok=True)

# Fetch search results and download filings
session = requests.Session()
headers = {
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'accept-language': 'fr,en-US;q=0.9,en;q=0.8',
    'cache-control': 'no-cache',
    'origin': 'https://www.sec.gov',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://www.sec.gov/',
    'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
}
# curl 'https://efts.sec.gov/LATEST/search-index?q=Tesla&dateRange=custom&category=custom&startdt=2018-08-15&enddt=2019-01-01&forms=N-PX&page=3&from=200'
page = 1
cursor = 0
while True:
    print(f"Downloading page {page}")
    resp = session.get(
        f'https://efts.sec.gov/LATEST/search-index?q=Tesla&dateRange=custom&category=custom&startdt={year}-01-01&enddt={year + 1}-01-01&forms=N-PX&page={page}&from={cursor}',
        headers=headers)
    data = resp.json()
    total_hits = data['hits']['total']['value']
    # Elasticsearch results format:
    # {
    #     "hits": {
    #         "total": {
    #             "value": 224,
    #             ...
    #         },
    #         ...,
    #         "hits": [
    #             {
    #                 "_id": "0001193125-18-261549:d611177dnpx.htm",
    #                 "_source": {
    #                     "ciks": [
    #                         "0000916620"
    #                     ],
    #                     "adsh": "0001193125-18-261549",
    #                     ...
    #                 }
    #             },
    #             ...
    #         ]
    #     }
    # }
    # -> curl 'https://www.sec.gov/Archives/edgar/data/916620/000119312518261549/0001193125-18-261549.txt'
    for hit in data['hits']['hits']:
        display_names = ", ".join(hit['_source']['display_names'])
        cik = hit['_source']['ciks'][0]
        adsh = hit['_source']['adsh']
        file_date = hit['_source']['file_date']
        print(f"Hit: {display_names} (CIK={cik}, id={hit['_id']})")
        id = hit['_id']
        # 0001104659-18-053437:a18-15410_5npx.htm -> 000110465918053437/a18-15410_5npx.htm
        url = f"https://www.sec.gov/Archives/edgar/data/{str(int(cik))}/{adsh.replace('-', '')}/{adsh}.txt"
        filename = f"{cik}-{adsh}.txt"
        filepath = os.path.join(app_util.get_fillings_folder_path(), filename)
        # Find the row by filename
        cu = conn.cursor()
        cu.execute(f"SELECT * FROM {table_name} WHERE url = ?", (url,))
        if cu.fetchone():
            print(f"Updating {filename}")
            conn.execute(
                f"UPDATE {table_name} SET filename = ?, cik = ?, display_name = ?, file_date = ? WHERE url = ?",
                (filename, cik, display_names, file_date, url)
            )
        else:
            conn.execute(
                f"INSERT INTO {table_name} (url, filename, cik, display_name, file_date) VALUES (?, ?, ?, ?, ?)",
                (url, filename, cik, display_names, file_date)
            )
        cu.close()
        if not os.path.exists(filepath):
            print(f"Downloading {url} to {filepath}")
            file_data = session.get(url, headers=headers)
            if file_data.status_code != 200:
                print(f"Failed to download {url}, status code: {file_data.status_code}")
                print(hit)
                continue
            with open(filepath, 'wb') as f:
                f.write(file_data.content)
        if os.stat(filepath).st_size < 400:
            print(f"File {filepath} is too small, deleting it")
            os.remove(filepath)
    cursor += len(data['hits']['hits'])
    if cursor >= total_hits:
        break
    page += 1
conn.commit()
session.close()
