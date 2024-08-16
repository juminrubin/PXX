import os
import sqlite3
from constant import DB_FILE

import pxx_util

year = 2018
app_util = pxx_util.AppUtil(year)
table_name = app_util.get_fillings_table_name()

# Create SQLite database
conn = sqlite3.connect(DB_FILE)
try:
    conn.row_factory = sqlite3.Row

    print("File date,Filing entity/person,Prop 1 vote,URL")
    # Export each filing in CSV format
    for row in conn.execute(f'SELECT * FROM {table_name} ORDER BY cik, file_date'):
        file_date = row['file_date']
        cik = row['cik']
        display_name = row['display_name']
        prop1 = row['prop1']
        if prop1 is None:
            prop1 = "unknown"
        url = row['url']
        print(f'{file_date},"{display_name}",{prop1},"{url}"')
except BaseException as e:
    print(f"An error occurred: {e}")

finally:
    conn.close()
