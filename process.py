import os
import requests
import json
import time
import pandas as pd
from datetime import datetime
import pathlib

token = os.environ["QUIVER_API_KEY"]

headers = {'accept': 'application/json',
'X-CSRFToken': 'TyTJwjuEC7VV7mOqZ622haRaaUr0x0Ng4nrwSRFKQs7vdoBcJlK9qjAS69ghzhFu',
'Authorization': "Token "+token}

pathlib.Path('/temp-output-directory/alternative/quiver/twitter/').mkdir(parents=True, exist_ok=True)

company_url = "https://api.quiverquant.com/beta/companies"
twitter_url = "https://api.quiverquant.com/beta/historical/twitter/"
companies = requests.get(company_url, headers=headers).json()
for c in companies:
    ticker = c['Ticker']
    print("Processing ticker: ", ticker)
    i = 5
    while i!=0:
        try:
            
            ticker_twitter = requests.get(twitter_url+ticker, headers=headers).json()
            time.sleep(.03)
            line_list = []
            for row in sorted(ticker_twitter, key=lambda x: x['Date']):
                date = str(datetime.strptime(row['Date'], "%Y-%m-%d").strftime("%Y%m%d"))#.replace("-", "")
                followers = str(row['Followers'])
                pct_change_day = str(row['pct_change_day'])
                pct_change_week = str(row['pct_change_week'])
                pct_change_month = str(row['pct_change_month'])
                lines = [date, followers, pct_change_day, pct_change_week, pct_change_month]
                line = ",".join(lines)
                line_list.append(line)
            if len(line_list) < 1:
                print("No data for ", ticker)
                break

            csv_lines = "\n".join(line_list)

            with open('/temp-output-directory/alternative/quiver/twitter/' + ticker.lower() + '.csv', 'w') as ticker_file:
                ticker_file.write(csv_lines)
            print("Finished processing ", ticker)
            break       
        except Exception as e:
            print(e+" - failed to parse data for " + ticker)
            time.sleep(1)
            i-=1
