# -*- coding: utf-8 -*-

from datetime import date, timedelta, datetime
import pandas as pd
import nselib
from nselib import capital_market
from sqlalchemy import create_engine
import numpy as np
import os
import requests
from growwapi import GrowwAPI
import pyotp
import json
import duckdb

# Creating the SQL connection to free neon.tech database
connection_string = os.getenv("NEON_TECH_CONNECTION")
engine = create_engine(connection_string)

motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
con = duckdb.connect(f"md:?motherduck_token={motherduck_token}")

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = '5798902540'

today = date.today()
trading_date = today- timedelta(days=1)
check_table_date = today- timedelta(days=7)
date_str = trading_date.strftime('%d-%m-%Y')

try:
  new_data = capital_market.bhav_copy_with_delivery(trade_date=date_str)
except FileNotFoundError:
  whatsapp_message = f'No Bhav copy for {date_str}'
  url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'
  payload = {
    'chat_id': CHAT_ID,
    'text': whatsapp_message
  }
  exit(1)  # Program stops here

new_data.columns = [col.lower() for col in new_data.columns]
new_data['date1']=pd.to_datetime(new_data['date1'],format='%d-%b-%Y')

# Convert columns to numeric, coercing errors and filling NaN with 0
for col in ['deliv_qty', 'deliv_per', 'last_price']:
  new_data[col] = pd.to_numeric(new_data[col], errors='coerce').fillna(0)

# STEP 1: Get existing composite keys from the table
query = f"SELECT date1, symbol FROM daily_nse_price WHERE date1 > '{check_table_date}'"
existing_keys = pd.read_sql(
  query,
  con=engine,
  parse_dates=['date1']
)

# STEP 2: Merge to filter out duplicates
merged = pd.merge(
  new_data,
  existing_keys,
  on=['date1', 'symbol'],
  how='left',
  indicator=True
)

# Keep only new rows
df_to_insert = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])

# STEP 3: Insert only new rows
if not df_to_insert.empty:
  df_to_insert.to_sql('daily_nse_price', con=engine, if_exists='append', index=False)
  con.register("df_to_insert", df_to_insert).execute("INSERT INTO daily_nse_price SELECT * FROM df_to_insert")
  whatsapp_message  = f"NSE bhav copy inserted {len(df_to_insert)} new rows for {date_str}."
else:
  whatsapp_message = f"NSE bhav copy — all entries for {date_str} already exist."

url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'
payload = {
    'chat_id': CHAT_ID,
    'text': whatsapp_message
}

response = requests.post(url, data=payload)
print(response.json())

df = con.sql("SELECT symbol as 'Ticker', date1 as 'Date', open_price as 'Open', high_price as 'High',low_price as 'Low',close_price as 'Close', deliv_qty as 'Volume' FROM daily_nse_price where series = 'EQ'").df()
df["Volume"] = df["Volume"].astype(float)

price_threshold = 0.03
volume_threshold = 2.0

latest_date = df["Date"].max()
results = []

for ticker, group in df.groupby("Ticker"):
    group = group.sort_values("Date").reset_index(drop=True)

    # Calculate daily % price change (Close vs Previous Close)
    group["Pct_Change"] = group["Close"].pct_change()

    # Calculate rolling average volume (last 20 days)
    group["Avg_Volume"] = group["Volume"].rolling(window=20, min_periods=5).mean()

    # Signal: price up > threshold & volume > threshold * avg_volume
    group["Signal"] = (group["Pct_Change"] > price_threshold) & (group["Volume"] > volume_threshold * group["Avg_Volume"])

    results.append(group)

result_df = pd.concat(results)

signals = result_df[result_df["Signal"]]
latest_signals = signals[signals["Date"] == latest_date]
