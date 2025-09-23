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
trading_date = today # - timedelta(days=1)
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

df = con.sql("SELECT symbol as 'Ticker', date1 as 'Date', open_price as 'Open', high_price as 'High',low_price as 'Low',close_price as 'Close', deliv_qty as 'Volume' FROM daily_nse_price where series = 'EQ' and date1 > '2025-01-01'").df()
df["Volume"] = df["Volume"].astype(float)

def weekly_supertrend_daily(df, atr_period=10, multiplier=3):
    """
    df: DataFrame with ['Ticker','Date','Open','High','Low','Close']
    Returns df with 'SuperTrend' and 'Trend' columns applied to daily rows, based on weekly ATR
    """
    df = df.copy()
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values(['Ticker','Date'])

    result_list = []

    for ticker, group in df.groupby('Ticker'):
        # Resample weekly to get High, Low, Close for SuperTrend calculation
        weekly = group.resample('W-FRI', on='Date').agg({
            'High':'max',
            'Low':'min',
            'Close':'last'
        }).sort_index()

        # Calculate weekly ATR
        weekly['H-L'] = weekly['High'] - weekly['Low']
        weekly['H-Cp'] = abs(weekly['High'] - weekly['Close'].shift())
        weekly['L-Cp'] = abs(weekly['Low'] - weekly['Close'].shift())
        weekly['TR'] = weekly[['H-L','H-Cp','L-Cp']].max(axis=1)
        weekly['ATR'] = weekly['TR'].rolling(atr_period, min_periods=1).mean()

        # Basic bands
        weekly['Basic_Up'] = (weekly['High'] + weekly['Low'])/2 + multiplier*weekly['ATR']
        weekly['Basic_Down'] = (weekly['High'] + weekly['Low'])/2 - multiplier*weekly['ATR']

        # Weekly SuperTrend
        weekly['SuperTrend'] = 0
        for i in range(1,len(weekly)):
            prev = weekly.iloc[i-1]
            if prev['SuperTrend'] < prev['Close']:
                curr_st = max(weekly.iloc[i]['Basic_Down'], prev['SuperTrend'])
            else:
                curr_st = min(weekly.iloc[i]['Basic_Up'], prev['SuperTrend'])
            weekly.iloc[i, weekly.columns.get_loc('SuperTrend')] = curr_st

        # ---- 2. Map weekly SuperTrend to daily data ----
        group = group.set_index('Date')
        group['SuperTrend'] = weekly['SuperTrend'].reindex(group.index, method='ffill')

        # ---- 3. Daily Trend based on daily Close vs weekly SuperTrend ----
        group['Trend'] = group['Close'] > group['SuperTrend']

        group['Ticker'] = ticker
        result_list.append(group.reset_index())

    return pd.concat(result_list, ignore_index=True)

daily_with_weekly_st = weekly_supertrend_daily(df, atr_period=10, multiplier=2)

latest_supertrend = daily_with_weekly_st.groupby("Ticker").tail(1)

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

allstocks= pd.read_excel("data/allstocks.xlsx")

latest_signals = pd.merge(
    latest_signals,
    allstocks,
    left_on="Ticker",         # column name in latest_signals
    right_on="NSE Code",     # column name in allstocks
    how="left"           # inner join (only matching tickers)
)

latest_signals = latest_signals.merge(
    latest_supertrend[["Ticker","SuperTrend","Trend"]],
    on="Ticker",         # column name in latest_signals
    how="left"           # inner join (only matching tickers)
)

filtered = latest_signals[
    (latest_signals["Market Capitalization"] > 1000) &
    (latest_signals["PEG TTM PE to Growth"] > 0) &
    (latest_signals["PEG TTM PE to Growth"] < 2) &
    (latest_signals["ROCE Annual 3Yr Avg %"] > 15) &
    (latest_signals['Long Term Debt To Equity Annual'] < 0.5) &
    (latest_signals['Promoter holding latest %'] > 60) &
    (latest_signals['Promoter holding pledge percentage % Qtr'] < 0.01) &
    (latest_signals['Net Profit Qtr Growth YoY %'] > 0) &
    (latest_signals['Operating Revenue growth TTM %'] > 15) &
    (latest_signals['Cash EPS 5Yr Growth %'] > 15) &
    (latest_signals['EPS TTM Growth %'] > 15)
]

def send_dataframe_via_telegram(df, bot_token, chat_id, caption="DataFrame"):
    import matplotlib.pyplot as plt
    import requests
    
    # Render DataFrame as image
    fig, ax = plt.subplots(figsize=(6, 0.5 + 0.3*len(df)))  # auto height
    ax.axis("off")
    table = ax.table(
        cellText=df.values,
        colLabels=df.columns,
        cellLoc="center",
        loc="center"
    )
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.2)
    
    filename = "table.png"
    plt.savefig(filename, bbox_inches="tight")
    plt.close()
    
    # Send to Telegram
    url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
    with open(filename, "rb") as f:
        res = requests.post(url, data={"chat_id": chat_id, "caption": caption}, files={"photo": f})
    return res.json()

send_dataframe_via_telegram(filtered[["Stock Name","Trend"]], BOT_TOKEN, CHAT_ID, "Buy")
