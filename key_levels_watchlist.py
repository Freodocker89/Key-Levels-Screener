import streamlit as st
import ccxt
import pandas as pd
from datetime import datetime, timedelta

st.set_page_config(layout="wide")
st.title("📌 Key Levels Watchlist")

# === Config ===
PROXIMITY_DEFAULT = 0.5  # percent
PROXIMITY_MIN = 0.1
PROXIMITY_MAX = 10.0

# === UI Elements ===
st.sidebar.header("🔧 Filters")
check_week_high = st.sidebar.checkbox("Near Previous Week High", value=True)
check_week_low = st.sidebar.checkbox("Near Previous Week Low", value=True)
check_month_high = st.sidebar.checkbox("Near Previous Month High", value=True)
check_month_low = st.sidebar.checkbox("Near Previous Month Low", value=True)

proximity_threshold = st.sidebar.slider("Proximity Threshold (%)", PROXIMITY_MIN, PROXIMITY_MAX, PROXIMITY_DEFAULT, 0.1)

# === Initialize Exchange ===
bitget = ccxt.bitget()
markets = bitget.load_markets()
symbols = [s for s in markets if "/USDT:USDT" in s and markets[s]['type'] == 'swap']

@st.cache_data(ttl=900)
def get_ohlcv(symbol, timeframe, since):
    try:
        ohlcv = bitget.fetch_ohlcv(symbol, timeframe=timeframe, since=since)
        return pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
    except:
        return pd.DataFrame()

def get_last_week_month_levels(symbol):
    now = datetime.utcnow()
    start_of_this_week = now - timedelta(days=now.weekday())
    start_of_last_week = start_of_this_week - timedelta(weeks=1)
    start_of_this_month = now.replace(day=1)
    start_of_last_month = (start_of_this_month - timedelta(days=1)).replace(day=1)

    week_data = get_ohlcv(symbol, '1d', int(start_of_last_week.timestamp() * 1000))
    month_data = get_ohlcv(symbol, '1d', int(start_of_last_month.timestamp() * 1000))

    prev_week = week_data[week_data['timestamp'] < int(start_of_this_week.timestamp() * 1000)]
    prev_month = month_data[month_data['timestamp'] < int(start_of_this_month.timestamp() * 1000)]

    levels = {}
    if not prev_week.empty:
        levels['week_high'] = prev_week['high'].max()
        levels['week_low'] = prev_week['low'].min()
    if not prev_month.empty:
        levels['month_high'] = prev_month['high'].max()
        levels['month_low'] = prev_month['low'].min()

    return levels

# === Collect Matches ===
results = {"week_high": [], "week_low": [], "month_high": [], "month_low": []}

with st.spinner("Scanning key levels..."):
    for symbol in symbols:
        ticker = bitget.fetch_ticker(symbol)
        price = ticker['last']
        levels = get_last_week_month_levels(symbol)

        if 'week_high' in levels and check_week_high:
            dist = abs(price - levels['week_high']) / levels['week_high'] * 100
            if dist <= proximity_threshold:
                results['week_high'].append((symbol, price, dist))

        if 'week_low' in levels and check_week_low:
            dist = abs(price - levels['week_low']) / levels['week_low'] * 100
            if dist <= proximity_threshold:
                results['week_low'].append((symbol, price, dist))

        if 'month_high' in levels and check_month_high:
            dist = abs(price - levels['month_high']) / levels['month_high'] * 100
            if dist <= proximity_threshold:
                results['month_high'].append((symbol, price, dist))

        if 'month_low' in levels and check_month_low:
            dist = abs(price - levels['month_low']) / levels['month_low'] * 100
            if dist <= proximity_threshold:
                results['month_low'].append((symbol, price, dist))

# === Display Tables ===
def show_table(title, rows):
    if rows:
        df = pd.DataFrame(rows, columns=["Symbol", "Current Price", "Distance (%)"])
        df["Distance (%)"] = df["Distance (%)"].round(2)
        st.subheader(title)
        st.dataframe(df, use_container_width=True)

if check_month_high:
    show_table("📈 Near Previous Month High", results['month_high'])
if check_month_low:
    show_table("📉 Near Previous Month Low", results['month_low'])
if check_week_high:
    show_table("📈 Near Previous Week High", results['week_high'])
if check_week_low:
    show_table("📉 Near Previous Week Low", results['week_low'])
