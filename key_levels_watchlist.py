import streamlit as st
import ccxt
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

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
# Use a few known active pairs for testing
symbols = [s for s in ["BTC/USDT:USDT", "ETH/USDT:USDT", "XRP/USDT:USDT", "SOL/USDT:USDT", "BNB/USDT:USDT"] if s in markets]

@st.cache_data(ttl=900)
def get_ohlcv(symbol, timeframe, since):
    try:
        ohlcv = bitget.fetch_ohlcv(symbol, timeframe=timeframe, since=since)
        if not ohlcv:
            return pd.DataFrame()
        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        return df
    except Exception as e:
        st.write(f"OHLCV fetch failed for {symbol}: {e}")
        return pd.DataFrame()

def get_last_week_month_levels(symbol):
    now = datetime.utcnow()
    start_of_this_week = now - timedelta(days=now.weekday())
    start_of_last_week = start_of_this_week - timedelta(weeks=1)
    start_of_this_month = now.replace(day=1)
    start_of_last_month = (start_of_this_month - timedelta(days=1)).replace(day=1)

    week_data = get_ohlcv(symbol, '1d', int(start_of_last_week.timestamp() * 1000))
    month_data = get_ohlcv(symbol, '1d', int(start_of_last_month.timestamp() * 1000))

    levels = {}

    if not week_data.empty and 'timestamp' in week_data.columns:
        prev_week = week_data[week_data['timestamp'] < int(start_of_this_week.timestamp() * 1000)]
        if not prev_week.empty:
            levels['week_high'] = prev_week['high'].max()
            levels['week_low'] = prev_week['low'].min()

    if not month_data.empty and 'timestamp' in month_data.columns:
        prev_month = month_data[month_data['timestamp'] < int(start_of_this_month.timestamp() * 1000)]
        if not prev_month.empty:
            levels['month_high'] = prev_month['high'].max()
            levels['month_low'] = prev_month['low'].min()

    return levels

def scan_symbol(symbol, progress_text):
    result = {"week_high": None, "week_low": None, "month_high": None, "month_low": None}
    try:
        progress_text.text(f"Scanning {symbol}...")
        ticker = bitget.fetch_ticker(symbol)
        price = ticker['last']
        levels = get_last_week_month_levels(symbol)

        if 'week_high' in levels:
            dist = abs(price - levels['week_high']) / levels['week_high'] * 100
            if dist <= proximity_threshold:
                result['week_high'] = (symbol, price, dist)

        if 'week_low' in levels:
            dist = abs(price - levels['week_low']) / levels['week_low'] * 100
            if dist <= proximity_threshold:
                result['week_low'] = (symbol, price, dist)

        if 'month_high' in levels:
            dist = abs(price - levels['month_high']) / levels['month_high'] * 100
            if dist <= proximity_threshold:
                result['month_high'] = (symbol, price, dist)

        if 'month_low' in levels:
            dist = abs(price - levels['month_low']) / levels['month_low'] * 100
            if dist <= proximity_threshold:
                result['month_low'] = (symbol, price, dist)

    except Exception as e:
        st.write(f"Error scanning {symbol}: {e}")
    return result

# === Collect Matches ===
results = {"week_high": [], "week_low": [], "month_high": [], "month_low": []}
progress_bar = st.progress(0)
progress_text = st.empty()
total = len(symbols)
completed = 0

with st.spinner("Scanning key levels in parallel..."):
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(scan_symbol, symbol, progress_text) for symbol in symbols]
        for future in as_completed(futures):
            res = future.result()
            for key in results:
                if res[key]:
                    results[key].append(res[key])
            completed += 1
            progress_bar.progress(completed / total)

progress_bar.empty()
progress_text.empty()

# === Display Tables ===
def show_table(title, rows):
    st.subheader(title)
    if rows:
        df = pd.DataFrame(rows, columns=["Symbol", "Current Price", "Distance (%)"])
        df["Distance (%)"] = df["Distance (%)"].round(2)
        st.dataframe(df, use_container_width=True)
    else:
        st.info("No matches found.")

if check_month_high:
    show_table("📈 Near Previous Month High", results['month_high'])
if check_month_low:
    show_table("📉 Near Previous Month Low", results['month_low'])
if check_week_high:
    show_table("📈 Near Previous Week High", results['week_high'])
if check_week_low:
    show_table("📉 Near Previous Week Low", results['week_low'])
