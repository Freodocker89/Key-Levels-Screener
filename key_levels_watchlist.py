import streamlit as st
import ccxt
import pandas as pd
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(layout="wide")
st.title("📌 Key Levels Watchlist")

# === Config ===
PROXIMITY_DEFAULT = 2.0
PROXIMITY_MIN = 0.1
PROXIMITY_MAX = 20.0

# === UI Elements ===
st.sidebar.header("🔧 Filters")
check_week_high = st.sidebar.checkbox("Near Previous Week High", value=True)
check_week_low = st.sidebar.checkbox("Near Previous Week Low", value=True)
check_month_high = st.sidebar.checkbox("Near Previous Month High", value=True)
check_month_low = st.sidebar.checkbox("Near Previous Month Low", value=True)

proximity_threshold = st.slider("🎯 Proximity Threshold (%)", PROXIMITY_MIN, PROXIMITY_MAX, PROXIMITY_DEFAULT, 0.1)

# === Initialize Exchange ===
bitget = ccxt.bitget()
markets = bitget.load_markets()
symbols = [s for s in markets if "/USDT:USDT" in s and markets[s]['type'] == 'swap']

@st.cache_data(ttl=900)
def get_ohlcv(symbol, timeframe, since, limit=200):
    try:
        ohlcv = bitget.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=limit)
        if not ohlcv:
            print(f"No OHLCV data for {symbol} on {timeframe}")
            return pd.DataFrame()
        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        return df
    except Exception as e:
        print(f"Error fetching OHLCV for {symbol}: {e}")
        return pd.DataFrame()

def get_last_week_month_levels(symbol):
    now = datetime.utcnow() + timedelta(hours=8)
    start_of_this_week = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    start_of_last_week = start_of_this_week - timedelta(weeks=1)
    end_of_last_week = start_of_this_week

    start_of_this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    start_of_last_month = (start_of_this_month - timedelta(days=1)).replace(day=1)
    end_of_last_month = start_of_this_month

    since = int((start_of_last_month - timedelta(days=5)).timestamp() * 1000)
    df = get_ohlcv(symbol, '1d', since, limit=100)
    if df.empty:
        return {}

    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms') + pd.Timedelta(hours=8)

    week_df = df[(df['datetime'] >= start_of_last_week) & (df['datetime'] < end_of_last_week)]
    month_df = df[(df['datetime'] >= start_of_last_month) & (df['datetime'] < end_of_last_month)]

    levels = {}

    if not week_df.empty:
        levels['week_high'] = week_df['high'].max()
        levels['week_low'] = week_df['low'].min()

    if not month_df.empty:
        levels['month_high'] = month_df['high'].max()
        levels['month_low'] = month_df['low'].min()

    print(f"{symbol} levels: {levels}")
    return levels

def scan_symbol(symbol):
    result = {"week_high": None, "week_low": None, "month_high": None, "month_low": None}
    try:
        ticker = bitget.fetch_ticker(symbol)
        price = ticker['last']
        levels = get_last_week_month_levels(symbol)

        for key in ["week_high", "week_low", "month_high", "month_low"]:
            if key in levels and levels[key]:
                diff = price - levels[key]
                dist = abs(diff) / levels[key] * 100
                sign = "+" if diff > 0 else "-"
                if dist <= proximity_threshold:
                    signed_dist = float(f"{sign}{round(dist, 2)}")
                    result[key] = (symbol, price, signed_dist)

    except Exception as e:
        pass
    return result

# === Collect Matches ===
results = {"week_high": [], "week_low": [], "month_high": [], "month_low": []}
progress_bar = st.progress(0)
progress_text = st.empty()
total = len(symbols)
completed = 0

with st.spinner("Scanning key levels in parallel..."):
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(scan_symbol, symbol) for symbol in symbols]
        for future in as_completed(futures):
            res = future.result()
            for key in results:
                if res[key]:
                    results[key].append(res[key])
            completed += 1
            progress_bar.progress(completed / total)
            progress_text.text(f"Scanning progress: {completed}/{total}")

progress_bar.empty()
progress_text.empty()

# === Display Tables ===
def show_table(title, rows):
    st.subheader(title)
    if rows:
        df = pd.DataFrame(rows, columns=["Symbol", "Current Price", "Distance (%)"])
        df["Distance (%)"] = df["Distance (%)"].astype(float)
        df = df.sort_values("Distance (%)")
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

