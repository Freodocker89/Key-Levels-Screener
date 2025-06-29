import streamlit as st
import ccxt
import pandas as pd
from datetime import datetime, timedelta
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

# Store scan distances for debug output
debug_rows = []
valid_levels_rows = []
all_levels_logged = []

@st.cache_data(ttl=900)
def get_ohlcv(symbol, timeframe, since, limit=100):
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
    now = datetime.utcnow()
    start_of_this_week = now - timedelta(days=now.weekday())
    start_of_last_week = start_of_this_week - timedelta(weeks=1)
    start_of_this_month = now.replace(day=1)
    start_of_last_month = (start_of_this_month - timedelta(days=1)).replace(day=1)

    # Use weekly/monthly timeframes instead of 1d
    week_data = get_ohlcv(symbol, '1w', int(start_of_last_week.timestamp() * 1000), limit=10)
    month_data = get_ohlcv(symbol, '1M', int(start_of_last_month.timestamp() * 1000), limit=5)

    print(f"{symbol}: week_data={len(week_data)}, month_data={len(month_data)}")

    levels = {}

    if not week_data.empty and 'timestamp' in week_data.columns:
        if len(week_data) >= 2:
            levels['week_high'] = week_data['high'].iloc[-2]
            levels['week_low'] = week_data['low'].iloc[-2]
        elif len(week_data) >= 1:
            levels['week_high'] = week_data['high'].iloc[-1]
            levels['week_low'] = week_data['low'].iloc[-1]
        else:
            print(f"{symbol} - Not enough weekly candles")

    if not month_data.empty and 'timestamp' in month_data.columns:
        if len(month_data) >= 2:
            levels['month_high'] = month_data['high'].iloc[-2]
            levels['month_low'] = month_data['low'].iloc[-2]
        elif len(month_data) >= 1:
            levels['month_high'] = month_data['high'].iloc[-1]
            levels['month_low'] = month_data['low'].iloc[-1]
        else:
            print(f"{symbol} - Not enough monthly candles")

    print(f"{symbol} key levels: {levels}")
    return levels

def scan_symbol(symbol):
    result = {"week_high": None, "week_low": None, "month_high": None, "month_low": None}
    try:
        ticker = bitget.fetch_ticker(symbol)
        price = ticker['last']
        levels = get_last_week_month_levels(symbol)

        distances = {"symbol": symbol}
        if levels:
            valid_levels_rows.append(symbol)
            all_levels_logged.append({"symbol": symbol, **levels})

        for key in ["week_high", "week_low", "month_high", "month_low"]:
            if key in levels:
                dist = abs(price - levels[key]) / levels[key] * 100
                distances[key] = round(dist, 2)
                if dist <= proximity_threshold:
                    result[key] = (symbol, price, dist)
            else:
                distances[key] = "no_data"

        debug_rows.append(distances)

    except Exception as e:
        error_message = f"{type(e).__name__}: {str(e)}"
        debug_rows.append({"symbol": symbol, "error": error_message})
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
        df["Distance (%)"] = df["Distance (%)"].round(2)
        st.dataframe(df.sort_values("Distance (%)"), use_container_width=True)
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

# === Show Debug Tables ===
if debug_rows:
    debug_df = pd.DataFrame(debug_rows).fillna("-")
    st.subheader("🛠️ All Scan Distances")
    expected_cols = ["week_high", "week_low", "month_high", "month_low"]
    available_cols = [col for col in expected_cols if col in debug_df.columns]
    if available_cols:
        st.dataframe(debug_df.sort_values(by=available_cols, ascending=True, na_position="last"), use_container_width=True)
    else:
        st.dataframe(debug_df, use_container_width=True)

if valid_levels_rows:
    st.subheader("✅ Symbols with Valid Key Levels")
    st.dataframe(pd.DataFrame(valid_levels_rows, columns=["Symbol"]), use_container_width=True)

if all_levels_logged:
    st.subheader("🗓 Logged Key Levels (debug)")
    st.dataframe(pd.DataFrame(all_levels_logged), use_container_width=True)

# === Top 10 Closest Overall ===
if debug_rows:
    all_dists = pd.DataFrame(debug_rows).set_index("symbol")
    melted = all_dists.melt(ignore_index=False, var_name="Level", value_name="Distance")
    melted = melted[melted["Distance"] != "-"]
    melted = melted[melted["Distance"] != "no_data"]
    melted = melted.dropna()
    top10 = melted.sort_values("Distance").head(10).reset_index()
    st.subheader("🎪 Top 10 Closest to Key Levels")
    st.dataframe(top10, use_container_width=True)

