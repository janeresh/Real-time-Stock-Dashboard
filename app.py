import os
import time
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import streamlit as st

# Environment variables
load_dotenv()

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB   = os.getenv("POSTGRES_DB", "stock_db")
PG_USER = os.getenv("POSTGRES_USER", "user")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "pass")

# Dashboard
st.set_page_config(page_title="Live Stock Dashboard", layout="wide")
st.title("📈 Real-Time Stock Dashboard")

refresh_secs = st.sidebar.slider("Refresh every (seconds)", 1, 30, 5)
window_mins  = st.sidebar.slider("Time window (minutes)", 1, 120, 15)

symbols = st.sidebar.text_input("Filter symbols", "")
symbols = [s.strip().upper() for s in symbols.split(",") if s.strip()] if symbols else None

# Connection
conn = psycopg2.connect(
    host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
)

@st.cache_data(ttl=5)
def load_data(window_mins, symbols):
    where = "timestamp >= NOW() - INTERVAL %s"
    params = [f"{window_mins} minutes"]
    if symbols:
        where += f" AND symbol = ANY(%s)"
        params.append(symbols)
    q = f"""
        SELECT timestamp, symbol, price
        FROM stock_prices
        WHERE {where}
        ORDER BY timestamp ASC
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q, params)
        rows = cur.fetchall()
    df = pd.DataFrame(rows)
    if df.empty:
        return df, pd.DataFrame()
    # wide format for multi-line chart
    wide = df.pivot_table(index="timestamp", columns="symbol", values="price", aggfunc="last").ffill()
    return df, wide

placeholder = st.empty()

while True:
    df, wide = load_data(window_mins, symbols)
    with placeholder.container():
        if df.empty:
            st.info("No data in the selected window yet. Keep the producer running…")
        else:
            st.subheader("Live Prices")
            st.line_chart(wide)

            latest = (df.sort_values("timestamp")
                        .groupby("symbol", as_index=False)
                        .tail(1)
                        .sort_values("symbol"))
            st.subheader("Latest Quotes")
            st.dataframe(latest, use_container_width=True)
    time.sleep(refresh_secs)
