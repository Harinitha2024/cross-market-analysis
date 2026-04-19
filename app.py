import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date
import os

# ─────────────────────────────────────────────────
# APP CONFIG
# ─────────────────────────────────────────────────
st.set_page_config(
    page_title="Cross-Market Analysis",
    page_icon="📈",
    layout="wide"
)

# ─────────────────────────────────────────────────
# AUTO SETUP: Create DB + Collect Data if not exists
# ─────────────────────────────────────────────────

def is_data_ready():
    """Check if database exists and has data in it."""
    if not os.path.exists("market_data.db"):
        return False
    try:
        from database import get_connection
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM crypto_prices")
        count = cursor.fetchone()[0]
        conn.close()
        return count > 0
    except:
        return False

if not is_data_ready():
    with st.spinner("⏳ First-time setup: collecting all market data... This takes 3–5 minutes. Please wait."):
        try:
            from database import create_tables, clear_tables
            from data_collection import (
                fetch_crypto_metadata, save_crypto_metadata,
                get_top3_coin_ids, fetch_coin_historical_prices,
                save_crypto_prices, fetch_and_save_oil_prices,
                fetch_and_save_stock_prices
            )
            import time

            create_tables()
            clear_tables()

            coins = fetch_crypto_metadata()
            if coins:
                save_crypto_metadata(coins)
                top3 = get_top3_coin_ids()
                for coin_id in top3:
                    prices = fetch_coin_historical_prices(coin_id)
                    if prices:
                        save_crypto_prices(prices)
                    time.sleep(2)

            fetch_and_save_oil_prices()
            fetch_and_save_stock_prices()

            st.success("✅ Data collection complete! Loading dashboard...")
            st.rerun()

        except Exception as e:
            st.error(f"❌ Data collection failed: {e}")
            st.info("Please refresh the page to try again.")
            st.stop()

# ─────────────────────────────────────────────────
# IMPORTS (after DB is ready)
# ─────────────────────────────────────────────────
from database import get_connection
from queries import q_multi_join_daily_snapshot, QUERY_MENU

# ─────────────────────────────────────────────────
# SIDEBAR NAVIGATION
# ─────────────────────────────────────────────────
st.sidebar.title("Navigation")
st.sidebar.markdown("---")
page = st.sidebar.radio(
    "Go to",
    ["📊 Market Overview", "🔍 SQL Query Runner", "🪙 Top 3 Crypto Analysis"]
)

# ─────────────────────────────────────────────────
# HELPER: Get top 3 coin IDs from DB
# ─────────────────────────────────────────────────
def get_top3_coins():
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT id, name FROM cryptocurrencies
        ORDER BY market_cap_rank ASC LIMIT 3
    """, conn)
    conn.close()
    return df


# ══════════════════════════════════════════════════
# PAGE 1: MARKET OVERVIEW
# ══════════════════════════════════════════════════
if page == "📊 Market Overview":

    st.title("📊 Cross-Market Overview")
    st.caption("Crypto · Oil · Stock-Market | SQL-powered analytics")
    st.markdown("---")

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=date(2024, 1, 1), min_value=date(2020, 1, 1))
    with col2:
        end_date = st.date_input("End Date", value=date(2026, 1, 1))

    start_str = str(start_date)
    end_str   = str(end_date)

    conn = get_connection()

    btc_avg = pd.read_sql_query("""
        SELECT ROUND(AVG(price_usd), 2) AS val FROM crypto_prices
        WHERE coin_id='bitcoin' AND date>=? AND date<=?
    """, conn, params=(start_str, end_str))

    oil_avg = pd.read_sql_query("""
        SELECT ROUND(AVG(price_usd), 2) AS val FROM oil_prices
        WHERE date>=? AND date<=?
    """, conn, params=(start_str, end_str))

    sp500_avg = pd.read_sql_query("""
        SELECT ROUND(AVG(close), 2) AS val FROM stock_prices
        WHERE ticker='^GSPC' AND date>=? AND date<=?
    """, conn, params=(start_str, end_str))

    nifty_avg = pd.read_sql_query("""
        SELECT ROUND(AVG(close), 2) AS val FROM stock_prices
        WHERE ticker='^NSEI' AND date>=? AND date<=?
    """, conn, params=(start_str, end_str))

    conn.close()

    m1, m2, m3, m4 = st.columns(4)
    btc_val   = btc_avg["val"].iloc[0]   if not btc_avg.empty   else None
    oil_val   = oil_avg["val"].iloc[0]   if not oil_avg.empty   else None
    sp_val    = sp500_avg["val"].iloc[0] if not sp500_avg.empty else None
    nifty_val = nifty_avg["val"].iloc[0] if not nifty_avg.empty else None

    m1.metric("Bitcoin Avg (₹)", f"₹{btc_val:,.2f}"   if btc_val   else "N/A")
    m2.metric("Oil Avg ($)",     f"${oil_val:,.2f}"    if oil_val   else "N/A")
    m3.metric("S&P 500 Avg",     f"{sp_val:,.2f}"      if sp_val    else "N/A")
    m4.metric("NIFTY Avg",       f"{nifty_val:,.2f}"   if nifty_val else "N/A")

    st.markdown("---")
    st.subheader("📋 Daily Market Snapshot")
    st.caption("Bitcoin, Oil, S&P 500, and NIFTY prices joined by date")

    snapshot_df = q_multi_join_daily_snapshot(start_str, end_str)

    if snapshot_df.empty:
        st.warning("No data found for this date range.")
    else:
        st.dataframe(snapshot_df, use_container_width=True, height=400)

        st.markdown("---")
        st.subheader("📈 Bitcoin vs S&P 500 Trend")
        chart_df = snapshot_df.sort_values("date")

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=chart_df["date"], y=chart_df["bitcoin_price"],
            name="Bitcoin (₹)", yaxis="y1", line=dict(color="#F7931A")
        ))
        fig.add_trace(go.Scatter(
            x=chart_df["date"], y=chart_df["sp500"],
            name="S&P 500", yaxis="y2", line=dict(color="#4472C4")
        ))
        fig.update_layout(
            yaxis=dict(
                title="Bitcoin Price (₹)",
                title_font=dict(color="#F7931A")
            ),
            yaxis2=dict(
                title="S&P 500",
                title_font=dict(color="#4472C4"),
                overlaying="y",
                side="right"
            ),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("🛢 Oil Price Trend")
        conn2 = get_connection()
        oil_df = pd.read_sql_query("""
            SELECT date, price_usd FROM oil_prices
            WHERE date >= ? AND date <= ?
            ORDER BY date ASC
        """, conn2, params=(start_str, end_str))
        conn2.close()

        if not oil_df.empty:
            fig2 = px.line(oil_df, x="date", y="price_usd",
                           title="WTI Crude Oil Price (USD)",
                           color_discrete_sequence=["#8B4513"])
            fig2.update_layout(height=350)
            st.plotly_chart(fig2, use_container_width=True)


# ══════════════════════════════════════════════════
# PAGE 2: SQL QUERY RUNNER
# ══════════════════════════════════════════════════
elif page == "🔍 SQL Query Runner":

    st.title("🔍 SQL Query Runner")
    st.caption("Predefined analytical SQL queries")

    selected_query_name = st.selectbox("Select a Query", list(QUERY_MENU.keys()))

    if st.button("▶ Run Query"):
        with st.spinner("Running query..."):
            try:
                result_df = QUERY_MENU[selected_query_name]()
                if result_df.empty:
                    st.warning("Query returned no results.")
                else:
                    st.success(f"✅ {len(result_df)} row(s) returned")
                    st.dataframe(result_df, use_container_width=True)
                    csv = result_df.to_csv(index=False)
                    st.download_button(
                        label="⬇ Download as CSV",
                        data=csv,
                        file_name=f"{selected_query_name.lower().replace(' ', '_')}.csv",
                        mime="text/csv"
                    )
            except Exception as e:
                st.error(f"Query error: {e}")

    st.caption("These queries run directly on the SQLite database.")


# ══════════════════════════════════════════════════
# PAGE 3: TOP 3 CRYPTO ANALYSIS
# ══════════════════════════════════════════════════
elif page == "🪙 Top 3 Crypto Analysis":

    st.title("🪙 Top 3 Crypto Analysis")
    st.caption("Daily price analysis for top cryptocurrencies")

    top3_df = get_top3_coins()

    if top3_df.empty:
        st.warning("No cryptocurrency data found.")
    else:
        coin_options = {row["name"]: row["id"] for _, row in top3_df.iterrows()}
        selected_coin_name = st.selectbox("Select a Cryptocurrency", list(coin_options.keys()))
        selected_coin_id   = coin_options[selected_coin_name]

        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date", value=date(2024, 1, 1),
                                       min_value=date(2020, 1, 1), key="crypto_start")
        with col2:
            end_date = st.date_input("End Date", value=date(2026, 1, 26), key="crypto_end")

        start_str = str(start_date)
        end_str   = str(end_date)

        conn = get_connection()
        price_df = pd.read_sql_query("""
            SELECT date, price_usd FROM crypto_prices
            WHERE coin_id = ? AND date >= ? AND date <= ?
            ORDER BY date ASC
        """, conn, params=(selected_coin_id, start_str, end_str))
        conn.close()

        if price_df.empty:
            st.warning(f"No price data found for {selected_coin_name} in this range.")
        else:
            c1, c2, c3 = st.columns(3)
            c1.metric("Min Price (₹)", f"₹{price_df['price_usd'].min():,.2f}")
            c2.metric("Max Price (₹)", f"₹{price_df['price_usd'].max():,.2f}")
            c3.metric("Avg Price (₹)", f"₹{price_df['price_usd'].mean():,.2f}")

            st.subheader(f"📈 {selected_coin_name.upper()} Price Trend")
            fig = px.line(
                price_df, x="date", y="price_usd",
                title=f"{selected_coin_name} Daily Price (₹)",
                labels={"price_usd": "Price (₹)", "date": "Date"},
                color_discrete_sequence=["#F7931A"]
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

            st.subheader("📋 Daily Price Table")
            st.dataframe(price_df, use_container_width=True, height=350)

            csv = price_df.to_csv(index=False)
            st.download_button(
                label="⬇ Download Price Data as CSV",
                data=csv,
                file_name=f"{selected_coin_id}_prices.csv",
                mime="text/csv"
            )