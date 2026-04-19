import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date
from database import get_connection
from queries import (
    q_multi_join_daily_snapshot,
    q_bitcoin_price_trend,
    QUERY_MENU
)

# ─────────────────────────────────────────────────
# APP CONFIG
# ─────────────────────────────────────────────────
st.set_page_config(
    page_title="Cross-Market Analysis",
    page_icon="📈",
    layout="wide"
)

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
# PAGE 1: MARKET OVERVIEW (Filters + Snapshot)
# ══════════════════════════════════════════════════
if page == "📊 Market Overview":

    st.title("📊 Cross-Market Overview")
    st.caption("Crypto · Oil · Stock-Market | SQL-powered analytics")
    st.markdown("---")

    # Date filters
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=date(2024, 1, 1), min_value=date(2020, 1, 1))
    with col2:
        end_date = st.date_input("End Date", value=date(2026, 1, 1))

    start_str = str(start_date)
    end_str   = str(end_date)

    # ── Metric cards ─────────────────────────────
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

    # Display 4 metric cards
    m1, m2, m3, m4 = st.columns(4)
    btc_val   = btc_avg["val"].iloc[0]   if not btc_avg.empty   else "N/A"
    oil_val   = oil_avg["val"].iloc[0]   if not oil_avg.empty   else "N/A"
    sp_val    = sp500_avg["val"].iloc[0] if not sp500_avg.empty else "N/A"
    nifty_val = nifty_avg["val"].iloc[0] if not nifty_avg.empty else "N/A"

    m1.metric("Bitcoin Avg (₹)", f"₹{btc_val:,.2f}" if isinstance(btc_val, float) else btc_val)
    m2.metric("Oil Avg ($)", f"${oil_val:,.2f}" if isinstance(oil_val, float) else oil_val)
    m3.metric("S&P 500 Avg", f"{sp_val:,.2f}" if isinstance(sp_val, float) else sp_val)
    m4.metric("NIFTY Avg", f"{nifty_val:,.2f}" if isinstance(nifty_val, float) else nifty_val)

    st.markdown("---")

    # ── Daily Market Snapshot Table ───────────────
    st.subheader("📋 Daily Market Snapshot")
    st.caption("Bitcoin, Oil, S&P 500, and NIFTY prices joined by date")

    snapshot_df = q_multi_join_daily_snapshot(start_str, end_str)

    if snapshot_df.empty:
        st.warning("No data found for this date range. Please run data_collection.py first.")
    else:
        st.dataframe(snapshot_df, use_container_width=True, height=400)

        # ── Line chart: Bitcoin vs S&P 500 ────────────
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
            yaxis=dict(title="Bitcoin Price (₹)", titlefont=dict(color="#F7931A")),
            yaxis2=dict(title="S&P 500", titlefont=dict(color="#4472C4"), overlaying="y", side="right"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)

        # ── Oil price chart ─────────────────────────
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
                query_fn = QUERY_MENU[selected_query_name]
                result_df = query_fn()
                if result_df.empty:
                    st.warning("Query returned no results. Make sure data is loaded.")
                else:
                    st.success(f"Query executed successfully — {len(result_df)} row(s) returned")
                    st.dataframe(result_df, use_container_width=True)

                    # Offer download
                    csv = result_df.to_csv(index=False)
                    st.download_button(
                        label="⬇ Download as CSV",
                        data=csv,
                        file_name=f"{selected_query_name.lower().replace(' ', '_')}.csv",
                        mime="text/csv"
                    )
            except Exception as e:
                st.error(f"Query error: {e}")

    st.caption("These queries run directly on your local SQLite database.")


# ══════════════════════════════════════════════════
# PAGE 3: TOP 3 CRYPTO ANALYSIS
# ══════════════════════════════════════════════════
elif page == "🪙 Top 3 Crypto Analysis":

    st.title("🪙 Top 3 Crypto Analysis")
    st.caption("Daily price analysis for top cryptocurrencies")

    # Get top 3 coins from DB
    top3_df = get_top3_coins()

    if top3_df.empty:
        st.warning("No cryptocurrency data found. Please run data_collection.py first.")
    else:
        coin_options = {row["name"]: row["id"] for _, row in top3_df.iterrows()}
        selected_coin_name = st.selectbox("Select a Cryptocurrency", list(coin_options.keys()))
        selected_coin_id   = coin_options[selected_coin_name]

        # Date range
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date", value=date(2024, 1, 1),
                                       min_value=date(2020, 1, 1), key="crypto_start")
        with col2:
            end_date = st.date_input("End Date", value=date(2026, 1, 26), key="crypto_end")

        start_str = str(start_date)
        end_str   = str(end_date)

        # Fetch price data
        conn = get_connection()
        price_df = pd.read_sql_query("""
            SELECT date, price_usd FROM crypto_prices
            WHERE coin_id = ?
              AND date >= ? AND date <= ?
            ORDER BY date ASC
        """, conn, params=(selected_coin_id, start_str, end_str))
        conn.close()

        if price_df.empty:
            st.warning(f"No price data found for {selected_coin_name} in this date range.")
        else:
            # Metrics
            c1, c2, c3 = st.columns(3)
            c1.metric("Min Price (₹)",  f"₹{price_df['price_usd'].min():,.2f}")
            c2.metric("Max Price (₹)",  f"₹{price_df['price_usd'].max():,.2f}")
            c3.metric("Avg Price (₹)",  f"₹{price_df['price_usd'].mean():,.2f}")

            # Line chart
            st.subheader(f"📈 {selected_coin_name.upper()} Price Trend")
            fig = px.line(
                price_df, x="date", y="price_usd",
                title=f"{selected_coin_name} Daily Price (₹)",
                labels={"price_usd": "Price (₹)", "date": "Date"},
                color_discrete_sequence=["#F7931A"]
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

            # Data table
            st.subheader("📋 Daily Price Table")
            st.dataframe(price_df, use_container_width=True, height=350)

            # Download
            csv = price_df.to_csv(index=False)
            st.download_button(
                label="⬇ Download Price Data as CSV",
                data=csv,
                file_name=f"{selected_coin_id}_prices.csv",
                mime="text/csv"
            )