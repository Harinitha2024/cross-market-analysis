import pandas as pd
from database import get_connection

# ─────────────────────────────────────────
# HELPER: Run any SQL and return a DataFrame
# ─────────────────────────────────────────

def run_query(sql, params=()):
    """Runs a SQL query and returns a Pandas DataFrame."""
    conn = get_connection()
    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    return df


# ══════════════════════════════════════════
# GROUP 1: CRYPTOCURRENCIES TABLE QUERIES
# ══════════════════════════════════════════

def q_top3_by_market_cap():
    """Find the top 3 cryptocurrencies by market cap."""
    return run_query("""
        SELECT name, symbol, market_cap, market_cap_rank
        FROM cryptocurrencies
        ORDER BY market_cap_rank ASC
        LIMIT 3
    """)

def q_circulating_over_90pct():
    """List all coins where circulating supply exceeds 90% of total supply."""
    return run_query("""
        SELECT name, symbol,
               circulating_supply,
               total_supply,
               ROUND(circulating_supply * 100.0 / total_supply, 2) AS pct_circulating
        FROM cryptocurrencies
        WHERE total_supply > 0
          AND circulating_supply >= 0.9 * total_supply
        ORDER BY pct_circulating DESC
    """)

def q_coins_near_ath():
    """Get coins that are within 10% of their all-time-high (ATH)."""
    return run_query("""
        SELECT name, symbol, current_price, ath,
               ROUND((ath - current_price) * 100.0 / ath, 2) AS pct_below_ath
        FROM cryptocurrencies
        WHERE ath > 0
          AND current_price >= 0.9 * ath
        ORDER BY pct_below_ath ASC
    """)

def q_avg_rank_high_volume():
    """Find the average market cap rank of coins with 24h volume above 1 Billion."""
    return run_query("""
        SELECT ROUND(AVG(market_cap_rank), 2) AS avg_market_cap_rank,
               COUNT(*) AS coin_count
        FROM cryptocurrencies
        WHERE total_volume > 1000000000
    """)

def q_most_recently_updated():
    """Get the most recently updated coin."""
    return run_query("""
        SELECT name, symbol, date AS last_updated
        FROM cryptocurrencies
        ORDER BY date DESC
        LIMIT 1
    """)


# ══════════════════════════════════════════
# GROUP 2: CRYPTO_PRICES TABLE QUERIES
# ══════════════════════════════════════════

def q_bitcoin_highest_price():
    """Find the highest daily price of Bitcoin in the last 365 days."""
    return run_query("""
        SELECT coin_id, MAX(price_usd) AS highest_price, date
        FROM crypto_prices
        WHERE coin_id = 'bitcoin'
        ORDER BY highest_price DESC
        LIMIT 1
    """)

def q_ethereum_avg_price():
    """Calculate the average daily price of Ethereum in the past 1 year."""
    return run_query("""
        SELECT coin_id, ROUND(AVG(price_usd), 2) AS avg_price_inr
        FROM crypto_prices
        WHERE coin_id = 'ethereum'
    """)

def q_bitcoin_price_trend(year=2025, month=1):
    """Show the daily price trend of Bitcoin for a given month and year."""
    month_str = f"{year}-{month:02d}"
    return run_query("""
        SELECT date, price_usd
        FROM crypto_prices
        WHERE coin_id = 'bitcoin'
          AND date LIKE ?
        ORDER BY date ASC
    """, (f"{month_str}%",))

def q_coin_highest_avg_price():
    """Find the coin with the highest average price over 1 year."""
    return run_query("""
        SELECT coin_id, ROUND(AVG(price_usd), 2) AS avg_price_inr
        FROM crypto_prices
        GROUP BY coin_id
        ORDER BY avg_price_inr DESC
        LIMIT 1
    """)

def q_bitcoin_pct_change():
    """Get % change in Bitcoin's price between Sep 2024 and Sep 2025."""
    return run_query("""
        SELECT
            (SELECT AVG(price_usd) FROM crypto_prices
             WHERE coin_id='bitcoin' AND date LIKE '2025-09%') AS price_sep_2025,
            (SELECT AVG(price_usd) FROM crypto_prices
             WHERE coin_id='bitcoin' AND date LIKE '2024-09%') AS price_sep_2024,
            ROUND(
                ((SELECT AVG(price_usd) FROM crypto_prices WHERE coin_id='bitcoin' AND date LIKE '2025-09%') -
                 (SELECT AVG(price_usd) FROM crypto_prices WHERE coin_id='bitcoin' AND date LIKE '2024-09%'))
                * 100.0 /
                (SELECT AVG(price_usd) FROM crypto_prices WHERE coin_id='bitcoin' AND date LIKE '2024-09%'),
            2) AS pct_change
    """)


# ══════════════════════════════════════════
# GROUP 3: OIL_PRICES TABLE QUERIES
# ══════════════════════════════════════════

def q_oil_highest_last5years():
    """Find the highest oil price in the last 5 years."""
    return run_query("""
        SELECT date, MAX(price_usd) AS highest_price
        FROM oil_prices
        WHERE date >= '2020-01-01'
    """)

def q_oil_avg_per_year():
    """Get the average oil price per year."""
    return run_query("""
        SELECT SUBSTR(date, 1, 4) AS year,
               ROUND(AVG(price_usd), 2) AS avg_price
        FROM oil_prices
        GROUP BY year
        ORDER BY year ASC
    """)

def q_oil_covid_crash():
    """Show oil prices during COVID crash (March–April 2020)."""
    return run_query("""
        SELECT date, price_usd
        FROM oil_prices
        WHERE date >= '2020-03-01' AND date <= '2020-04-30'
        ORDER BY date ASC
    """)

def q_oil_lowest_10years():
    """Find the lowest price of oil in the last 10 years."""
    return run_query("""
        SELECT date, MIN(price_usd) AS lowest_price
        FROM oil_prices
        WHERE date >= '2015-01-01'
    """)

def q_oil_volatility_per_year():
    """Calculate the volatility of oil prices (max-min difference per year)."""
    return run_query("""
        SELECT SUBSTR(date, 1, 4) AS year,
               ROUND(MAX(price_usd) - MIN(price_usd), 2) AS volatility
        FROM oil_prices
        GROUP BY year
        ORDER BY year ASC
    """)


# ══════════════════════════════════════════
# GROUP 4: STOCK_PRICES TABLE QUERIES
# ══════════════════════════════════════════

def q_all_stocks_for_ticker(ticker="^GSPC"):
    """Get all stock prices for a given ticker."""
    return run_query("""
        SELECT date, open, high, low, close, volume
        FROM stock_prices
        WHERE ticker = ?
        ORDER BY date ASC
    """, (ticker,))

def q_nasdaq_highest_close():
    """Find the highest closing price for NASDAQ."""
    return run_query("""
        SELECT date, MAX(close) AS highest_close
        FROM stock_prices
        WHERE ticker = '^IXIC'
    """)

def q_sp500_top5_high_low():
    """List top 5 days with highest price difference (high - low) for S&P 500."""
    return run_query("""
        SELECT date, high, low,
               ROUND(high - low, 2) AS price_range
        FROM stock_prices
        WHERE ticker = '^GSPC'
        ORDER BY price_range DESC
        LIMIT 5
    """)

def q_monthly_avg_close():
    """Get monthly average closing price for each ticker."""
    return run_query("""
        SELECT ticker,
               SUBSTR(date, 1, 7) AS month,
               ROUND(AVG(close), 2) AS avg_close
        FROM stock_prices
        GROUP BY ticker, month
        ORDER BY ticker, month ASC
    """)

def q_nsei_avg_volume_2024():
    """Get average trading volume of NSEI in 2024."""
    return run_query("""
        SELECT ROUND(AVG(volume), 0) AS avg_volume_2024
        FROM stock_prices
        WHERE ticker = '^NSEI'
          AND date LIKE '2024%'
    """)


# ══════════════════════════════════════════
# GROUP 5: JOIN / CROSS-MARKET QUERIES
# ══════════════════════════════════════════

def q_bitcoin_vs_oil_2025():
    """Compare Bitcoin vs Oil average price in 2025."""
    return run_query("""
        SELECT
            ROUND(AVG(cp.price_usd), 2) AS avg_bitcoin_price_inr,
            ROUND(AVG(op.price_usd), 2) AS avg_oil_price_usd
        FROM crypto_prices cp
        JOIN oil_prices op ON cp.date = op.date
        WHERE cp.coin_id = 'bitcoin'
          AND cp.date LIKE '2025%'
    """)

def q_bitcoin_vs_sp500():
    """Check if Bitcoin moves with S&P 500 — daily prices side by side."""
    return run_query("""
        SELECT cp.date,
               ROUND(cp.price_usd, 2)  AS bitcoin_price,
               ROUND(sp.close, 2)       AS sp500_close
        FROM crypto_prices cp
        JOIN stock_prices sp ON cp.date = sp.date
        WHERE cp.coin_id = 'bitcoin'
          AND sp.ticker = '^GSPC'
        ORDER BY cp.date ASC
    """)

def q_ethereum_vs_nasdaq_2025():
    """Compare Ethereum and NASDAQ daily prices for 2025."""
    return run_query("""
        SELECT cp.date,
               ROUND(cp.price_usd, 2) AS ethereum_price,
               ROUND(sp.close, 2)      AS nasdaq_close
        FROM crypto_prices cp
        JOIN stock_prices sp ON cp.date = sp.date
        WHERE cp.coin_id = 'ethereum'
          AND sp.ticker = '^IXIC'
          AND cp.date LIKE '2025%'
        ORDER BY cp.date ASC
    """)

def q_oil_spikes_vs_bitcoin():
    """Find days when oil price spiked (top 10%) and compare with Bitcoin price."""
    return run_query("""
        SELECT op.date,
               ROUND(op.price_usd, 2) AS oil_price,
               ROUND(cp.price_usd, 2) AS bitcoin_price
        FROM oil_prices op
        JOIN crypto_prices cp ON op.date = cp.date
        WHERE cp.coin_id = 'bitcoin'
          AND op.price_usd > (SELECT AVG(price_usd) * 1.1 FROM oil_prices)
        ORDER BY op.price_usd DESC
        LIMIT 20
    """)

def q_top3_vs_nifty():
    """Compare top 3 coins daily price trend vs Nifty (^NSEI)."""
    return run_query("""
        SELECT cp.date, cp.coin_id,
               ROUND(cp.price_usd, 2) AS crypto_price,
               ROUND(sp.close, 2)      AS nifty_close
        FROM crypto_prices cp
        JOIN stock_prices sp ON cp.date = sp.date
        WHERE sp.ticker = '^NSEI'
        ORDER BY cp.date ASC, cp.coin_id ASC
    """)

def q_sp500_vs_oil():
    """Compare stock prices (^GSPC) with crude oil prices on the same dates."""
    return run_query("""
        SELECT sp.date,
               ROUND(sp.close, 2)      AS sp500_close,
               ROUND(op.price_usd, 2)  AS oil_price
        FROM stock_prices sp
        JOIN oil_prices op ON sp.date = op.date
        WHERE sp.ticker = '^GSPC'
        ORDER BY sp.date ASC
    """)

def q_multi_join_daily_snapshot(start_date="2024-01-01", end_date="2026-01-01"):
    """Multi-join: stock prices, oil prices, and Bitcoin prices for daily comparison."""
    return run_query("""
        SELECT
            cp.date,
            ROUND(cp.price_usd, 2)       AS bitcoin_price,
            ROUND(op.price_usd, 2)        AS oil_price,
            ROUND(sp_gspc.close, 2)       AS sp500,
            ROUND(sp_nsei.close, 2)       AS nifty
        FROM crypto_prices cp
        JOIN oil_prices op        ON cp.date = op.date
        JOIN stock_prices sp_gspc ON cp.date = sp_gspc.date AND sp_gspc.ticker = '^GSPC'
        JOIN stock_prices sp_nsei ON cp.date = sp_nsei.date AND sp_nsei.ticker = '^NSEI'
        WHERE cp.coin_id = 'bitcoin'
          AND cp.date >= ?
          AND cp.date <= ?
        ORDER BY cp.date DESC
    """, (start_date, end_date))


# ══════════════════════════════════════════
# PREDEFINED QUERY MENU (for Streamlit Page 2)
# ══════════════════════════════════════════

QUERY_MENU = {
    "Top 3 Cryptocurrencies by Market Cap":        q_top3_by_market_cap,
    "Coins with >90% Circulating Supply":          q_circulating_over_90pct,
    "Coins Within 10% of All-Time High":           q_coins_near_ath,
    "Avg Market Cap Rank (Volume > $1B)":          q_avg_rank_high_volume,
    "Most Recently Updated Coin":                  q_most_recently_updated,
    "Bitcoin – Highest Daily Price (365 days)":    q_bitcoin_highest_price,
    "Ethereum – Average Price (1 year)":           q_ethereum_avg_price,
    "Coin with Highest Average Price":             q_coin_highest_avg_price,
    "Bitcoin % Change Sep 2024 vs Sep 2025":       q_bitcoin_pct_change,
    "Oil – Highest Price (Last 5 Years)":          q_oil_highest_last5years,
    "Oil – Average Price Per Year":                q_oil_avg_per_year,
    "Oil Prices During COVID Crash (Mar–Apr 2020)":q_oil_covid_crash,
    "Oil – Lowest Price (Last 10 Years)":          q_oil_lowest_10years,
    "Oil – Volatility Per Year (Max–Min)":         q_oil_volatility_per_year,
    "NASDAQ – Highest Closing Price":              q_nasdaq_highest_close,
    "S&P 500 – Top 5 Most Volatile Days":         q_sp500_top5_high_low,
    "Monthly Average Close by Ticker":             q_monthly_avg_close,
    "NSEI Average Volume in 2024":                 q_nsei_avg_volume_2024,
    "Bitcoin vs Oil Average (2025)":               q_bitcoin_vs_oil_2025,
    "Bitcoin vs S&P 500 (Daily)":                  q_bitcoin_vs_sp500,
    "Ethereum vs NASDAQ (2025)":                   q_ethereum_vs_nasdaq_2025,
    "Oil Spike Days vs Bitcoin Price":             q_oil_spikes_vs_bitcoin,
    "Top 3 Crypto vs Nifty (Daily)":              q_top3_vs_nifty,
    "S&P 500 vs Oil Prices (Daily)":              q_sp500_vs_oil,
}