import requests
import pandas as pd
import yfinance as yf
import sqlite3
import time
from datetime import datetime, timedelta
from database import get_connection, create_tables, clear_tables

# ─────────────────────────────────────────────
# SECTION 1: CRYPTOCURRENCY DATA (CoinGecko API)
# ─────────────────────────────────────────────

def fetch_crypto_metadata():
    """
    Fetches metadata for top 250 cryptocurrencies from CoinGecko.
    Returns a list of coin dictionaries.
    """
    print("📥 Fetching crypto metadata from CoinGecko...")
    url = (
        "https://api.coingecko.com/api/v3/coins/markets"
        "?vs_currency=inr&per_page=250&order=market_cap_desc&page=1&sparkline=false"
    )
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        print(f"   ✅ Fetched {len(data)} coins.")
        return data
    except Exception as e:
        print(f"   ❌ Error fetching crypto metadata: {e}")
        return []


def save_crypto_metadata(coins):
    """
    Saves cryptocurrency metadata into the 'cryptocurrencies' table.
    Picks only the columns we need.
    """
    conn = get_connection()
    cursor = conn.cursor()
    inserted = 0
    for coin in coins:
        try:
            # Extract just the date part from last_updated timestamp
            last_updated = coin.get("last_updated", "")
            date_only = last_updated[:10] if last_updated else ""

            cursor.execute("""
                INSERT OR REPLACE INTO cryptocurrencies
                (id, symbol, name, current_price, market_cap, market_cap_rank,
                 total_volume, circulating_supply, total_supply, ath, atl, date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                coin.get("id"),
                coin.get("symbol"),
                coin.get("name"),
                coin.get("current_price"),
                coin.get("market_cap"),
                coin.get("market_cap_rank"),
                coin.get("total_volume"),
                coin.get("circulating_supply"),
                coin.get("total_supply"),
                coin.get("ath"),
                coin.get("atl"),
                date_only
            ))
            inserted += 1
        except Exception as e:
            print(f"   ⚠️  Skipping coin {coin.get('id')}: {e}")
    conn.commit()
    conn.close()
    print(f"   ✅ Saved {inserted} coins to database.")


def get_top3_coin_ids():
    """
    Returns the IDs of the top 3 coins by market_cap_rank from the database.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id FROM cryptocurrencies
        ORDER BY market_cap_rank ASC
        LIMIT 3
    """)
    rows = cursor.fetchall()
    conn.close()
    ids = [row["id"] for row in rows]
    print(f"   Top 3 coins: {ids}")
    return ids


def fetch_coin_historical_prices(coin_id):
    """
    Fetches 365 days of daily price data for a specific coin from CoinGecko.
    Returns a list of (date, price) tuples.
    """
    print(f"   📈 Fetching 1-year price history for: {coin_id}")
    url = (
        f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
        f"?vs_currency=inr&days=365"
    )
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        prices = data.get("prices", [])
        # Each price entry is [timestamp_ms, price]
        result = []
        for entry in prices:
            timestamp_ms = entry[0]
            price = entry[1]
            date = datetime.utcfromtimestamp(timestamp_ms / 1000).strftime("%Y-%m-%d")
            result.append((coin_id, date, price))
        print(f"      ✅ Got {len(result)} daily prices for {coin_id}")
        return result
    except Exception as e:
        print(f"      ❌ Error fetching prices for {coin_id}: {e}")
        return []


def save_crypto_prices(prices):
    """
    Saves daily crypto prices to the 'crypto_prices' table.
    prices is a list of (coin_id, date, price_usd) tuples.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.executemany("""
        INSERT INTO crypto_prices (coin_id, date, price_usd)
        VALUES (?, ?, ?)
    """, prices)
    conn.commit()
    conn.close()
    print(f"   ✅ Saved {len(prices)} price records.")


# ─────────────────────────────────────────────
# SECTION 2: OIL PRICES (GitHub CSV)
# ─────────────────────────────────────────────

def fetch_and_save_oil_prices():
    """
    Downloads WTI crude oil daily prices from GitHub and filters for 2020-2026.
    Saves to the 'oil_prices' table.
    """
    print("\n📥 Fetching oil prices from GitHub...")
    url = "https://raw.githubusercontent.com/datasets/oil-prices/main/data/wti-daily.csv"
    try:
        df = pd.read_csv(url)
        # The CSV has columns: Date, Price
        df.columns = df.columns.str.strip()
        df.rename(columns={"Date": "date", "Price": "price_usd"}, inplace=True)
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

        # Filter for Jan 2020 to Jan 2026
        df = df[(df["date"] >= "2020-01-01") & (df["date"] <= "2026-01-31")]
        df.dropna(subset=["price_usd"], inplace=True)

        # Save to database
        conn = get_connection()
        cursor = conn.cursor()
        records = list(df[["date", "price_usd"]].itertuples(index=False, name=None))
        cursor.executemany("""
            INSERT OR REPLACE INTO oil_prices (date, price_usd)
            VALUES (?, ?)
        """, records)
        conn.commit()
        conn.close()
        print(f"   ✅ Saved {len(records)} oil price records (2020–2026).")
    except Exception as e:
        print(f"   ❌ Error fetching oil prices: {e}")


# ─────────────────────────────────────────────
# SECTION 3: STOCK PRICES (Yahoo Finance)
# ─────────────────────────────────────────────

def fetch_and_save_stock_prices():
    """
    Downloads daily stock data for S&P500, NASDAQ, and NIFTY50
    using the yfinance library. Saves to 'stock_prices' table.
    """
    tickers = {
        "^GSPC": "S&P 500",
        "^IXIC": "NASDAQ",
        "^NSEI": "NIFTY"
    }
    start_date = "2020-01-01"
    end_date   = "2025-09-30"

    conn = get_connection()
    cursor = conn.cursor()

    for ticker, name in tickers.items():
        print(f"\n📥 Fetching stock data for {name} ({ticker})...")
        try:
            df = yf.download(ticker, start=start_date, end=end_date, auto_adjust=True, progress=False)

            if df.empty:
                print(f"   ⚠️  No data returned for {ticker}")
                continue

            # Flatten MultiIndex columns if present
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            df.reset_index(inplace=True)
            df.rename(columns={"Date": "date", "Open": "open", "High": "high",
                                "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            df["ticker"] = ticker
            df.dropna(subset=["close"], inplace=True)

            records = []
            for _, row in df.iterrows():
                records.append((
                    str(row["date"]),
                    float(row.get("open", 0) or 0),
                    float(row.get("high", 0) or 0),
                    float(row.get("low", 0) or 0),
                    float(row.get("close", 0) or 0),
                    int(row.get("volume", 0) or 0),
                    str(row["ticker"])
                ))

            cursor.executemany("""
                INSERT INTO stock_prices (date, open, high, low, close, volume, ticker)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, records)
            conn.commit()
            print(f"   ✅ Saved {len(records)} records for {name}.")

        except Exception as e:
            print(f"   ❌ Error fetching {ticker}: {e}")

    conn.close()


# ─────────────────────────────────────────────
# MAIN: Run everything in order
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 55)
    print("  CROSS-MARKET DATA COLLECTION STARTING")
    print("=" * 55)

    # Step 1: Make sure tables exist, clear old data
    create_tables()
    clear_tables()

    # Step 2: Crypto metadata
    coins = fetch_crypto_metadata()
    if coins:
        save_crypto_metadata(coins)

        # Step 3: Historical prices for top 3 coins
        print("\n📥 Fetching historical prices for top 3 coins...")
        top3 = get_top3_coin_ids()
        for coin_id in top3:
            prices = fetch_coin_historical_prices(coin_id)
            if prices:
                save_crypto_prices(prices)
            time.sleep(2)  # Pause to avoid hitting API rate limit
    else:
        print("⚠️  Skipping historical prices — no coin metadata fetched.")

    # Step 4: Oil prices
    fetch_and_save_oil_prices()

    # Step 5: Stock prices
    fetch_and_save_stock_prices()

    print("\n" + "=" * 55)
    print("  DATA COLLECTION COMPLETE!")
    print("  Database file: market_data.db")
    print("=" * 55)