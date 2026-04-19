import sqlite3

DB_NAME = "market_data.db"

def get_connection():
    """Returns a connection to the SQLite database."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row  # lets us access columns by name
    return conn

def create_tables():
    """Creates all 4 tables if they don't already exist."""
    conn = get_connection()
    cursor = conn.cursor()

    # Table 1: Cryptocurrency metadata (one row per coin)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cryptocurrencies (
            id                TEXT PRIMARY KEY,
            symbol            TEXT,
            name              TEXT,
            current_price     REAL,
            market_cap        INTEGER,
            market_cap_rank   INTEGER,
            total_volume      INTEGER,
            circulating_supply REAL,
            total_supply      REAL,
            ath               REAL,
            atl               REAL,
            date              TEXT
        )
    """)

    # Table 2: Daily crypto prices (one row per coin per day)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS crypto_prices (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            coin_id TEXT,
            date    TEXT,
            price_usd REAL,
            FOREIGN KEY (coin_id) REFERENCES cryptocurrencies(id)
        )
    """)

    # Table 3: Daily oil prices
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS oil_prices (
            date      TEXT PRIMARY KEY,
            price_usd REAL
        )
    """)

    # Table 4: Stock index daily prices
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_prices (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            date    TEXT,
            open    REAL,
            high    REAL,
            low     REAL,
            close   REAL,
            volume  INTEGER,
            ticker  TEXT
        )
    """)

    conn.commit()
    conn.close()
    print("✅ All tables created successfully!")

def clear_tables():
    """Clears all data from tables (useful when re-running data collection)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM crypto_prices")
    cursor.execute("DELETE FROM cryptocurrencies")
    cursor.execute("DELETE FROM oil_prices")
    cursor.execute("DELETE FROM stock_prices")
    conn.commit()
    conn.close()
    print("🗑️  All tables cleared.")

if __name__ == "__main__":
    create_tables()