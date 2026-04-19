# Cross-Market Analysis: Crypto, Oil & Stocks

A SQL-powered financial analytics platform that compares cryptocurrency, oil, and stock market data using Python, SQLite, and Streamlit.

## Project Overview
This project collects real-time and historical data from multiple financial markets, stores it in a relational database, and visualizes insights through an interactive Streamlit dashboard.

## Features
- Live cryptocurrency data from CoinGecko API (top 250 coins)
- 1-year historical daily prices for top 3 cryptocurrencies
- WTI Crude Oil daily prices (2020–2026)
- Stock index data: S&P 500, NASDAQ, NIFTY (2020–2025)
- 20+ SQL analytics queries
- 10 cross-market JOIN queries
- 3-page interactive Streamlit dashboard

## Tech Stack
- Python 3.11
- SQLite (via sqlite3)
- Pandas
- Streamlit
- Plotly
- yfinance
- CoinGecko API

## Project Structure
```
cross_market_analysis/
├── database.py          # SQLite schema creation
├── data_collection.py   # Data fetching from all APIs
├── queries.py           # All SQL analytics queries
├── app.py               # Streamlit dashboard
├── requirements.txt     # Python dependencies
└── market_data.db       # Auto-created SQLite database
```

## How to Run

### Step 1: Install dependencies
```bash
pip install -r requirements.txt
```

### Step 2: Collect all data
```bash
python data_collection.py
```
This will create `market_data.db` and populate all 4 tables.

### Step 3: Launch the dashboard
```bash
streamlit run app.py
```

## Dashboard Pages
1. **Market Overview** — Date filters, metric cards, daily snapshot table, price charts
2. **SQL Query Runner** — Select and run 20+ predefined SQL queries
3. **Top 3 Crypto Analysis** — Price trends and tables for top cryptocurrencies

## Domain
Financial Analytics / Business Intelligence (BI)

## Skills Demonstrated
- API Integration (CoinGecko, Yahoo Finance)
- Database Design (SQL)
- ETL Workflow
- SQL Analytics and Cross-Market Joins
- Streamlit Dashboarding
- Pandas Data Processing