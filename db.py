import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "polymarket_dashboard.db"


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()

    conn.execute("""
        CREATE TABLE IF NOT EXISTS wallets (
            address TEXT PRIMARY KEY,
            name TEXT,
            pseudonym TEXT,
            source TEXT,
            score REAL DEFAULT 0,
            current_streak INTEGER DEFAULT 0,
            recent_pnl REAL DEFAULT 0,
            avg_size REAL DEFAULT 0,
            recent_trade_count INTEGER DEFAULT 0,
            recent_win_rate REAL DEFAULT 0,
            realized_pnl REAL DEFAULT 0,
            last_trade_ts INTEGER
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            tx_hash TEXT PRIMARY KEY,
            wallet TEXT,
            side TEXT,
            outcome TEXT,
            size REAL DEFAULT 0,
            price REAL DEFAULT 0,
            title TEXT,
            market_slug TEXT,
            timestamp INTEGER,
            is_elite INTEGER DEFAULT 0
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    conn.commit()
    conn.close()
