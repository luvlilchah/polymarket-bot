import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "polymarket_dashboard.db"


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    conn = get_conn()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS wallets (
                address            TEXT PRIMARY KEY,
                name               TEXT,
                pseudonym          TEXT,
                source             TEXT,
                score              REAL    DEFAULT 0,
                current_streak     INTEGER DEFAULT 0,
                recent_pnl         REAL    DEFAULT 0,
                avg_size           REAL    DEFAULT 0,
                recent_trade_count INTEGER DEFAULT 0,
                recent_win_rate    REAL    DEFAULT 0,
                realized_pnl       REAL    DEFAULT 0,
                last_trade_ts      INTEGER,
                last_pnl_check_ts  INTEGER DEFAULT 0,
                is_goat            INTEGER DEFAULT 0,
                goat_reason        TEXT
            );

            CREATE TABLE IF NOT EXISTS trades (
                tx_hash      TEXT PRIMARY KEY,
                wallet       TEXT,
                side         TEXT,
                outcome      TEXT,
                size         REAL    DEFAULT 0,
                price        REAL    DEFAULT 0,
                title        TEXT,
                market_slug  TEXT,
                timestamp    INTEGER,
                resolved_pnl REAL    DEFAULT 0,
                is_win       INTEGER DEFAULT -1,
                is_elite     INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_trades_wallet    ON trades(wallet);
            CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
            CREATE INDEX IF NOT EXISTS idx_trades_is_elite  ON trades(is_elite);
            CREATE INDEX IF NOT EXISTS idx_wallets_is_goat  ON wallets(is_goat);
            CREATE INDEX IF NOT EXISTS idx_wallets_score    ON wallets(score DESC);
        """)

        _add_column_if_missing(conn, "wallets", "is_goat",           "INTEGER DEFAULT 0")
        _add_column_if_missing(conn, "wallets", "goat_reason",       "TEXT")
        _add_column_if_missing(conn, "wallets", "last_pnl_check_ts", "INTEGER DEFAULT 0")
        _add_column_if_missing(conn, "trades",  "resolved_pnl",      "REAL    DEFAULT 0")
        _add_column_if_missing(conn, "trades",  "is_win",            "INTEGER DEFAULT -1")

        conn.commit()
    finally:
        conn.close()


def _add_column_if_missing(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        print(f"Migration: added {table}.{column}")
