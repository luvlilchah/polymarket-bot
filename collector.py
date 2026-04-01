from __future__ import annotations

import json
import time
from pathlib import Path

import requests

from db import get_conn, init_db

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"

GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"


def load_config() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def safe_get_json(url: str, params: dict | None = None):
    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Request failed: {url} params={params} err={e}")
        return None


def upsert_wallet(conn, address: str, source: str = "leaderboard", name: str | None = None, pseudonym: str | None = None):
    conn.execute(
        """
        INSERT INTO wallets(address, name, pseudonym, source)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(address) DO UPDATE SET
            name = COALESCE(excluded.name, wallets.name),
            pseudonym = COALESCE(excluded.pseudonym, wallets.pseudonym),
            source = COALESCE(excluded.source, wallets.source)
        """,
        (address, name, pseudonym, source),
    )


def fetch_leaderboard_wallets(conn):
    data = safe_get_json(f"{DATA_API}/v1/leaderboard")
    if not data:
        return 0

    if isinstance(data, dict):
        rows = data.get("users") or data.get("data") or data.get("leaderboard") or []
    elif isinstance(data, list):
        rows = data
    else:
        rows = []

    added = 0
    for row in rows[:100]:
        address = row.get("proxyWallet") or row.get("walletAddress") or row.get("address")
        if not address:
            continue
        upsert_wallet(
            conn,
            address=address,
            source="leaderboard",
            name=row.get("name"),
            pseudonym=row.get("pseudonym"),
        )
        added += 1
    return added


def fetch_market_holders(conn, condition_id: str):
    data = safe_get_json(f"{DATA_API}/holders", params={"market": condition_id})
    if not data:
        return 0

    if isinstance(data, dict):
        rows = data.get("holders") or data.get("data") or []
    elif isinstance(data, list):
        rows = data
    else:
        rows = []

    added = 0
    for row in rows[:100]:
        address = row.get("proxyWallet") or row.get("walletAddress") or row.get("address")
        if not address:
            continue
        upsert_wallet(
            conn,
            address=address,
            source="holders",
            name=row.get("name"),
            pseudonym=row.get("pseudonym"),
        )
        added += 1
    return added


def fetch_recent_trades(conn, wallet: str, limit: int = 20):
    data = safe_get_json(f"{DATA_API}/trades", params={"user": wallet})
    if not data:
        return []

    if isinstance(data, dict):
        rows = data.get("history") or data.get("data") or data.get("trades") or []
    elif isinstance(data, list):
        rows = data
    else:
        rows = []

    out = []
    for row in rows[:limit]:
        tx_hash = row.get("transactionHash") or row.get("txHash") or row.get("id")
        if not tx_hash:
            continue

        side = row.get("side") or row.get("type") or "?"
        outcome = row.get("outcome") or row.get("outcomeIndex") or "?"
        size = float(row.get("size", 0) or row.get("shares", 0) or 0)
        price = float(row.get("price", 0) or 0)
        title = row.get("title") or row.get("marketTitle") or row.get("question") or "Unknown market"
        market_slug = row.get("slug") or row.get("marketSlug") or ""
        timestamp = int(row.get("timestamp", 0) or row.get("time", 0) or 0)

        conn.execute(
            """
            INSERT OR IGNORE INTO trades
            (tx_hash, wallet, side, outcome, size, price, title, market_slug, timestamp, is_elite)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            """,
            (tx_hash, wallet, str(side), str(outcome), size, price, title, market_slug, timestamp),
        )

        out.append(
            {
                "tx_hash": tx_hash,
                "wallet": wallet,
                "side": str(side),
                "outcome": str(outcome),
                "size": size,
                "price": price,
                "title": title,
                "market_slug": market_slug,
                "timestamp": timestamp,
            }
        )

    return out


def fetch_closed_positions(wallet: str, limit: int = 20):
    data = safe_get_json(f"{DATA_API}/closed-positions", params={"user": wallet})
    if not data:
        return []

    if isinstance(data, dict):
        rows = data.get("data") or data.get("positions") or []
    elif isinstance(data, list):
        rows = data
    else:
        rows = []

    return rows[:limit]


def score_wallet(conn, wallet: str):
    positions = fetch_closed_positions(wallet, limit=20)
    recent_trades = conn.execute(
        """
        SELECT size, price, timestamp
        FROM trades
        WHERE wallet = ?
        ORDER BY timestamp DESC
        LIMIT 20
        """,
        (wallet,),
    ).fetchall()

    recent_pnl = 0.0
    realized_pnl = 0.0
    wins = 0
    losses = 0
    streak = 0
    avg_size = 0.0
    last_trade_ts = None

    for p in positions:
        pnl = float(p.get("realizedPnl", 0) or 0)
        realized_pnl += pnl
        recent_pnl += pnl
        if pnl > 0:
            wins += 1
            if losses == 0:
                streak += 1
        elif pnl < 0:
            losses += 1

    if recent_trades:
        avg_size = sum(float(r["size"] or 0) for r in recent_trades) / max(len(recent_trades), 1)
        last_trade_ts = max(int(r["timestamp"] or 0) for r in recent_trades)

    recent_trade_count = len(recent_trades)
    decisions = wins + losses
    recent_win_rate = wins / decisions if decisions else 0.0

    goat_score = (
        (streak * 120.0)
        + (recent_pnl * 0.02)
        + (avg_size * 0.15)
        + (recent_trade_count * 10.0)
        + (recent_win_rate * 200.0)
    )

    conn.execute(
        """
        UPDATE wallets
        SET score = ?,
            current_streak = ?,
            recent_pnl = ?,
            avg_size = ?,
            recent_trade_count = ?,
            recent_win_rate = ?,
            realized_pnl = ?,
            last_trade_ts = ?
        WHERE address = ?
        """,
        (
            goat_score,
            streak,
            recent_pnl,
            avg_size,
            recent_trade_count,
            recent_win_rate,
            realized_pnl,
            last_trade_ts,
            wallet,
        ),
    )


def mark_elite_wallet_trades(conn):
    elite_wallets = conn.execute(
        """
        SELECT address
        FROM wallets
        WHERE score > 500
        ORDER BY score DESC
        LIMIT 20
        """
    ).fetchall()

    elite_set = {row["address"] for row in elite_wallets}
    for wallet in elite_set:
        conn.execute("UPDATE trades SET is_elite = 1 WHERE wallet = ?", (wallet,))


def collector_loop():
    config = load_config()
    condition_id = config["market"]["condition_id"]
    poll_seconds = int(config.get("poll_seconds", 15))

    init_db()

    while True:
        print("Collector tick...")
        conn = get_conn()

        lb_added = fetch_leaderboard_wallets(conn)
        holders_added = fetch_market_holders(conn, condition_id)

        wallets = conn.execute(
            """
            SELECT address
            FROM wallets
            ORDER BY source DESC, address ASC
            LIMIT 50
            """
        ).fetchall()

        for row in wallets:
            wallet = row["address"]
            fetch_recent_trades(conn, wallet, limit=20)
            score_wallet(conn, wallet)

        mark_elite_wallet_trades(conn)

        conn.execute(
            """
            INSERT INTO meta(key, value)
            VALUES('last_run_ms', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (str(int(time.time() * 1000)),),
        )

        conn.commit()
        conn.close()

        print(f"Done. leaderboard={lb_added} holders={holders_added} wallets_scored={len(wallets)}")
        time.sleep(poll_seconds)


if __name__ == "__main__":
    collector_loop()
