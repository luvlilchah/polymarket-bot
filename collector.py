from __future__ import annotations

import json
import time
from pathlib import Path

import requests

from db import get_conn, init_db

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"

DATA_API = "https://data-api.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"

MAX_AGE_MS = 15 * 60 * 1000
MIN_AVG_SIZE = 10.0
MIN_RECENT_TRADES = 2
ROLLING_MARKETS = 18


def load_config() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_config(cfg: dict):
    with CONFIG_PATH.open("w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)


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


def looks_like_btc_5m(question: str) -> bool:
    q = (question or "").lower()
    return "bitcoin up or down" in q or "btc updown 5m" in q or ("bitcoin" in q and "5" in q and "minute" in q)


def discover_btc_5m_markets():
    data = safe_get_json(
        f"{GAMMA_API}/markets",
        params={"active": "true", "closed": "false", "limit": 200}
    )
    if not data or not isinstance(data, list):
        return None, []

    btc_markets = []
    for row in data:
        question = row.get("question") or ""
        if not looks_like_btc_5m(question):
            continue

        end_date = row.get("endDate") or row.get("end_date") or ""
        btc_markets.append({
            "slug": row.get("slug"),
            "condition_id": row.get("conditionId"),
            "question": question,
            "end_date": end_date,
        })

    btc_markets.sort(key=lambda x: x["end_date"] or "", reverse=True)

    current_market = btc_markets[0] if btc_markets else None
    rolling = btc_markets[:ROLLING_MARKETS]

    return current_market, rolling


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
    for row in rows[:30]:
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


def fetch_market_traders(conn, condition_id: str):
    data = safe_get_json(
        f"{DATA_API}/trades",
        params={"market": condition_id, "limit": 200},
    )
    if not data:
        return 0

    if isinstance(data, dict):
        rows = data.get("history") or data.get("data") or data.get("trades") or []
    elif isinstance(data, list):
        rows = data
    else:
        rows = []

    added = 0
    for row in rows:
        address = row.get("proxyWallet") or row.get("walletAddress") or row.get("user") or row.get("maker") or row.get("address")
        if not address:
            continue
        upsert_wallet(conn, address=address, source="market_trade")
        added += 1
    return added


def fetch_recent_trades(conn, wallet: str, condition_id: str, limit: int = 20):
    data = safe_get_json(
        f"{DATA_API}/trades",
        params={
            "user": wallet,
            "market": condition_id,
            "limit": limit,
        },
    )
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
        if timestamp and timestamp < 10_000_000_000:
            timestamp *= 1000

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


def rolling_recent_pnl(wallet: str, rolling_condition_ids: list[str]) -> float:
    total = 0.0
    for condition_id in rolling_condition_ids:
        data = safe_get_json(
            f"{DATA_API}/trades",
            params={"user": wallet, "market": condition_id, "limit": 20},
        )
        if not data:
            continue

        if isinstance(data, dict):
            rows = data.get("history") or data.get("data") or data.get("trades") or []
        elif isinstance(data, list):
            rows = data
        else:
            rows = []

        for row in rows:
            size = float(row.get("size", 0) or row.get("shares", 0) or 0)
            price = float(row.get("price", 0) or 0)
            total += size * price
    return total


def score_wallet(conn, wallet: str, rolling_condition_ids: list[str]):
    recent_trades = conn.execute(
        """
        SELECT tx_hash, size, price, timestamp, side, outcome
        FROM trades
        WHERE wallet = ?
        ORDER BY timestamp DESC
        LIMIT 20
        """,
        (wallet,),
    ).fetchall()

    wins = 0
    losses = 0
    streak = 0
    avg_size = 0.0
    last_trade_ts = None
    recent_trade_count = len(recent_trades)
    goat_reason = "filtered_out"

    if recent_trades:
        avg_size = sum(float(r["size"] or 0) for r in recent_trades) / max(len(recent_trades), 1)
        last_trade_ts = max(int(r["timestamp"] or 0) for r in recent_trades)

    rolling_pnl = rolling_recent_pnl(wallet, rolling_condition_ids)

    # simple streak proxy: count consecutive current-market trades in same direction/outcome
    if recent_trades:
        first_side = recent_trades[0]["side"]
        first_outcome = recent_trades[0]["outcome"]
        for r in recent_trades:
            if r["side"] == first_side and r["outcome"] == first_outcome:
                streak += 1
            else:
                break

    recent_win_rate = 1.0 if recent_trade_count > 0 else 0.0

    now_ms = int(time.time() * 1000)
    age_ms = now_ms - last_trade_ts if last_trade_ts else 10**15

    recency_bonus = max(0.0, (MAX_AGE_MS - age_ms) / MAX_AGE_MS) * 600.0
    size_bonus = min(avg_size, 250.0) * 3.0
    streak_bonus = streak * 120.0
    trade_bonus = min(recent_trade_count, 10) * 30.0
    rolling_bonus = rolling_pnl * 0.02
    win_bonus = recent_win_rate * 120.0

    penalty = 0.0
    if avg_size < MIN_AVG_SIZE:
        penalty += 600.0
    if recent_trade_count < MIN_RECENT_TRADES:
        penalty += 500.0
    if age_ms > MAX_AGE_MS:
        penalty += 1000.0

    goat_score = recency_bonus + size_bonus + streak_bonus + trade_bonus + rolling_bonus + win_bonus - penalty

    is_goat = int(
        age_ms <= MAX_AGE_MS
        and avg_size >= MIN_AVG_SIZE
        and recent_trade_count >= MIN_RECENT_TRADES
        and goat_score > 0
    )

    if age_ms > MAX_AGE_MS:
        goat_reason = "stale"
    elif avg_size < MIN_AVG_SIZE:
        goat_reason = "small_size"
    elif recent_trade_count < MIN_RECENT_TRADES:
        goat_reason = "too_few_trades"
    else:
        goat_reason = "goat"

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
            last_trade_ts = ?,
            is_goat = ?,
            goat_reason = ?
        WHERE address = ?
        """,
        (
            goat_score,
            streak,
            rolling_pnl,
            avg_size,
            recent_trade_count,
            recent_win_rate,
            rolling_pnl,
            last_trade_ts,
            is_goat,
            goat_reason,
            wallet,
        ),
    )


def mark_elite_wallet_trades(conn):
    elite_wallets = conn.execute(
        """
        SELECT address
        FROM wallets
        WHERE is_goat = 1
        ORDER BY score DESC
        LIMIT 20
        """
    ).fetchall()

    elite_set = {row["address"] for row in elite_wallets}
    conn.execute("UPDATE trades SET is_elite = 0")
    for wallet in elite_set:
        conn.execute("UPDATE trades SET is_elite = 1 WHERE wallet = ?", (wallet,))


def collector_loop():
    cfg = load_config()
    poll_seconds = int(cfg.get("poll_seconds", 15))

    init_db()

    while True:
        print("Collector tick...")

        current_market, rolling_markets = discover_btc_5m_markets()
        if not current_market:
            print("No active BTC 5m markets found.")
            time.sleep(poll_seconds)
            continue

        cfg["market"]["slug"] = current_market["slug"]
        cfg["market"]["condition_id"] = current_market["condition_id"]
        cfg["market"]["rolling_count"] = len(rolling_markets)
        save_config(cfg)

        current_condition_id = current_market["condition_id"]
        rolling_condition_ids = [m["condition_id"] for m in rolling_markets if m.get("condition_id")]

        conn = get_conn()

        traders_added = fetch_market_traders(conn, current_condition_id)
        holders_added = fetch_market_holders(conn, current_condition_id)
        leaderboard_added = fetch_leaderboard_wallets(conn)

        wallets = conn.execute(
            """
            SELECT address
            FROM wallets
            ORDER BY
                CASE source
                    WHEN 'market_trade' THEN 1
                    WHEN 'holders' THEN 2
                    WHEN 'leaderboard' THEN 3
                    ELSE 4
                END,
                address ASC
            LIMIT 100
            """
        ).fetchall()

        # clear old trades so Recent GOAT Trades is current-market only
        conn.execute("DELETE FROM trades")

        for row in wallets:
            wallet = row["address"]
            fetch_recent_trades(conn, wallet, current_condition_id, limit=20)
            score_wallet(conn, wallet, rolling_condition_ids)

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

        print(
            f"Done. current_slug={current_market['slug']} rolling={len(rolling_markets)} "
            f"market_traders={traders_added} holders={holders_added} leaderboard={leaderboard_added}"
        )
        time.sleep(poll_seconds)


if __name__ == "__main__":
    collector_loop()
