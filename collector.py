from __future__ import annotations

import json
import time
from datetime import datetime, timezone
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
ELITE_COUNT = 20
MAX_WALLETS = 100
RETRY_ATTEMPTS = 3
RETRY_DELAY = 1.5


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_config(cfg: dict) -> None:
    with CONFIG_PATH.open("w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)


# ---------------------------------------------------------------------------
# HTTP with retry
# ---------------------------------------------------------------------------

def safe_get_json(url: str, params: dict | None = None):
    for attempt in range(RETRY_ATTEMPTS):
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code < 500:
                print(f"Client error {e.response.status_code}: {url}")
                return None
            print(f"Server error (attempt {attempt + 1}): {url} — {e}")
        except Exception as e:
            print(f"Request failed (attempt {attempt + 1}): {url} — {e}")

        if attempt < RETRY_ATTEMPTS - 1:
            time.sleep(RETRY_DELAY * (2 ** attempt))

    return None


# ---------------------------------------------------------------------------
# Wallet upsert
# ---------------------------------------------------------------------------

def upsert_wallet(
    conn,
    address: str,
    source: str = "leaderboard",
    name: str | None = None,
    pseudonym: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO wallets(address, name, pseudonym, source)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(address) DO UPDATE SET
            name      = COALESCE(excluded.name,      wallets.name),
            pseudonym = COALESCE(excluded.pseudonym, wallets.pseudonym),
            source    = COALESCE(excluded.source,    wallets.source)
        """,
        (address, name, pseudonym, source),
    )


# ---------------------------------------------------------------------------
# Market discovery
# ---------------------------------------------------------------------------

def looks_like_btc_5m(row: dict) -> bool:
    slug = (row.get("slug") or "").lower()
    question = (row.get("question") or "").lower()
    series_slug = ""
    for event in (row.get("events") or []):
        series_slug = (event.get("seriesSlug") or "").lower()

    if "btc-updown-5m" in slug:
        return True
    if "btc-up-or-down-5m" in series_slug:
        return True
    if "bitcoin up or down - 5 minutes" in question:
        return True
    if "bitcoin up or down" in question and "5 min" in question:
        return True
    return False


def discover_btc_5m_markets() -> tuple[dict | None, list[dict]]:
    all_rows: list[dict] = []

    for offset in range(0, 1000, 200):
        data = safe_get_json(
            f"{GAMMA_API}/markets",
            params={
                "limit": 200,
                "offset": offset,
                "order": "createdAt",
                "ascending": "false",
            },
        )
        if not data or not isinstance(data, list):
            break
        all_rows.extend(data)
        if len(data) < 200:
            break

    now_ms = int(time.time() * 1000)
    btc_markets = []

    for row in all_rows:
        if not looks_like_btc_5m(row):
            continue
        condition_id = row.get("conditionId")
        if not condition_id:
            continue

        end_date_raw = row.get("endDate") or row.get("end_date") or ""
        end_ts = None
        if end_date_raw:
            try:
                end_ts = int(datetime.fromisoformat(
                    end_date_raw.replace("Z", "+00:00")
                ).timestamp() * 1000)
            except Exception:
                pass

        if not end_ts or end_ts <= now_ms:
            continue

        clob_ids_raw = row.get("clobTokenIds") or "[]"
        try:
            clob_token_ids = json.loads(clob_ids_raw)
        except Exception:
            clob_token_ids = []

        btc_markets.append({
            "slug": row.get("slug") or "",
            "condition_id": condition_id,
            "clob_token_ids": clob_token_ids,
            "question": row.get("question") or "",
            "end_date": end_date_raw,
            "end_ts": end_ts,
        })

    btc_markets.sort(key=lambda x: x["end_ts"], reverse=True)
    return (btc_markets[0] if btc_markets else None), btc_markets[:ROLLING_MARKETS]


# ---------------------------------------------------------------------------
# Wallet collection
# ---------------------------------------------------------------------------

def _extract_rows(data, *keys: str) -> list:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in keys:
            val = data.get(key)
            if val:
                return val
    return []


def _extract_address(row: dict) -> str | None:
    return (
        row.get("proxyWallet")
        or row.get("walletAddress")
        or row.get("address")
        or None
    )


def fetch_leaderboard_wallets(conn) -> int:
    data = safe_get_json(f"{DATA_API}/v1/leaderboard")
    if not data:
        return 0
    rows = _extract_rows(data, "users", "data", "leaderboard")
    added = 0
    for row in rows[:30]:
        address = _extract_address(row)
        if not address:
            continue
        upsert_wallet(conn, address=address, source="leaderboard",
                      name=row.get("name"), pseudonym=row.get("pseudonym"))
        added += 1
    return added


def fetch_market_holders(conn, condition_id: str) -> int:
    # holders endpoint uses condition_id with 'market' param
    data = safe_get_json(f"{DATA_API}/holders", params={"market": condition_id})
    if not data:
        return 0
    rows = _extract_rows(data, "holders", "data")
    added = 0
    for row in rows[:100]:
        address = _extract_address(row)
        if not address:
            continue
        upsert_wallet(conn, address=address, source="holders",
                      name=row.get("name"), pseudonym=row.get("pseudonym"))
        added += 1
    return added


def fetch_market_traders(conn, token_id: str) -> int:
    # trades endpoint uses token_id with 'token' param
    data = safe_get_json(f"{DATA_API}/trades", params={"token": token_id, "limit": 200})
    if not data:
        return 0
    rows = _extract_rows(data, "history", "data", "trades")
    added = 0
    for row in rows:
        address = _extract_address(row) or row.get("user") or row.get("maker") or None
        if not address:
            continue
        upsert_wallet(conn, address=address, source="market_trade")
        added += 1
    return added


# ---------------------------------------------------------------------------
# Trade fetching
# ---------------------------------------------------------------------------

def _normalize_timestamp(ts) -> int:
    ts = int(ts)
    return ts * 1000 if ts and ts < 10_000_000_000 else ts


def fetch_recent_trades(conn, wallet: str, token_id: str, limit: int = 20) -> list[dict]:
    # trades endpoint uses 'token' param not 'market'
    data = safe_get_json(
        f"{DATA_API}/trades",
        params={"user": wallet, "token": token_id, "limit": limit},
    )
    if not data:
        return []

    rows = _extract_rows(data, "history", "data", "trades")
    out = []

    for row in rows[:limit]:
        tx_hash = row.get("transactionHash") or row.get("txHash") or row.get("id")
        if not tx_hash:
            continue

        raw_ts = row.get("timestamp", 0) or row.get("time", 0) or 0
        timestamp = _normalize_timestamp(raw_ts) if raw_ts else 0
        size = float(row.get("size", 0) or row.get("shares", 0) or 0)
        price = float(row.get("price", 0) or 0)
        side = str(row.get("side") or row.get("type") or "?")
        outcome = str(row.get("outcome") or row.get("outcomeIndex") or "?")
        title = row.get("title") or row.get("marketTitle") or row.get("question") or "Unknown market"
        market_slug = row.get("slug") or row.get("marketSlug") or ""
        resolved_pnl = float(row.get("pnl", 0) or row.get("profit", 0) or 0)
        is_win = 1 if resolved_pnl > 0 else (0 if resolved_pnl < 0 else -1)

        conn.execute(
            """
            INSERT OR IGNORE INTO trades
            (tx_hash, wallet, side, outcome, size, price, title, market_slug,
             timestamp, resolved_pnl, is_win, is_elite)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            """,
            (tx_hash, wallet, side, outcome, size, price, title, market_slug,
             timestamp, resolved_pnl, is_win),
        )

        out.append({
            "tx_hash": tx_hash, "wallet": wallet, "side": side, "outcome": outcome,
            "size": size, "price": price, "title": title, "market_slug": market_slug,
            "timestamp": timestamp, "resolved_pnl": resolved_pnl, "is_win": is_win,
        })

    return out


# ---------------------------------------------------------------------------
# Rolling realized PnL
# ---------------------------------------------------------------------------

def rolling_realized_pnl(wallet: str, rolling_token_ids: list[str]) -> float:
    total = 0.0
    for token_id in rolling_token_ids:
        data = safe_get_json(
            f"{DATA_API}/trades",
            params={"user": wallet, "token": token_id, "limit": 20},
        )
        if not data:
            continue
        rows = _extract_rows(data, "history", "data", "trades")
        for row in rows:
            total += float(row.get("pnl", 0) or row.get("profit", 0) or 0)
    return total


# ---------------------------------------------------------------------------
# Wallet scoring
# ---------------------------------------------------------------------------

def score_wallet(conn, wallet: str, rolling_token_ids: list[str]) -> None:
    recent_trades = conn.execute(
        """
        SELECT tx_hash, size, price, timestamp, side, outcome, resolved_pnl, is_win
        FROM trades
        WHERE wallet = ?
        ORDER BY timestamp DESC
        LIMIT 20
        """,
        (wallet,),
    ).fetchall()

    recent_trade_count = len(recent_trades)
    avg_size = 0.0
    last_trade_ts = None

    if recent_trades:
        avg_size = sum(float(r["size"] or 0) for r in recent_trades) / recent_trade_count
        last_trade_ts = max(int(r["timestamp"] or 0) for r in recent_trades)

    resolved = [r for r in recent_trades if r["is_win"] in (0, 1)]
    wins = sum(1 for r in resolved if r["is_win"] == 1)
    recent_win_rate = (wins / len(resolved)) if resolved else 0.5

    streak = 0
    if resolved:
        first_result = resolved[0]["is_win"]
        for r in resolved:
            if r["is_win"] == first_result:
                streak += 1
            else:
                break
        if first_result == 0:
            streak = -streak

    rolling_pnl = rolling_realized_pnl(wallet, rolling_token_ids)

    now_ms = int(time.time() * 1000)
    age_ms = (now_ms - last_trade_ts) if last_trade_ts else 10 ** 15

    recency_bonus = max(0.0, (MAX_AGE_MS - age_ms) / MAX_AGE_MS) * 600.0
    size_bonus = min(avg_size, 250.0) * 3.0
    streak_bonus = max(streak, 0) * 120.0
    trade_bonus = min(recent_trade_count, 10) * 30.0
    rolling_bonus = max(rolling_pnl, 0) * 0.02
    win_bonus = recent_win_rate * 200.0

    penalty = 0.0
    if avg_size < MIN_AVG_SIZE:
        penalty += 600.0
    if recent_trade_count < MIN_RECENT_TRADES:
        penalty += 500.0
    if age_ms > MAX_AGE_MS:
        penalty += 1000.0
    if streak < -2:
        penalty += abs(streak) * 60.0

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
    elif not is_goat:
        goat_reason = "low_score"
    else:
        goat_reason = "goat"

    conn.execute(
        """
        UPDATE wallets
        SET score              = ?,
            current_streak     = ?,
            recent_pnl         = ?,
            avg_size           = ?,
            recent_trade_count = ?,
            recent_win_rate    = ?,
            realized_pnl       = ?,
            last_trade_ts      = ?,
            is_goat            = ?,
            goat_reason        = ?
        WHERE address = ?
        """,
        (goat_score, streak, rolling_pnl, avg_size, recent_trade_count,
         recent_win_rate, rolling_pnl, last_trade_ts, is_goat, goat_reason, wallet),
    )


# ---------------------------------------------------------------------------
# Elite marking
# ---------------------------------------------------------------------------

def mark_elite_wallet_trades(conn) -> None:
    elite_wallets = conn.execute(
        """
        SELECT address FROM wallets
        WHERE is_goat = 1
        ORDER BY score DESC
        LIMIT ?
        """,
        (ELITE_COUNT,),
    ).fetchall()

    elite_set = {row["address"] for row in elite_wallets}
    conn.execute("UPDATE trades SET is_elite = 0")
    for wallet in elite_set:
        conn.execute("UPDATE trades SET is_elite = 1 WHERE wallet = ?", (wallet,))


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def collector_loop() -> None:
    cfg = load_config()
    poll_seconds = int(cfg.get("poll_seconds", 15))

    init_db()

    while True:
        print("Collector tick...")
        tick_start = time.time()

        current_market, rolling_markets = discover_btc_5m_markets()
        if not current_market:
            print("No active BTC 5m markets found. Sleeping...")
            time.sleep(poll_seconds)
            continue

        # trades API uses clobTokenId, holders API uses conditionId
        clob_ids = current_market.get("clob_token_ids") or []
        current_token_id = clob_ids[0] if clob_ids else current_market["condition_id"]
        current_condition_id = current_market["condition_id"]

        # rolling token IDs for PnL scoring
        rolling_token_ids = []
        for m in rolling_markets:
            ids = m.get("clob_token_ids") or []
            if ids:
                rolling_token_ids.append(ids[0])

        cfg["market"]["slug"] = current_market["slug"]
        cfg["market"]["condition_id"] = current_condition_id
        cfg["market"]["token_id"] = current_token_id
        cfg["market"]["rolling_count"] = len(rolling_markets)
        save_config(cfg)

        conn = get_conn()
        try:
            traders_added = fetch_market_traders(conn, current_token_id)
            holders_added = fetch_market_holders(conn, current_condition_id)
            leaderboard_added = fetch_leaderboard_wallets(conn)

            wallets = conn.execute(
                """
                SELECT address FROM wallets
                ORDER BY
                    CASE source
                        WHEN 'market_trade' THEN 1
                        WHEN 'holders'      THEN 2
                        WHEN 'leaderboard'  THEN 3
                        ELSE 4
                    END,
                    address ASC
                LIMIT ?
                """,
                (MAX_WALLETS,),
            ).fetchall()

            for row in wallets:
                fetch_recent_trades(conn, row["address"], current_token_id, limit=20)
                score_wallet(conn, row["address"], rolling_token_ids)

            mark_elite_wallet_trades(conn)

            conn.execute(
                """
                INSERT INTO meta(key, value) VALUES('last_run_ms', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (str(int(time.time() * 1000)),),
            )
            conn.commit()

        except Exception as e:
            print(f"Tick error: {e}")
            conn.rollback()
        finally:
            conn.close()

        elapsed = time.time() - tick_start
        print(
            f"Done in {elapsed:.1f}s. slug={current_market['slug']} "
            f"token={current_token_id[:12]}... "
            f"rolling={len(rolling_markets)} traders={traders_added} "
            f"holders={holders_added} leaderboard={leaderboard_added}"
        )
        time.sleep(max(0, poll_seconds - elapsed))


if __name__ == "__main__":
    collector_loop()
