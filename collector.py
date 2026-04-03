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

MAX_AGE_MS        = 15 * 60 * 1000  # 15 min — trade must be this recent to count
MIN_AVG_SIZE      = 10.0
MIN_RECENT_TRADES = 2
ROLLING_MARKETS   = 18
ELITE_COUNT       = 20
MAX_WALLETS       = 100
RETRY_ATTEMPTS    = 3
RETRY_DELAY       = 1.5

FAST_LOOP_INTERVAL_S = 5    # how often to check for new live trades
SLOW_LOOP_INTERVAL_S = 300  # how often to rescore wallets and refresh PnL
PNL_REFRESH_INTERVAL_S = 300


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


def is_btc_5m_trade(trade: dict, valid_condition_ids: set[str]) -> bool:
    condition_id = trade.get("conditionId") or ""
    if condition_id and condition_id in valid_condition_ids:
        return True
    slug = (trade.get("slug") or trade.get("eventSlug") or "").lower()
    return "btc-updown-5m" in slug


def discover_btc_5m_markets() -> tuple[dict | None, list[dict]]:
    all_rows: list[dict] = []
    for offset in range(0, 1000, 200):
        data = safe_get_json(
            f"{GAMMA_API}/markets",
            params={"limit": 200, "offset": offset, "order": "createdAt", "ascending": "false"},
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
# Wallet collection (slow loop)
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


def fetch_market_traders(conn, token_id: str, valid_condition_ids: set[str]) -> int:
    data = safe_get_json(f"{DATA_API}/trades", params={"token": token_id, "limit": 200})
    if not data:
        return 0
    rows = _extract_rows(data, "history", "data", "trades")
    added = 0
    for row in rows:
        if not is_btc_5m_trade(row, valid_condition_ids):
            continue
        address = _extract_address(row) or row.get("user") or row.get("maker") or None
        if not address:
            continue
        upsert_wallet(conn, address=address, source="market_trade",
                      name=row.get("name"), pseudonym=row.get("pseudonym"))
        added += 1
    return added


# ---------------------------------------------------------------------------
# Fast loop — one call to market trades endpoint, match against known wallets
# ---------------------------------------------------------------------------

def fast_tick(conn, current_token_id: str, valid_condition_ids: set[str]) -> int:
    """
    Single API call to get the latest trades on the current market.
    Stores any new trades from known wallets instantly.
    Returns number of new trades saved.
    """
    data = safe_get_json(
        f"{DATA_API}/trades",
        params={"token": current_token_id, "limit": 200},
    )
    if not data:
        return 0

    rows = _extract_rows(data, "history", "data", "trades")

    # load known wallet addresses into a set for O(1) lookup
    known = {
        row[0] for row in conn.execute("SELECT address FROM wallets").fetchall()
    }

    saved = 0
    for row in rows:
        if not is_btc_5m_trade(row, valid_condition_ids):
            continue

        address = _extract_address(row) or row.get("user") or row.get("maker") or None
        if not address or address not in known:
            continue

        tx_hash = row.get("transactionHash") or row.get("txHash") or row.get("id")
        if not tx_hash:
            continue

        raw_ts = int(row.get("timestamp", 0) or 0)
        timestamp = raw_ts * 1000 if raw_ts and raw_ts < 10_000_000_000 else raw_ts
        size = float(row.get("size", 0) or 0)
        price = float(row.get("price", 0) or 0)
        side = str(row.get("side") or "?")
        outcome = str(row.get("outcome") or row.get("outcomeIndex") or "?")
        title = row.get("title") or "Unknown market"
        market_slug = row.get("slug") or row.get("eventSlug") or ""

        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO trades
            (tx_hash, wallet, side, outcome, size, price, title, market_slug,
             timestamp, resolved_pnl, is_win, is_elite)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 0, -1, 0)
            """,
            (tx_hash, address, side, outcome, size, price, title, market_slug, timestamp),
        )
        if cursor.rowcount:
            saved += 1

    return saved


# ---------------------------------------------------------------------------
# Closed positions PnL — cached per wallet
# ---------------------------------------------------------------------------

def fetch_closed_positions_pnl(
    conn, wallet: str, rolling_token_ids: list[str]
) -> tuple[float, int, int]:
    now_s = int(time.time())
    row = conn.execute(
        "SELECT last_pnl_check_ts, realized_pnl, recent_win_rate, recent_trade_count FROM wallets WHERE address = ?",
        (wallet,),
    ).fetchone()

    last_check = int(row["last_pnl_check_ts"] or 0) if row and row["last_pnl_check_ts"] else 0
    if now_s - last_check < PNL_REFRESH_INTERVAL_S:
        if row:
            cached_pnl = float(row["realized_pnl"] or 0)
            cached_wr = float(row["recent_win_rate"] or 0.5)
            cached_trades = int(row["recent_trade_count"] or 0)
            wins = round(cached_wr * cached_trades)
            losses = cached_trades - wins
            return cached_pnl, wins, losses
        return 0.0, 0, 0

    data = safe_get_json(
        f"{DATA_API}/positions",
        params={"user": wallet, "sizeThreshold": "0"},
    )
    conn.execute(
        "UPDATE wallets SET last_pnl_check_ts = ? WHERE address = ?",
        (now_s, wallet),
    )
    if not data:
        return 0.0, 0, 0

    rows = _extract_rows(data, "positions", "data")
    token_set = set(rolling_token_ids)
    total_pnl = 0.0
    wins = 0
    losses = 0
    for r in rows:
        asset = r.get("asset") or r.get("tokenId") or ""
        if asset not in token_set:
            continue
        pnl = float(r.get("realizedPnl") or r.get("pnl") or 0)
        total_pnl += pnl
        if pnl > 0:
            wins += 1
        elif pnl < 0:
            losses += 1
    return total_pnl, wins, losses


# ---------------------------------------------------------------------------
# Wallet scoring (slow loop)
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

    rolling_pnl, wins, losses = fetch_closed_positions_pnl(conn, wallet, rolling_token_ids)
    total_resolved = wins + losses
    recent_win_rate = (wins / total_resolved) if total_resolved > 0 else 0.5
    streak = wins - losses

    now_ms = int(time.time() * 1000)
    age_ms = (now_ms - last_trade_ts) if last_trade_ts else 10 ** 15

    recency_bonus = max(0.0, (MAX_AGE_MS - age_ms) / MAX_AGE_MS) * 600.0
    size_bonus    = min(avg_size, 250.0) * 3.0
    streak_bonus  = max(streak, 0) * 120.0
    trade_bonus   = min(recent_trade_count, 10) * 30.0
    rolling_bonus = max(rolling_pnl, 0) * 0.02
    win_bonus     = recent_win_rate * 200.0

    penalty = 0.0
    if avg_size < MIN_AVG_SIZE:      penalty += 600.0
    if recent_trade_count < MIN_RECENT_TRADES: penalty += 500.0
    if age_ms > MAX_AGE_MS:          penalty += 1000.0
    if streak < -2:                  penalty += abs(streak) * 60.0

    goat_score = recency_bonus + size_bonus + streak_bonus + trade_bonus + rolling_bonus + win_bonus - penalty

    is_goat = int(
        age_ms <= MAX_AGE_MS
        and avg_size >= MIN_AVG_SIZE
        and recent_trade_count >= MIN_RECENT_TRADES
        and goat_score > 0
    )

    if age_ms > MAX_AGE_MS:               goat_reason = "stale"
    elif avg_size < MIN_AVG_SIZE:          goat_reason = "small_size"
    elif recent_trade_count < MIN_RECENT_TRADES: goat_reason = "too_few_trades"
    elif not is_goat:                      goat_reason = "low_score"
    else:                                  goat_reason = "goat"

    conn.execute(
        """
        UPDATE wallets
        SET score=?, current_streak=?, recent_pnl=?, avg_size=?,
            recent_trade_count=?, recent_win_rate=?, realized_pnl=?,
            last_trade_ts=?, is_goat=?, goat_reason=?
        WHERE address=?
        """,
        (goat_score, streak, rolling_pnl, avg_size, recent_trade_count,
         recent_win_rate, rolling_pnl, last_trade_ts, is_goat, goat_reason, wallet),
    )


def mark_elite_wallet_trades(conn) -> None:
    elite_wallets = conn.execute(
        "SELECT address FROM wallets WHERE is_goat=1 ORDER BY score DESC LIMIT ?",
        (ELITE_COUNT,),
    ).fetchall()
    elite_set = {row[0] for row in elite_wallets}
    conn.execute("UPDATE trades SET is_elite=0")
    for wallet in elite_set:
        conn.execute("UPDATE trades SET is_elite=1 WHERE wallet=?", (wallet,))


# ---------------------------------------------------------------------------
# Main loop — two speeds
# ---------------------------------------------------------------------------

def collector_loop() -> None:
    init_db()

    last_slow_tick = 0.0
    current_market = None
    rolling_markets: list[dict] = []
    current_token_id = ""
    valid_condition_ids: set[str] = set()
    rolling_token_ids: list[str] = []

    print("Collector starting...")

    while True:
        now = time.time()

        # ── SLOW LOOP: rediscover markets, rescore wallets ──────────────────
        if now - last_slow_tick >= SLOW_LOOP_INTERVAL_S:
            print("Slow tick — rediscovering markets and rescoring wallets...")
            current_market, rolling_markets = discover_btc_5m_markets()

            if current_market:
                clob_ids = current_market.get("clob_token_ids") or []
                current_token_id = clob_ids[0] if clob_ids else current_market["condition_id"]

                rolling_token_ids = []
                valid_condition_ids = set()
                for m in rolling_markets:
                    rolling_token_ids.extend(m.get("clob_token_ids") or [])
                    if m.get("condition_id"):
                        valid_condition_ids.add(m["condition_id"])

                cfg = load_config()
                cfg["market"]["slug"] = current_market["slug"]
                cfg["market"]["condition_id"] = current_market["condition_id"]
                cfg["market"]["token_id"] = current_token_id
                cfg["market"]["rolling_count"] = len(rolling_markets)
                save_config(cfg)

                conn = get_conn()
                try:
                    fetch_market_traders(conn, current_token_id, valid_condition_ids)
                    fetch_market_holders(conn, current_market["condition_id"])
                    fetch_leaderboard_wallets(conn)

                    wallets = conn.execute(
                        """
                        SELECT address FROM wallets
                        ORDER BY CASE source
                            WHEN 'market_trade' THEN 1
                            WHEN 'holders'      THEN 2
                            WHEN 'leaderboard'  THEN 3
                            ELSE 4
                        END, address ASC
                        LIMIT ?
                        """,
                        (MAX_WALLETS,),
                    ).fetchall()

                    for row in wallets:
                        score_wallet(conn, row[0], rolling_token_ids)
                        time.sleep(0.1)

                    mark_elite_wallet_trades(conn)

                    conn.execute(
                        "INSERT INTO meta(key,value) VALUES('last_run_ms',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                        (str(int(time.time() * 1000)),),
                    )
                    conn.commit()
                    print(f"Slow tick done. slug={current_market['slug']} wallets={len(wallets)}")
                except Exception as e:
                    print(f"Slow tick error: {e}")
                    conn.rollback()
                finally:
                    conn.close()
            else:
                print("No active BTC 5m markets found.")

            last_slow_tick = time.time()

        # ── FAST LOOP: one call, check for new elite trades ─────────────────
        if current_token_id:
            conn = get_conn()
            try:
                new_trades = fast_tick(conn, current_token_id, valid_condition_ids)
                if new_trades:
                    mark_elite_wallet_trades(conn)
                    conn.execute(
                        "INSERT INTO meta(key,value) VALUES('last_run_ms',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                        (str(int(time.time() * 1000)),),
                    )
                    print(f"Fast tick: {new_trades} new trade(s) from known wallets")
                conn.commit()
            except Exception as e:
                print(f"Fast tick error: {e}")
                conn.rollback()
            finally:
                conn.close()

        time.sleep(FAST_LOOP_INTERVAL_S)


if __name__ == "__main__":
    collector_loop()
