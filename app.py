from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, abort, jsonify, render_template

from db import get_conn, init_db

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"

app = Flask(__name__)


def load_config() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def ts_to_iso(ms: int | None) -> str:
    if not ms:
        return "—"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def polymarket_profile_url(address: str) -> str:
    return f"https://polymarket.com/@{address}"


@app.route("/")
def index():
    config = load_config()
    conn = get_conn()

    wallets = conn.execute(
        """
        SELECT address,
               COALESCE(name, pseudonym, substr(address,1,10) || '...') AS display_name,
               score, current_streak, recent_pnl, avg_size, recent_trade_count,
               recent_win_rate, realized_pnl, last_trade_ts, is_goat, goat_reason
        FROM wallets
        WHERE is_goat = 1
        ORDER BY score DESC, last_trade_ts DESC
        LIMIT 25
        """
    ).fetchall()

    recent_trades = conn.execute(
        """
        SELECT t.tx_hash, t.wallet, t.side, t.outcome, t.size, t.price, t.title, t.timestamp, t.is_elite
        FROM trades t
        JOIN wallets w ON w.address = t.wallet
        WHERE w.is_goat = 1
        ORDER BY t.timestamp DESC
        LIMIT 40
        """
    ).fetchall()

    last_run = conn.execute("SELECT value FROM meta WHERE key = 'last_run_ms'").fetchone()
    conn.close()

    return render_template(
        "index.html",
        config=config,
        wallets=wallets,
        recent_trades=recent_trades,
        ts_to_iso=ts_to_iso,
        last_run=ts_to_iso(int(last_run[0])) if last_run else "—",
        polymarket_profile_url=polymarket_profile_url,
    )


@app.route("/wallet/<address>")
def wallet_detail(address: str):
    config = load_config()
    conn = get_conn()

    wallet = conn.execute(
        """
        SELECT address,
               COALESCE(name, pseudonym, substr(address,1,10) || '...') AS display_name,
               score, current_streak, recent_pnl, avg_size, recent_trade_count,
               recent_win_rate, realized_pnl, last_trade_ts, is_goat, goat_reason
        FROM wallets
        WHERE address = ?
        """,
        (address,),
    ).fetchone()

    if not wallet:
        conn.close()
        abort(404)

    trades = conn.execute(
        """
        SELECT tx_hash, wallet, side, outcome, size, price, title, timestamp, is_elite
        FROM trades
        WHERE wallet = ?
        ORDER BY timestamp DESC
        LIMIT 50
        """,
        (address,),
    ).fetchall()

    conn.close()

    return render_template(
        "wallet.html",
        config=config,
        wallet=wallet,
        trades=trades,
        ts_to_iso=ts_to_iso,
        polymarket_profile_url=polymarket_profile_url,
    )


@app.route("/api/leaderboard")
def api_leaderboard():
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT address,
               COALESCE(name, pseudonym, substr(address,1,10) || '...') AS display_name,
               score, current_streak, recent_pnl, avg_size, recent_trade_count,
               recent_win_rate, realized_pnl, last_trade_ts, goat_reason
        FROM wallets
        WHERE is_goat = 1
        ORDER BY score DESC, last_trade_ts DESC
        LIMIT 25
        """
    ).fetchall()
    conn.close()

    payload = []
    for r in rows:
        payload.append({
            "address": r["address"],
            "display_name": r["display_name"],
            "score": r["score"],
            "current_streak": r["current_streak"],
            "recent_pnl": r["recent_pnl"],
            "avg_size": r["avg_size"],
            "recent_trade_count": r["recent_trade_count"],
            "recent_win_rate": r["recent_win_rate"],
            "realized_pnl": r["realized_pnl"],
            "last_trade_ts": r["last_trade_ts"],
            "last_trade_iso": ts_to_iso(r["last_trade_ts"]),
            "goat_reason": r["goat_reason"],
            "profile_url": polymarket_profile_url(r["address"]),
            "detail_url": f"/wallet/{r['address']}",
        })
    return jsonify(payload)


@app.route("/api/recent-trades")
def api_recent_trades():
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT t.tx_hash, t.wallet, t.side, t.outcome, t.size, t.price, t.title, t.timestamp, t.is_elite
        FROM trades t
        JOIN wallets w ON w.address = t.wallet
        WHERE w.is_goat = 1
        ORDER BY t.timestamp DESC
        LIMIT 50
        """
    ).fetchall()
    conn.close()

    payload = []
    for r in rows:
        payload.append({
            "tx_hash": r["tx_hash"],
            "wallet": r["wallet"],
            "side": r["side"],
            "outcome": r["outcome"],
            "size": r["size"],
            "price": r["price"],
            "title": r["title"],
            "timestamp": r["timestamp"],
            "timestamp_iso": ts_to_iso(r["timestamp"]),
            "is_elite": bool(r["is_elite"]),
            "profile_url": polymarket_profile_url(r["wallet"]),
            "detail_url": f"/wallet/{r['wallet']}",
        })
    return jsonify(payload)


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=8000, debug=False)
