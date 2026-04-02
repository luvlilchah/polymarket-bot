from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, abort, jsonify, render_template

from db import get_conn, init_db

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"

app = Flask(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_config() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def ts_to_iso(ms: int | None) -> str:
    if not ms:
        return "—"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def polymarket_profile_url(address: str) -> str:
    return f"https://polymarket.com/@{address}"


@contextmanager
def db():
    conn = get_conn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# Register ts_to_iso as a Jinja filter so templates can use {{ val | ts_to_iso }}
@app.template_filter("ts_to_iso")
def ts_to_iso_filter(ms):
    return ts_to_iso(ms)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    config = load_config()
    with db() as conn:
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
            SELECT t.tx_hash, t.wallet, t.side, t.outcome, t.size, t.price,
                   t.title, t.timestamp, t.is_elite, t.resolved_pnl, t.is_win,
                   COALESCE(w.name, w.pseudonym, substr(t.wallet,1,10) || '...') AS display_name
            FROM trades t
            JOIN wallets w ON w.address = t.wallet
            WHERE w.is_goat = 1
            ORDER BY t.timestamp DESC
            LIMIT 40
            """
        ).fetchall()

        last_run = conn.execute(
            "SELECT value FROM meta WHERE key = 'last_run_ms'"
        ).fetchone()

    return render_template(
        "index.html",
        config=config,
        wallets=wallets,
        recent_trades=recent_trades,
        last_run=ts_to_iso(int(last_run[0])) if last_run else "—",
        polymarket_profile_url=polymarket_profile_url,
        current_market_slug=config.get("market", {}).get("slug", "—"),
        rolling_count=config.get("market", {}).get("rolling_count", 0),
    )


@app.route("/wallet/<address>")
def wallet_detail(address: str):
    config = load_config()
    with db() as conn:
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
            abort(404)

        trades = conn.execute(
            """
            SELECT tx_hash, wallet, side, outcome, size, price, title,
                   timestamp, resolved_pnl, is_win, is_elite
            FROM trades
            WHERE wallet = ?
            ORDER BY timestamp DESC
            LIMIT 50
            """,
            (address,),
        ).fetchall()

    return render_template(
        "wallet.html",
        config=config,
        wallet=wallet,
        trades=trades,
        polymarket_profile_url=polymarket_profile_url,
    )


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

@app.route("/api/leaderboard")
def api_leaderboard():
    with db() as conn:
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

    return jsonify([
        {
            "address":            r["address"],
            "display_name":       r["display_name"],
            "score":              r["score"],
            "current_streak":     r["current_streak"],
            "recent_pnl":         r["recent_pnl"],
            "avg_size":           r["avg_size"],
            "recent_trade_count": r["recent_trade_count"],
            "recent_win_rate":    r["recent_win_rate"],
            "realized_pnl":       r["realized_pnl"],
            "last_trade_ts":      r["last_trade_ts"],
            "last_trade_iso":     ts_to_iso(r["last_trade_ts"]),
            "goat_reason":        r["goat_reason"],
            "profile_url":        polymarket_profile_url(r["address"]),
            "detail_url":         f"/wallet/{r['address']}",
        }
        for r in rows
    ])


@app.route("/api/recent-trades")
def api_recent_trades():
    with db() as conn:
        rows = conn.execute(
            """
            SELECT t.tx_hash, t.wallet, t.side, t.outcome, t.size, t.price,
                   t.title, t.timestamp, t.is_elite, t.resolved_pnl, t.is_win,
                   COALESCE(w.name, w.pseudonym, substr(t.wallet,1,10) || '...') AS display_name
            FROM trades t
            JOIN wallets w ON w.address = t.wallet
            WHERE w.is_goat = 1
            ORDER BY t.timestamp DESC
            LIMIT 50
            """
        ).fetchall()

    return jsonify([
        {
            "tx_hash":       r["tx_hash"],
            "wallet":        r["wallet"],
            "display_name":  r["display_name"],
            "side":          r["side"],
            "outcome":       r["outcome"],
            "size":          r["size"],
            "price":         r["price"],
            "title":         r["title"],
            "timestamp":     r["timestamp"],
            "timestamp_iso": ts_to_iso(r["timestamp"]),
            "resolved_pnl":  r["resolved_pnl"],
            "is_win":        r["is_win"],
            "is_elite":      bool(r["is_elite"]),
            "profile_url":   polymarket_profile_url(r["wallet"]),
            "detail_url":    f"/wallet/{r['wallet']}",
        }
        for r in rows
    ])


@app.route("/api/elite-trades")
def api_elite_trades():
    """Only trades from the top 20 GOAT wallets — the actual copy-trade signal."""
    with db() as conn:
        rows = conn.execute(
            """
            SELECT t.tx_hash, t.wallet, t.side, t.outcome, t.size, t.price,
                   t.title, t.timestamp, t.resolved_pnl, t.is_win,
                   w.score, w.current_streak, w.recent_win_rate,
                   COALESCE(w.name, w.pseudonym, substr(t.wallet,1,10) || '...') AS display_name
            FROM trades t
            JOIN wallets w ON w.address = t.wallet
            WHERE t.is_elite = 1
            ORDER BY t.timestamp DESC
            LIMIT 50
            """
        ).fetchall()

    return jsonify([
        {
            "tx_hash":          r["tx_hash"],
            "wallet":           r["wallet"],
            "display_name":     r["display_name"],
            "side":             r["side"],
            "outcome":          r["outcome"],
            "size":             r["size"],
            "price":            r["price"],
            "title":            r["title"],
            "timestamp":        r["timestamp"],
            "timestamp_iso":    ts_to_iso(r["timestamp"]),
            "resolved_pnl":     r["resolved_pnl"],
            "is_win":           r["is_win"],
            "wallet_score":     r["score"],
            "wallet_streak":    r["current_streak"],
            "wallet_win_rate":  r["recent_win_rate"],
            "profile_url":      polymarket_profile_url(r["wallet"]),
            "detail_url":       f"/wallet/{r['wallet']}",
        }
        for r in rows
    ])


@app.route("/api/status")
def api_status():
    """Quick health check — last collector run and current market."""
    config = load_config()
    with db() as conn:
        last_run = conn.execute(
            "SELECT value FROM meta WHERE key = 'last_run_ms'"
        ).fetchone()
        goat_count = conn.execute(
            "SELECT COUNT(*) FROM wallets WHERE is_goat = 1"
        ).fetchone()[0]
        elite_trade_count = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE is_elite = 1"
        ).fetchone()[0]

    last_run_ms = int(last_run[0]) if last_run else None
    return jsonify({
        "last_run_ms":        last_run_ms,
        "last_run_iso":       ts_to_iso(last_run_ms),
        "current_slug":       config.get("market", {}).get("slug", "—"),
        "rolling_count":      config.get("market", {}).get("rolling_count", 0),
        "goat_count":         goat_count,
        "elite_trade_count":  elite_trade_count,
    })


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=8000, debug=False)
