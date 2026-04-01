import argparse
import asyncio
import json
import sqlite3
from pathlib import Path

import requests
import websockets

DB_PATH = Path("polymarket_edge.db")
GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wallets (
            address TEXT PRIMARY KEY,
            source TEXT,
            score REAL DEFAULT 0,
            win_rate REAL DEFAULT 0,
            realized_pnl REAL DEFAULT 0,
            trades_count INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    return conn


def lookup_market(slug: str):
    url = f"{GAMMA_API}/markets/slug/{slug}"
    r = requests.get(url)
    data = r.json()

    print("Question     :", data.get("question"))
    print("Condition ID :", data.get("conditionId"))
    print("Token IDs    :", data.get("clobTokenIds"))


def discover_wallets(condition_id: str):
    conn = get_db()

    r = requests.get(f"{DATA_API}/leaderboard")
    wallets = r.json()

    for w in wallets[:50]:
        addr = w.get("proxyWallet") or w.get("address")
        if addr:
            conn.execute(
                "INSERT OR IGNORE INTO wallets(address, source) VALUES(?, ?)",
                (addr, "leaderboard"),
            )

    conn.commit()
    print("Wallets added")


def score_wallets(condition_id: str):
    conn = get_db()
    wallets = conn.execute("SELECT address FROM wallets").fetchall()

    for (address,) in wallets:
        r = requests.get(f"{DATA_API}/closed-positions?user={address}")
        positions = r.json()

        pnl = 0
        wins = 0
        total = 0

        for p in positions:
            val = float(p.get("realizedPnl", 0) or 0)
            pnl += val
            total += 1
            if val > 0:
                wins += 1

        win_rate = (wins / total) if total else 0
        score = pnl + (win_rate * 100)

        conn.execute("""
            UPDATE wallets
            SET score = ?, win_rate = ?, realized_pnl = ?
            WHERE address = ?
        """, (score, win_rate, pnl, address))

    conn.commit()
    print("Scoring complete")


async def watch_orderbook(asset_ids):
    async with websockets.connect(WS_URL) as ws:
        await ws.send(json.dumps({
            "assets_ids": asset_ids,
            "type": "market"
        }))

        while True:
            msg = await ws.recv()
            print(msg)


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd")

    sub.add_parser("lookup-market").add_argument("--slug", required=True)
    sub.add_parser("discover-wallets").add_argument("--condition-id", required=True)
    sub.add_parser("score-wallets").add_argument("--condition-id", required=True)
    sub.add_parser("watch-orderbook").add_argument("--asset-id", action="append")

    args = parser.parse_args()

    if args.cmd == "lookup-market":
        lookup_market(args.slug)
    elif args.cmd == "discover-wallets":
        discover_wallets(args.condition_id)
    elif args.cmd == "score-wallets":
        score_wallets(args.condition_id)
    elif args.cmd == "watch-orderbook":
        asyncio.run(watch_orderbook(args.asset_id))


if __name__ == "__main__":
    main()
