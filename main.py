import os
import io
import csv
import json
import time
import math
import zipfile
import sqlite3
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from typing import Optional

import aiohttp
from aiohttp import web
import websockets
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIG
# ============================================================

LEADER_WALLET = os.getenv(
    "LEADER_WALLET",
    "0x13e0d447520ebe7f8eeaf7817211201b2c585204"
).lower()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# 0.10s = 10 requests/sec. Polymarket currently documents /trades
# at 200 requests / 10 seconds, so this stays at half the documented limit.
LEADER_POLL_INTERVAL = float(os.getenv("LEADER_POLL_INTERVAL", "0.25"))

# Copy size. 1.0 = same shares as leader, 0.10 = 10%, etc.
COPY_MULTIPLIER = float(os.getenv("COPY_MULTIPLIER", "1.0"))

# Paper bankroll. Set high for unrestricted 1:1 simulation.
INITIAL_BALANCE = float(os.getenv("INITIAL_BALANCE", "100000.0"))

# If true, the simulator refuses fills that exceed current virtual cash.
ENFORCE_BALANCE = os.getenv("ENFORCE_BALANCE", "false").lower() == "true"

# Simulated reaction time after detection. 0 means immediate.
EXTRA_EXECUTION_DELAY_MS = int(os.getenv("EXTRA_EXECUTION_DELAY_MS", "0"))

# If book is older than this at copy time, fetch a fresh REST snapshot.
MAX_BOOK_AGE_MS = int(os.getenv("MAX_BOOK_AGE_MS", "750"))

REPORT_DELAY_SECONDS = int(os.getenv("REPORT_DELAY_SECONDS", "300"))
REPORT_CHECK_INTERVAL = int(os.getenv("REPORT_CHECK_INTERVAL", "30"))

PORT = int(os.getenv("PORT", "8080"))

DATA_API = "https://data-api.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# Current official crypto taker feeRate.
CRYPTO_FEE_RATE = float(os.getenv("CRYPTO_FEE_RATE", "0.07"))

DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data"))
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    probe = DATA_DIR / ".write_test"
    probe.write_text("ok")
    probe.unlink()
except Exception:
    DATA_DIR = Path("./data")
    DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "copy_simulator.db"
REPORT_DIR = DATA_DIR / "copy_reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("copy-sim")

session: Optional[aiohttp.ClientSession] = None
ws_send_queue: asyncio.Queue = asyncio.Queue()
subscribed_assets = set()

# Full local books:
# books[token] = {
#   "bids": {price: size},
#   "asks": {price: size},
#   "received_ms": ...,
#   "exchange_ms": ...,
#   "condition_id": ...
# }
books = {}

# asset metadata discovered from leader trades / new market events
asset_meta = {}
market_assets = defaultdict(set)


# ============================================================
# HELPERS
# ============================================================

def now_ms():
    return int(time.time() * 1000)

def now_ts():
    return int(time.time())

def utc_iso_ms(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()

def utc_iso(ts=None):
    if ts is None:
        ts = time.time()
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()

def sf(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default

def si(v, default=0):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default

def jd(v):
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"))

def parse_jsonish(v):
    if isinstance(v, list):
        return v
    if v is None:
        return []
    try:
        x = json.loads(v)
        return x if isinstance(x, list) else []
    except Exception:
        return []

def leader_trade_uid(t):
    return "|".join([
        str(t.get("transactionHash", "")),
        str(t.get("timestamp", "")),
        str(t.get("asset", "")),
        str(t.get("side", "")),
        str(t.get("price", "")),
        str(t.get("size", "")),
        str(t.get("outcome", "")),
        str(t.get("conditionId", "")),
    ])

def symbol_from_trade(t):
    s = f"{t.get('title','')} {t.get('slug','')} {t.get('eventSlug','')}".lower()
    if "bitcoin" in s or "btc" in s:
        return "BTC"
    if "ethereum" in s or "eth" in s:
        return "ETH"
    return "OTHER"

def looks_like_target_market(t):
    s = f"{t.get('title','')} {t.get('slug','')} {t.get('eventSlug','')}".lower()
    return (
        ("bitcoin" in s or "btc" in s or "ethereum" in s or "eth" in s)
        and ("up or down" in s or "up-down" in s or "5m" in s or "5-min" in s)
    )

def crypto_taker_fee(shares, price):
    # Official formula: fee = C * feeRate * p * (1-p)
    fee = shares * CRYPTO_FEE_RATE * price * (1.0 - price)
    return round(fee, 5) if fee >= 0.000005 else 0.0


# ============================================================
# DATABASE
# ============================================================

def db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def init_db():
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS leader_trades (
            uid TEXT PRIMARY KEY,
            detected_ms INTEGER NOT NULL,
            leader_ts INTEGER NOT NULL,
            detection_delay_ms INTEGER,
            asset TEXT,
            condition_id TEXT,
            symbol TEXT,
            title TEXT,
            outcome TEXT,
            side TEXT,
            leader_price REAL,
            leader_size REAL,
            leader_notional REAL,
            transaction_hash TEXT,
            raw_json TEXT
        );

        CREATE TABLE IF NOT EXISTS copy_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            leader_uid TEXT UNIQUE,
            attempt_ms INTEGER NOT NULL,
            asset TEXT,
            condition_id TEXT,
            symbol TEXT,
            title TEXT,
            outcome TEXT,
            side TEXT,
            requested_shares REAL,
            filled_shares REAL,
            unfilled_shares REAL,
            leader_price REAL,
            copy_avg_price REAL,
            slippage_abs REAL,
            slippage_bps REAL,
            gross_notional REAL,
            taker_fee REAL,
            total_cash_change REAL,
            book_age_ms INTEGER,
            book_source TEXT,
            status TEXT,
            fills_json TEXT
        );

        CREATE TABLE IF NOT EXISTS resolutions (
            condition_id TEXT PRIMARY KEY,
            resolved_ms INTEGER,
            winning_asset TEXT,
            winning_outcome TEXT,
            raw_json TEXT
        );

        CREATE TABLE IF NOT EXISTS market_results (
            condition_id TEXT PRIMARY KEY,
            title TEXT,
            symbol TEXT,
            winning_asset TEXT,
            winning_outcome TEXT,
            total_cost REAL,
            total_fees REAL,
            payout REAL,
            realized_pnl REAL,
            copy_trade_count INTEGER,
            filled_shares REAL,
            settled_ms INTEGER
        );

        CREATE TABLE IF NOT EXISTS state (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS market_meta (
            condition_id TEXT PRIMARY KEY,
            slug TEXT,
            title TEXT,
            start_ts INTEGER,
            end_ts INTEGER,
            up_asset TEXT,
            down_asset TEXT,
            resolved INTEGER DEFAULT 0,
            winning_asset TEXT,
            winning_outcome TEXT
        );

        CREATE TABLE IF NOT EXISTS equity_snapshots (
            ts_ms INTEGER PRIMARY KEY,
            cash REAL,
            open_position_value REAL,
            equity REAL,
            realized_pnl REAL,
            unrealized_pnl REAL,
            open_markets INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_copy_condition ON copy_attempts(condition_id);
        CREATE INDEX IF NOT EXISTS idx_copy_attempt_ms ON copy_attempts(attempt_ms);
        """)

def state_get(key, default=None):
    with db() as conn:
        r = conn.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
        return r["value"] if r else default

def state_set(key, value):
    with db() as conn:
        conn.execute(
            "INSERT INTO state(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )
        conn.commit()

def cash_balance():
    return float(state_get("cash_balance", str(INITIAL_BALANCE)))

def set_cash_balance(v):
    state_set("cash_balance", f"{v:.10f}")


def slot_start_from_slug(slug):
    try:
        return int(str(slug).rstrip("/").split("-")[-1])
    except Exception:
        return None


def upsert_market_meta_from_trade(t):
    condition = str(t.get("conditionId", ""))
    if not condition:
        return

    slug = str(t.get("slug") or t.get("eventSlug") or "")
    title = str(t.get("title") or "")
    start_ts = slot_start_from_slug(slug)
    end_ts = start_ts + 300 if start_ts else None
    asset = str(t.get("asset") or "")
    outcome = str(t.get("outcome") or "").strip().lower()

    with db() as conn:
        existing = conn.execute(
            "SELECT * FROM market_meta WHERE condition_id=?",
            (condition,),
        ).fetchone()

        up_asset = existing["up_asset"] if existing else None
        down_asset = existing["down_asset"] if existing else None

        if outcome in ("up", "yes"):
            up_asset = asset
        elif outcome in ("down", "no"):
            down_asset = asset

        conn.execute("""
            INSERT INTO market_meta(
                condition_id, slug, title, start_ts, end_ts,
                up_asset, down_asset, resolved
            ) VALUES (?,?,?,?,?,?,?,0)
            ON CONFLICT(condition_id) DO UPDATE SET
                slug=COALESCE(NULLIF(excluded.slug,''), market_meta.slug),
                title=COALESCE(NULLIF(excluded.title,''), market_meta.title),
                start_ts=COALESCE(excluded.start_ts, market_meta.start_ts),
                end_ts=COALESCE(excluded.end_ts, market_meta.end_ts),
                up_asset=COALESCE(excluded.up_asset, market_meta.up_asset),
                down_asset=COALESCE(excluded.down_asset, market_meta.down_asset)
        """, (
            condition, slug, title, start_ts, end_ts,
            up_asset, down_asset,
        ))
        conn.commit()


def bootstrap_market_meta():
    """
    Rebuild market metadata from already-stored leader trades after upgrade,
    so existing open positions can still settle.
    """
    with db() as conn:
        rows = conn.execute(
            "SELECT condition_id, raw_json FROM leader_trades"
        ).fetchall()

    for r in rows:
        try:
            raw = json.loads(r["raw_json"])
        except Exception:
            continue
        upsert_market_meta_from_trade(raw)


def position_inventory():
    """
    Net simulated shares per asset/condition from executed copy attempts.
    BUY adds shares, SELL subtracts.
    Resolved markets are excluded from open inventory.
    """
    with db() as conn:
        rows = conn.execute("""
            SELECT
                c.condition_id,
                c.asset,
                c.outcome,
                SUM(
                    CASE
                        WHEN UPPER(c.side)='BUY' THEN c.filled_shares
                        WHEN UPPER(c.side)='SELL' THEN -c.filled_shares
                        ELSE 0
                    END
                ) AS net_shares
            FROM copy_attempts c
            LEFT JOIN market_meta m
              ON m.condition_id = c.condition_id
            WHERE COALESCE(m.resolved,0)=0
            GROUP BY c.condition_id, c.asset, c.outcome
            HAVING ABS(net_shares) > 0.0000001
        """).fetchall()

    return rows


async def portfolio_metrics(refresh_stale=False):
    """
    Conservative mark-to-market:
      long positions -> current best bid (liquidation value)
      short positions -> liability at current best ask
    """
    inv = position_inventory()
    value = 0.0
    open_conditions = set()

    for r in inv:
        asset = str(r["asset"])
        shares = sf(r["net_shares"])
        open_conditions.add(str(r["condition_id"]))

        if refresh_stale:
            await ensure_fresh_book(asset)

        b = books.get(asset)
        if not b:
            # Last-resort REST snapshot if we have no book at all.
            await fetch_book_rest(asset)
            b = books.get(asset)

        if not b:
            continue

        if shares >= 0:
            px = max(b["bids"]) if b["bids"] else 0.0
            value += shares * px
        else:
            px = min(b["asks"]) if b["asks"] else 1.0
            value += shares * px

    cash = cash_balance()

    with db() as conn:
        realized = sf(conn.execute(
            "SELECT COALESCE(SUM(realized_pnl),0) p FROM market_results"
        ).fetchone()["p"])

    equity = cash + value
    # Unrealized versus initial balance after removing realized component.
    unrealized = equity - INITIAL_BALANCE - realized

    return {
        "cash": cash,
        "open_position_value": value,
        "equity": equity,
        "realized_pnl": realized,
        "unrealized_pnl": unrealized,
        "open_markets": len(open_conditions),
    }


async def save_equity_snapshot():
    m = await portfolio_metrics(refresh_stale=False)
    with db() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO equity_snapshots(
                ts_ms, cash, open_position_value, equity,
                realized_pnl, unrealized_pnl, open_markets
            ) VALUES (?,?,?,?,?,?,?)
        """, (
            now_ms(), m["cash"], m["open_position_value"], m["equity"],
            m["realized_pnl"], m["unrealized_pnl"], m["open_markets"],
        ))
        conn.commit()
    return m


async def fetch_event_by_slug(slug):
    if not slug:
        return None

    data = await get_json(f"{GAMMA_API}/events/slug/{slug}")
    if isinstance(data, dict):
        return data

    data = await get_json(f"{GAMMA_API}/events", params={"slug": slug})
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return data[0]

    return None


def resolved_winner_from_event(event, condition_id):
    if not isinstance(event, dict):
        return None, None

    raw_markets = event.get("markets")
    if not isinstance(raw_markets, list):
        return None, None

    raw = None
    for m in raw_markets:
        if str(m.get("conditionId") or "") == str(condition_id):
            raw = m
            break

    if raw is None and len(raw_markets) == 1:
        raw = raw_markets[0]

    if not isinstance(raw, dict):
        return None, None

    outcomes = parse_jsonish(raw.get("outcomes"))
    tokens = parse_jsonish(raw.get("clobTokenIds"))
    prices = parse_jsonish(raw.get("outcomePrices"))

    if len(outcomes) >= 2 and len(tokens) >= 2 and len(prices) >= 2:
        p = [sf(x, -1) for x in prices]
        idx = max(range(len(p)), key=lambda i: p[i])
        other = max(p[i] for i in range(len(p)) if i != idx)

        if p[idx] >= 0.999 and other <= 0.001:
            return str(tokens[idx]), str(outcomes[idx])

    token_objs = raw.get("tokens")
    if isinstance(token_objs, list):
        for tok in token_objs:
            if isinstance(tok, dict) and bool(tok.get("winner", False)):
                asset = str(
                    tok.get("token_id")
                    or tok.get("tokenId")
                    or tok.get("id")
                    or ""
                )
                outcome = str(tok.get("outcome") or tok.get("name") or "")
                if asset:
                    return asset, outcome

    return None, None


async def settlement_fallback_loop():
    """
    Resolve ended 5m markets via the exact slug used by the market.
    This fixes the old simulator's '0 settled markets' problem.
    """
    while True:
        try:
            cutoff = now_ts() - 10

            with db() as conn:
                rows = conn.execute("""
                    SELECT condition_id, slug, end_ts
                    FROM market_meta
                    WHERE resolved=0
                      AND end_ts IS NOT NULL
                      AND end_ts < ?
                    ORDER BY end_ts
                    LIMIT 100
                """, (cutoff,)).fetchall()

            for r in rows:
                cid = str(r["condition_id"])
                slug = str(r["slug"] or "")

                event = await fetch_event_by_slug(slug)
                if not event:
                    continue

                winning_asset, winning_outcome = resolved_winner_from_event(event, cid)
                if not winning_asset:
                    continue

                log.info(
                    "SETTLEMENT FALLBACK %s | winner=%s",
                    slug,
                    winning_outcome or winning_asset[-8:],
                )

                settle_market(cid, winning_asset, winning_outcome, now_ms())

                with db() as conn:
                    conn.execute("""
                        UPDATE market_meta
                        SET resolved=1,
                            winning_asset=?,
                            winning_outcome=?
                        WHERE condition_id=?
                    """, (winning_asset, winning_outcome, cid))
                    conn.commit()

        except Exception:
            log.exception("Settlement fallback failed")

        await asyncio.sleep(10)


# ============================================================
# HTTP
# ============================================================

async def get_json(url, params=None):
    for attempt in range(3):
        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                txt = await r.text()
                if r.status == 200:
                    return json.loads(txt)
                log.warning("HTTP %s %s %s -> %s", r.status, url, params, txt[:300])
        except Exception as e:
            log.warning("HTTP GET error %s: %s", url, e)
        if attempt < 2:
            await asyncio.sleep(0.05 * (attempt + 1))
    return None

async def fetch_book_rest(asset):
    data = await get_json(f"{CLOB_API}/book", params={"token_id": asset})
    if not isinstance(data, dict):
        return False
    apply_full_book(asset, data, "rest")
    return True


# ============================================================
# ORDER BOOK
# ============================================================

def level_map(rows):
    out = {}
    for x in rows or []:
        if not isinstance(x, dict):
            continue
        p = sf(x.get("price"), math.nan)
        s = sf(x.get("size"), 0)
        if not math.isnan(p) and s > 0:
            out[p] = s
    return out

def apply_full_book(asset, payload, source="ws"):
    recv = now_ms()
    ex = si(payload.get("timestamp"), 0) or None
    books[asset] = {
        "bids": level_map(payload.get("bids")),
        "asks": level_map(payload.get("asks")),
        "received_ms": recv,
        "exchange_ms": ex,
        "condition_id": str(payload.get("market") or payload.get("condition_id") or ""),
        "source": source,
    }

def apply_price_changes(payload):
    recv = now_ms()
    ex = si(payload.get("timestamp"), 0) or None
    market = str(payload.get("market") or "")
    changes = payload.get("price_changes") or payload.get("priceChanges") or []

    for ch in changes:
        if not isinstance(ch, dict):
            continue
        asset = str(ch.get("asset_id") or ch.get("token_id") or ch.get("tokenId") or "")
        if not asset:
            continue
        b = books.setdefault(asset, {
            "bids": {},
            "asks": {},
            "received_ms": recv,
            "exchange_ms": ex,
            "condition_id": market,
            "source": "ws-delta",
        })

        price = sf(ch.get("price"), math.nan)
        size = sf(ch.get("size"), 0)
        side = str(ch.get("side", "")).upper()

        if math.isnan(price):
            continue

        # CLOB market channel side refers to resting order side.
        target = b["bids"] if side == "BUY" else b["asks"]

        if size <= 0:
            target.pop(price, None)
        else:
            target[price] = size

        b["received_ms"] = recv
        b["exchange_ms"] = ex
        b["condition_id"] = market or b.get("condition_id", "")
        b["source"] = "ws"

def simulate_buy_from_asks(asset, wanted_shares):
    b = books.get(asset)
    if not b:
        return [], 0.0, wanted_shares

    remaining = wanted_shares
    fills = []

    for price in sorted(b["asks"].keys()):
        available = b["asks"][price]
        if available <= 0:
            continue
        take = min(remaining, available)
        if take <= 0:
            break
        fills.append((price, take))
        remaining -= take
        if remaining <= 1e-12:
            break

    filled = wanted_shares - remaining
    return fills, filled, remaining

def simulate_sell_to_bids(asset, wanted_shares):
    b = books.get(asset)
    if not b:
        return [], 0.0, wanted_shares

    remaining = wanted_shares
    fills = []

    for price in sorted(b["bids"].keys(), reverse=True):
        available = b["bids"][price]
        if available <= 0:
            continue
        take = min(remaining, available)
        if take <= 0:
            break
        fills.append((price, take))
        remaining -= take
        if remaining <= 1e-12:
            break

    filled = wanted_shares - remaining
    return fills, filled, remaining

async def ensure_fresh_book(asset):
    b = books.get(asset)
    if b is not None:
        age = now_ms() - b["received_ms"]
        if age <= MAX_BOOK_AGE_MS and (b["asks"] or b["bids"]):
            return age, b.get("source", "ws")

    ok = await fetch_book_rest(asset)
    b = books.get(asset)
    if ok and b:
        return now_ms() - b["received_ms"], "rest"

    return None, "missing"


# ============================================================
# WS SUBSCRIPTIONS
# ============================================================

async def subscribe_asset(asset):
    if not asset or asset in subscribed_assets:
        return
    subscribed_assets.add(asset)
    await ws_send_queue.put({
        "operation": "subscribe",
        "assets_ids": [asset],
    })

async def seed_recent_assets():
    # Seed from recent public leader trades so WS can connect immediately.
    rows = await get_json(
        f"{DATA_API}/trades",
        params={
            "user": LEADER_WALLET,
            "limit": 500,
            "offset": 0,
            "takerOnly": "false",
            "start": now_ts() - 3600,
            "end": now_ts() + 5,
        },
    )

    if not isinstance(rows, list):
        return

    for t in rows:
        if not looks_like_target_market(t):
            continue
        asset = str(t.get("asset", ""))
        condition = str(t.get("conditionId", ""))
        if asset:
            subscribed_assets.add(asset)
            market_assets[condition].add(asset)
            asset_meta[asset] = {
                "condition_id": condition,
                "title": str(t.get("title", "")),
                "outcome": str(t.get("outcome", "")),
                "symbol": symbol_from_trade(t),
            }

    log.info("Seeded %d assets", len(subscribed_assets))

def new_market_is_target(event):
    q = str(event.get("question") or event.get("title") or "").lower()
    slug = str(event.get("slug") or "").lower()
    s = q + " " + slug
    return (
        ("bitcoin" in s or "btc" in s or "ethereum" in s or "eth" in s)
        and ("up or down" in s or "up-down" in s or "5m" in s or "5-min" in s)
    )

async def handle_new_market(event):
    if not new_market_is_target(event):
        return

    assets = event.get("assets_ids") or event.get("clob_token_ids") or []
    outcomes = event.get("outcomes") or []
    condition = str(event.get("condition_id") or event.get("market") or "")
    title = str(event.get("question") or event.get("title") or "")
    symbol = "BTC" if ("bitcoin" in title.lower() or "btc" in title.lower()) else "ETH"

    for i, asset in enumerate(assets):
        asset = str(asset)
        outcome = str(outcomes[i]) if i < len(outcomes) else ""
        asset_meta[asset] = {
            "condition_id": condition,
            "title": title,
            "outcome": outcome,
            "symbol": symbol,
        }
        market_assets[condition].add(asset)
        await subscribe_asset(asset)

    log.info("Pre-subscribed new %s market: %s", symbol, title)

def parse_ws(raw):
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "ignore")
    if raw in ("PING", "PONG", ""):
        return []
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, list) else [obj]
    except Exception:
        return []

async def ws_heartbeat(ws):
    while True:
        try:
            await ws.send("PING")
        except Exception:
            return
        await asyncio.sleep(10)

async def ws_sender(ws):
    while True:
        msg = await ws_send_queue.get()
        try:
            await ws.send(jd(msg))
        except Exception:
            await ws_send_queue.put(msg)
            return

async def market_ws():
    await seed_recent_assets()

    while True:
        try:
            if not subscribed_assets:
                # Wait until seed or leader poll discovers first asset.
                await asyncio.sleep(0.5)
                continue

            async with websockets.connect(
                MARKET_WS,
                ping_interval=None,
                close_timeout=5,
                max_size=20_000_000,
            ) as ws:
                await ws.send(jd({
                    "assets_ids": list(subscribed_assets),
                    "type": "market",
                    "custom_feature_enabled": True,
                }))
                log.info("Market WS connected; assets=%d", len(subscribed_assets))

                hb = asyncio.create_task(ws_heartbeat(ws))
                sender = asyncio.create_task(ws_sender(ws))

                try:
                    async for raw in ws:
                        for ev in parse_ws(raw):
                            if not isinstance(ev, dict):
                                continue

                            et = str(ev.get("event_type") or ev.get("type") or "")
                            payload = ev.get("payload") if isinstance(ev.get("payload"), dict) else ev

                            if et == "book":
                                asset = str(
                                    payload.get("asset_id")
                                    or payload.get("token_id")
                                    or payload.get("tokenId")
                                    or ""
                                )
                                if asset:
                                    apply_full_book(asset, payload, "ws")

                            elif et == "price_change":
                                apply_price_changes(payload)

                            elif et == "new_market":
                                await handle_new_market(payload)

                            elif et == "market_resolved":
                                handle_resolution(payload)
                finally:
                    hb.cancel()
                    sender.cancel()

        except Exception as e:
            log.warning("Market WS reconnect: %s", e)
            await asyncio.sleep(1)


# ============================================================
# PAPER EXECUTION
# ============================================================

def store_leader_trade(t, detected_ms):
    uid = leader_trade_uid(t)
    upsert_market_meta_from_trade(t)
    leader_ts = si(t.get("timestamp"))
    delay = detected_ms - leader_ts * 1000 if leader_ts else None

    with db() as conn:
        cur = conn.execute("""
            INSERT OR IGNORE INTO leader_trades(
                uid, detected_ms, leader_ts, detection_delay_ms,
                asset, condition_id, symbol, title, outcome, side,
                leader_price, leader_size, leader_notional,
                transaction_hash, raw_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            uid,
            detected_ms,
            leader_ts,
            delay,
            str(t.get("asset", "")),
            str(t.get("conditionId", "")),
            symbol_from_trade(t),
            str(t.get("title", "")),
            str(t.get("outcome", "")),
            str(t.get("side", "")).upper(),
            sf(t.get("price")),
            sf(t.get("size")),
            sf(t.get("price")) * sf(t.get("size")),
            str(t.get("transactionHash", "")),
            jd(t),
        ))
        conn.commit()
        return cur.rowcount > 0, uid

async def paper_copy_trade(t, uid, detected_ms):
    asset = str(t.get("asset", ""))
    condition = str(t.get("conditionId", ""))
    title = str(t.get("title", ""))
    outcome = str(t.get("outcome", ""))
    symbol = symbol_from_trade(t)
    side = str(t.get("side", "")).upper()
    leader_price = sf(t.get("price"))
    leader_size = sf(t.get("size"))
    requested = max(0.0, leader_size * COPY_MULTIPLIER)

    asset_meta[asset] = {
        "condition_id": condition,
        "title": title,
        "outcome": outcome,
        "symbol": symbol,
    }
    market_assets[condition].add(asset)

    await subscribe_asset(asset)

    if EXTRA_EXECUTION_DELAY_MS > 0:
        await asyncio.sleep(EXTRA_EXECUTION_DELAY_MS / 1000)

    book_age, source = await ensure_fresh_book(asset)

    if requested <= 0:
        status = "ZERO_SIZE"
        fills = []
        filled = 0.0
        unfilled = requested
    elif side == "BUY":
        fills, filled, unfilled = simulate_buy_from_asks(asset, requested)
        status = "FULL" if unfilled <= 1e-9 else ("PARTIAL" if filled > 0 else "NO_LIQUIDITY")
    elif side == "SELL":
        fills, filled, unfilled = simulate_sell_to_bids(asset, requested)
        status = "FULL" if unfilled <= 1e-9 else ("PARTIAL" if filled > 0 else "NO_LIQUIDITY")
    else:
        fills, filled, unfilled = [], 0.0, requested
        status = "UNSUPPORTED_SIDE"

    gross = sum(p * q for p, q in fills)
    avg = gross / filled if filled > 0 else None
    fee = sum(crypto_taker_fee(q, p) for p, q in fills)

    # For BUY cash decreases by notional+fee. For SELL cash increases by notional-fee.
    cash_change = -(gross + fee) if side == "BUY" else (gross - fee)

    # Optional cash constraint.
    if ENFORCE_BALANCE and side == "BUY" and filled > 0:
        available = cash_balance()
        needed = gross + fee

        if needed > available + 1e-9:
            # Re-simulate only as much as can be afforded, level by level.
            affordable_fills = []
            cash_left = available
            shares_done = 0.0

            for p, q in fills:
                # approximate max affordable shares at this level including fee/share
                fee_per_share = CRYPTO_FEE_RATE * p * (1 - p)
                unit = p + fee_per_share
                maxq = cash_left / unit if unit > 0 else 0
                take = min(q, maxq)
                if take <= 1e-12:
                    break
                affordable_fills.append((p, take))
                level_fee = crypto_taker_fee(take, p)
                cash_left -= p * take + level_fee
                shares_done += take

            fills = affordable_fills
            filled = shares_done
            unfilled = max(0.0, requested - filled)
            gross = sum(p * q for p, q in fills)
            avg = gross / filled if filled > 0 else None
            fee = sum(crypto_taker_fee(q, p) for p, q in fills)
            cash_change = -(gross + fee)
            status = "BALANCE_PARTIAL" if filled > 0 else "NO_BALANCE"

    slippage_abs = None
    slippage_bps = None
    if avg is not None and leader_price > 0:
        if side == "BUY":
            slippage_abs = avg - leader_price
        else:
            slippage_abs = leader_price - avg
        slippage_bps = slippage_abs / leader_price * 10000

    with db() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO copy_attempts(
                leader_uid, attempt_ms, asset, condition_id, symbol,
                title, outcome, side, requested_shares, filled_shares,
                unfilled_shares, leader_price, copy_avg_price,
                slippage_abs, slippage_bps, gross_notional, taker_fee,
                total_cash_change, book_age_ms, book_source, status,
                fills_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            uid,
            now_ms(),
            asset,
            condition,
            symbol,
            title,
            outcome,
            side,
            requested,
            filled,
            unfilled,
            leader_price,
            avg,
            slippage_abs,
            slippage_bps,
            gross,
            fee,
            cash_change,
            book_age,
            source,
            status,
            jd([{"price": p, "shares": q} for p, q in fills]),
        ))
        conn.commit()

    if cash_change:
        set_cash_balance(cash_balance() + cash_change)

    log.info(
        "COPY %s %s %.4fsh leader=%.4f copy=%s slip=%s fee=%.5f status=%s",
        side,
        outcome,
        filled,
        leader_price,
        f"{avg:.4f}" if avg is not None else "-",
        f"{slippage_abs:+.4f}" if slippage_abs is not None else "-",
        fee,
        status,
    )


# ============================================================
# FAST LEADER POLLING
# ============================================================

async def leader_poller():
    log.info(
        "Fast leader poller started: %.3fs interval | wallet=%s",
        LEADER_POLL_INTERVAL,
        LEADER_WALLET,
    )

    # Ignore historical fills on first launch: use current second minus 3
    # for a tiny overlap, but mark anything older than launch as baseline.
    launch_ms = now_ms()
    last_ts = si(state_get("last_leader_ts", "0"))
    if last_ts <= 0:
        last_ts = now_ts() - 3

    while True:
        started = time.perf_counter()
        try:
            rows = await get_json(
                f"{DATA_API}/trades",
                params={
                    "user": LEADER_WALLET,
                    "limit": 100,
                    "offset": 0,
                    "takerOnly": "false",
                    "start": max(1, last_ts - 2),
                    "end": now_ts() + 2,
                },
            )

            if isinstance(rows, list):
                rows.sort(key=lambda x: si(x.get("timestamp")))
                max_ts = last_ts

                for t in rows:
                    if str(t.get("proxyWallet", "")).lower() not in ("", LEADER_WALLET):
                        continue

                    ts = si(t.get("timestamp"))
                    max_ts = max(max_ts, ts)

                    # We focus on crypto Up/Down markets.
                    if not looks_like_target_market(t):
                        continue

                    detected = now_ms()
                    inserted, uid = store_leader_trade(t, detected)

                    if not inserted:
                        continue

                    # On brand-new DB, don't paper-copy stale baseline trades.
                    if detected - ts * 1000 > 15_000 and detected < launch_ms + 10_000:
                        log.info("Baseline leader trade stored, not copied")
                        continue

                    await paper_copy_trade(t, uid, detected)

                if max_ts > 0:
                    last_ts = max_ts
                    state_set("last_leader_ts", last_ts)

        except Exception:
            log.exception("Leader poller error")

        elapsed = time.perf_counter() - started
        await asyncio.sleep(max(0.0, LEADER_POLL_INTERVAL - elapsed))


# ============================================================
# RESOLUTION / PNL
# ============================================================

def handle_resolution(ev):
    condition = str(ev.get("market") or ev.get("condition_id") or "")
    winning_asset = str(ev.get("winning_asset_id") or "")
    winning_outcome = str(ev.get("winning_outcome") or "")
    resolved_ms = si(ev.get("timestamp"), 0) or now_ms()

    if not condition:
        return

    with db() as conn:
        conn.execute("""
            INSERT INTO resolutions(
                condition_id, resolved_ms, winning_asset,
                winning_outcome, raw_json
            ) VALUES (?,?,?,?,?)
            ON CONFLICT(condition_id) DO UPDATE SET
                resolved_ms=excluded.resolved_ms,
                winning_asset=excluded.winning_asset,
                winning_outcome=excluded.winning_outcome,
                raw_json=excluded.raw_json
        """, (condition, resolved_ms, winning_asset, winning_outcome, jd(ev)))
        conn.commit()

    settle_market(condition, winning_asset, winning_outcome, resolved_ms)

    with db() as conn:
        conn.execute("""
            UPDATE market_meta
            SET resolved=1,
                winning_asset=?,
                winning_outcome=?
            WHERE condition_id=?
        """, (winning_asset, winning_outcome, condition))
        conn.commit()

def settle_market(condition, winning_asset, winning_outcome, resolved_ms):
    with db() as conn:
        attempts = conn.execute("""
            SELECT * FROM copy_attempts
            WHERE condition_id=?
            ORDER BY attempt_ms
        """, (condition,)).fetchall()

        if not attempts:
            return

        # Current leader data is overwhelmingly BUY-only. For completeness:
        # BUY winning shares pay $1 each. SELL cash flow is already recorded;
        # short settlement is not modeled because selling requires inventory.
        buys = [r for r in attempts if r["side"] == "BUY"]

        total_cost = sum(
            sf(r["gross_notional"]) + sf(r["taker_fee"])
            for r in buys
        )
        total_fees = sum(sf(r["taker_fee"]) for r in attempts)

        payout = sum(
            sf(r["filled_shares"])
            for r in buys
            if str(r["asset"]) == winning_asset
        )

        pnl = payout - total_cost
        title = str(attempts[0]["title"])
        symbol = str(attempts[0]["symbol"])
        count = len(attempts)
        filled_shares = sum(sf(r["filled_shares"]) for r in attempts)

        conn.execute("""
            INSERT INTO market_results(
                condition_id, title, symbol, winning_asset, winning_outcome,
                total_cost, total_fees, payout, realized_pnl,
                copy_trade_count, filled_shares, settled_ms
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(condition_id) DO UPDATE SET
                winning_asset=excluded.winning_asset,
                winning_outcome=excluded.winning_outcome,
                total_cost=excluded.total_cost,
                total_fees=excluded.total_fees,
                payout=excluded.payout,
                realized_pnl=excluded.realized_pnl,
                copy_trade_count=excluded.copy_trade_count,
                filled_shares=excluded.filled_shares,
                settled_ms=excluded.settled_ms
        """, (
            condition, title, symbol, winning_asset, winning_outcome,
            total_cost, total_fees, payout, pnl,
            count, filled_shares, resolved_ms,
        ))
        conn.commit()

    # Credit settlement payout to virtual cash.
    if payout > 0:
        set_cash_balance(cash_balance() + payout)

    log.info(
        "SETTLED %s pnl=%+.2f cost=%.2f payout=%.2f",
        title,
        pnl,
        total_cost,
        payout,
    )


# ============================================================
# TELEGRAM REPORT
# ============================================================

def csv_bytes(rows, columns=None):
    s = io.StringIO()
    if rows:
        if columns is None:
            columns = list(rows[0].keys())
        w = csv.DictWriter(s, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(dict(r))
    elif columns:
        w = csv.DictWriter(s, fieldnames=columns)
        w.writeheader()
    return s.getvalue().encode("utf-8-sig")

async def tg_file(path, caption):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured; report at %s", path)
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"

    try:
        form = aiohttp.FormData()
        form.add_field("chat_id", TELEGRAM_CHAT_ID)
        form.add_field("caption", caption[:1024])
        form.add_field(
            "document",
            path.read_bytes(),
            filename=path.name,
            content_type="application/zip",
        )

        async with session.post(
            url,
            data=form,
            timeout=aiohttp.ClientTimeout(total=120),
        ) as r:
            if r.status != 200:
                log.warning("Telegram error: %s", await r.text())
                return False
            return True
    except Exception:
        log.exception("Telegram send failed")
        return False

async def make_hour_report(start_ts, end_ts):
    start_ms = start_ts * 1000
    end_ms = end_ts * 1000

    with db() as conn:
        leaders = conn.execute("""
            SELECT * FROM leader_trades
            WHERE detected_ms >= ? AND detected_ms < ?
            ORDER BY detected_ms
        """, (start_ms, end_ms)).fetchall()

        copies = conn.execute("""
            SELECT * FROM copy_attempts
            WHERE attempt_ms >= ? AND attempt_ms < ?
            ORDER BY attempt_ms
        """, (start_ms, end_ms)).fetchall()

        # Attribute settled markets by actual market end time where possible.
        results = conn.execute("""
            SELECT mr.*
            FROM market_results mr
            LEFT JOIN market_meta mm
              ON mm.condition_id = mr.condition_id
            WHERE (
                mm.end_ts IS NOT NULL
                AND (mm.end_ts * 1000) >= ?
                AND (mm.end_ts * 1000) < ?
            )
            OR (
                mm.end_ts IS NULL
                AND mr.settled_ms >= ?
                AND mr.settled_ms < ?
            )
            ORDER BY mr.settled_ms
        """, (start_ms, end_ms, start_ms, end_ms)).fetchall()

    metrics = await save_equity_snapshot()

    detected_delays = [
        sf(r["detection_delay_ms"])
        for r in leaders
        if r["detection_delay_ms"] is not None
    ]
    slips = [
        sf(r["slippage_abs"])
        for r in copies
        if r["slippage_abs"] is not None
    ]

    total_leader_notional = sum(sf(r["leader_notional"]) for r in leaders)
    total_copy_notional = sum(sf(r["gross_notional"]) for r in copies)
    total_fees = sum(sf(r["taker_fee"]) for r in copies)
    realized_hour = sum(sf(r["realized_pnl"]) for r in results)

    full = sum(1 for r in copies if r["status"] == "FULL")
    partial = sum(1 for r in copies if "PARTIAL" in str(r["status"]))
    missed = len(copies) - full - partial

    avg_delay = sum(detected_delays) / len(detected_delays) if detected_delays else 0
    avg_slip = sum(slips) / len(slips) if slips else 0

    text_report = "\n".join([
        "COPY SIMULATOR v2 - EQUITY ACCOUNTING",
        "=" * 60,
        f"Wallet: {LEADER_WALLET}",
        f"Period UTC: {utc_iso(start_ts)} -> {utc_iso(end_ts)}",
        f"Copy multiplier: {COPY_MULTIPLIER}",
        f"Leader poll interval: {LEADER_POLL_INTERVAL:.3f}s",
        f"Extra simulated execution delay: {EXTRA_EXECUTION_DELAY_MS} ms",
        "",
        f"Leader trades detected: {len(leaders)}",
        f"Copy attempts: {len(copies)}",
        f"FULL: {full} | PARTIAL: {partial} | OTHER/MISSED: {missed}",
        f"Leader notional observed: ${total_leader_notional:.2f}",
        f"Copy executed notional: ${total_copy_notional:.2f}",
        f"Taker fees charged this hour: ${total_fees:.4f}",
        f"Average detection delay*: {avg_delay:.0f} ms",
        f"Average execution slippage vs leader: {avg_slip:+.5f}",
        "",
        "PORTFOLIO",
        f"Cash: ${metrics['cash']:.2f}",
        f"Open position liquidation value: ${metrics['open_position_value']:.2f}",
        f"EQUITY: ${metrics['equity']:.2f}",
        f"Total return vs initial ${INITIAL_BALANCE:.2f}: ${metrics['equity']-INITIAL_BALANCE:+.2f}",
        f"Realized PnL total: ${metrics['realized_pnl']:+.2f}",
        f"Unrealized PnL estimate: ${metrics['unrealized_pnl']:+.2f}",
        f"Open markets: {metrics['open_markets']}",
        "",
        f"Markets settled in this hour: {len(results)}",
        f"Realized PnL from markets ending this hour: ${realized_hour:+.2f}",
        "",
        "* Public Data API timestamps are second-resolution.",
        "Open position value uses best bid for long positions (conservative liquidation mark).",
        "",
        "FILES",
        "leader_trades.csv  - leader trades as detected",
        "copy_attempts.csv  - simulated executions and slippage",
        "market_results.csv - resolved market PnL",
        "portfolio.csv      - cash / position value / equity snapshot",
        "report.txt         - this summary",
    ])

    dt1 = datetime.fromtimestamp(start_ts, tz=timezone.utc)
    dt2 = datetime.fromtimestamp(end_ts, tz=timezone.utc)
    path = REPORT_DIR / f"copy_sim_{dt1:%Y-%m-%d_%H-%M}_{dt2:%H-%M}_UTC.zip"

    portfolio_row = [{
        "timestamp_utc": utc_iso(),
        **metrics,
        "initial_balance": INITIAL_BALANCE,
        "total_return": metrics["equity"] - INITIAL_BALANCE,
    }]

    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("leader_trades.csv", csv_bytes(leaders))
        z.writestr("copy_attempts.csv", csv_bytes(copies))
        z.writestr("market_results.csv", csv_bytes(results))
        z.writestr("portfolio.csv", csv_bytes(portfolio_row))
        z.writestr("report.txt", text_report.encode("utf-8"))

    return path, len(copies), realized_hour, metrics

async def reporter():
    saved = si(state_get("last_report_end", "0"))

    if saved <= 0:
        d = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        saved = int(d.timestamp())
        state_set("last_report_end", saved)

    last_end = saved

    while True:
        try:
            eligible = ((now_ts() - REPORT_DELAY_SECONDS) // 3600) * 3600

            while last_end < eligible:
                start = last_end
                end = start + 3600

                path, n, pnl, metrics = await make_hour_report(start, end)

                ok = await tg_file(
                    path,
                    (
                        "📊 Powerwinner Paper Copy\n"
                        f"{utc_iso(start)} → {utc_iso(end)}\n"
                        f"Copy attempts: {n}\n"
                        f"Settled PnL this hour: ${pnl:+.2f}\n"
                        f"Cash: ${metrics['cash']:.2f}\n"
                        f"Equity: ${metrics['equity']:.2f}\n"
                        f"Open positions: ${metrics['open_position_value']:.2f}"
                    ),
                )

                if not ok:
                    break

                last_end = end
                state_set("last_report_end", last_end)

        except Exception:
            log.exception("Reporter failed")

        await asyncio.sleep(REPORT_CHECK_INTERVAL)


# ============================================================
# HEALTH
# ============================================================

async def health(request):
    with db() as conn:
        lt = conn.execute("SELECT COUNT(*) c FROM leader_trades").fetchone()["c"]
        cp = conn.execute("SELECT COUNT(*) c FROM copy_attempts").fetchone()["c"]
        rs = conn.execute("SELECT COUNT(*) c FROM market_results").fetchone()["c"]

    metrics = await portfolio_metrics(refresh_stale=False)

    return web.json_response({
        "ok": True,
        "version": "2.0-equity",
        "leader_wallet": LEADER_WALLET,
        "poll_interval_s": LEADER_POLL_INTERVAL,
        "copy_multiplier": COPY_MULTIPLIER,
        "enforce_balance": ENFORCE_BALANCE,
        "cash": metrics["cash"],
        "open_position_value": metrics["open_position_value"],
        "equity": metrics["equity"],
        "total_return": metrics["equity"] - INITIAL_BALANCE,
        "realized_pnl": metrics["realized_pnl"],
        "unrealized_pnl": metrics["unrealized_pnl"],
        "open_markets": metrics["open_markets"],
        "leader_trades": lt,
        "copy_attempts": cp,
        "settled_markets": rs,
        "ws_assets": len(subscribed_assets),
        "books": len(books),
        "time_utc": utc_iso(),
    })

async def web_server():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info("Health server on :%d", PORT)


# ============================================================
# MAIN
# ============================================================

async def main():
    global session

    init_db()
    bootstrap_market_meta()

    if state_get("cash_balance") is None:
        set_cash_balance(INITIAL_BALANCE)

    session = aiohttp.ClientSession(headers={
        "User-Agent": "PowerwinnerPaperCopySimulator/1.0",
        "Accept": "application/json",
    })

    tasks = [
        asyncio.create_task(web_server()),
        asyncio.create_task(market_ws()),
        asyncio.create_task(leader_poller()),
        asyncio.create_task(settlement_fallback_loop()),
        asyncio.create_task(reporter()),
    ]

    log.info(
        "Copy Simulator v2 EQUITY started | wallet=%s | poll=%.3fs | multiplier=%.3f | cash=%.2f",
        LEADER_WALLET,
        LEADER_POLL_INTERVAL,
        COPY_MULTIPLIER,
        cash_balance(),
    )

    try:
        await asyncio.gather(*tasks)
    finally:
        for t in tasks:
            t.cancel()
        if session:
            await session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
