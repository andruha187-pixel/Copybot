import os
import io
import csv
import json
import time
import math
import sqlite3
import asyncio
import zipfile
import logging
from pathlib import Path
from datetime import datetime, timezone
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
    "0xf3531b23b504cf0aed4ff21325232b2a2d496685",
).lower()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# Public Data API polling. 0.25s is deliberately below the documented
# /trades rate limit and avoids the 429 bursts we saw at 0.10s on Render.
LEADER_POLL_INTERVAL = float(os.getenv("LEADER_POLL_INTERVAL", "0.25"))

# Fixed all-in paper budget per signal variant.
PAPER_BUDGET_USD = float(os.getenv("PAPER_BUDGET_USD", "10.0"))

# Signal thresholds reconstructed from our Powerwinner research.
SIGNAL_THRESHOLDS = [60, 75, 90]
EXECUTION_DELAYS = [0, 1, 3, 5, 10]

# Crypto taker fee model from Polymarket docs.
CRYPTO_FEE_RATE = float(os.getenv("CRYPTO_FEE_RATE", "0.07"))

DISCOVERY_INTERVAL = float(os.getenv("DISCOVERY_INTERVAL", "5"))
MAX_BOOK_AGE_MS = int(os.getenv("MAX_BOOK_AGE_MS", "750"))

REPORT_DELAY_SECONDS = int(os.getenv("REPORT_DELAY_SECONDS", "300"))
REPORT_CHECK_INTERVAL = int(os.getenv("REPORT_CHECK_INTERVAL", "30"))

PORT = int(os.getenv("PORT", "8080"))

DATA_API = "https://data-api.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data"))
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    probe = DATA_DIR / ".write_test"
    probe.write_text("ok")
    probe.unlink()
except Exception:
    DATA_DIR = Path("./data")
    DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "late_signal.db"
REPORT_DIR = DATA_DIR / "late_signal_reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("late-signal")

session: Optional[aiohttp.ClientSession] = None

# market state
markets = {}                    # condition_id -> metadata
asset_to_market = {}            # token -> condition_id
books = {}                      # token -> full local book
subscribed_assets = set()
ws_send_queue: asyncio.Queue = asyncio.Queue()

# The first observed leader BUY per market is the only directional signal.
first_leader_buy = {}           # condition_id -> signal dict
signal_tasks = set()

# ============================================================
# HELPERS
# ============================================================

def now_ts():
    return int(time.time())

def now_ms():
    return int(time.time() * 1000)

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

def trade_uid(t):
    return "|".join([
        str(t.get("transactionHash", "")),
        str(t.get("timestamp", "")),
        str(t.get("asset", "")),
        str(t.get("side", "")),
        str(t.get("price", "")),
        str(t.get("size", "")),
        str(t.get("conditionId", "")),
    ])

def fee_for(shares, price):
    fee = shares * CRYPTO_FEE_RATE * price * (1.0 - price)
    return round(fee, 5) if fee >= 0.000005 else 0.0

def slot_start_from_slug(slug):
    try:
        return int(str(slug).rstrip("/").split("-")[-1])
    except Exception:
        return None

def is_btc_5m_trade(t):
    s = f"{t.get('title','')} {t.get('slug','')} {t.get('eventSlug','')}".lower()
    return (
        ("bitcoin" in s or "btc" in s)
        and ("up or down" in s or "up-down" in s or "btc-updown-5m" in s)
    )

# ============================================================
# DB
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
        CREATE TABLE IF NOT EXISTS markets (
            condition_id TEXT PRIMARY KEY,
            slug TEXT,
            title TEXT,
            start_ts INTEGER,
            end_ts INTEGER,
            up_asset TEXT,
            down_asset TEXT,
            resolved INTEGER DEFAULT 0,
            winning_asset TEXT,
            winning_outcome TEXT,
            discovered_ms INTEGER
        );

        CREATE TABLE IF NOT EXISTS seen_leader_trades (
            uid TEXT PRIMARY KEY,
            leader_ts INTEGER,
            detected_ms INTEGER,
            condition_id TEXT,
            asset TEXT,
            outcome TEXT,
            side TEXT,
            price REAL,
            size REAL,
            raw_json TEXT
        );

        CREATE TABLE IF NOT EXISTS signals (
            condition_id TEXT PRIMARY KEY,
            leader_uid TEXT,
            leader_ts INTEGER,
            detected_ms INTEGER,
            detection_delay_ms INTEGER,
            elapsed_sec REAL,
            asset TEXT,
            outcome TEXT,
            leader_price REAL,
            leader_size REAL,
            best_bid REAL,
            best_ask REAL,
            spread REAL,
            maker_class TEXT,
            market_slug TEXT,
            title TEXT
        );

        CREATE TABLE IF NOT EXISTS executions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            condition_id TEXT,
            threshold_sec INTEGER,
            maker_filter TEXT,
            delay_sec INTEGER,
            eligible INTEGER,
            attempted_ms INTEGER,
            asset TEXT,
            outcome TEXT,
            maker_class TEXT,
            requested_budget REAL,
            filled_shares REAL,
            avg_price REAL,
            gross_cost REAL,
            fee REAL,
            total_cost REAL,
            unspent_budget REAL,
            best_bid REAL,
            best_ask REAL,
            book_age_ms INTEGER,
            status TEXT,
            fills_json TEXT,
            UNIQUE(condition_id, threshold_sec, maker_filter, delay_sec)
        );

        CREATE TABLE IF NOT EXISTS results (
            execution_id INTEGER PRIMARY KEY,
            condition_id TEXT,
            threshold_sec INTEGER,
            maker_filter TEXT,
            delay_sec INTEGER,
            winning_asset TEXT,
            winning_outcome TEXT,
            won INTEGER,
            payout REAL,
            pnl REAL,
            roi_pct REAL,
            settled_ms INTEGER
        );

        CREATE TABLE IF NOT EXISTS state (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_exec_condition ON executions(condition_id);
        CREATE INDEX IF NOT EXISTS idx_exec_time ON executions(attempted_ms);
        CREATE INDEX IF NOT EXISTS idx_results_condition ON results(condition_id);
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

# ============================================================
# HTTP
# ============================================================

async def get_json(url, params=None):
    for attempt in range(3):
        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=12),
            ) as r:
                text = await r.text()
                if r.status == 200:
                    return json.loads(text)

                if r.status == 429:
                    # Respect server throttling; don't hammer with immediate retry.
                    await asyncio.sleep(0.6 * (attempt + 1))
                    continue

                log.warning("HTTP %s %s -> %s", r.status, url, text[:250])
        except Exception as e:
            log.warning("GET %s failed: %s", url, e)

        await asyncio.sleep(0.15 * (attempt + 1))

    return None

# ============================================================
# PROVEN SLUG DISCOVERY
# ============================================================

async def fetch_event_by_slug(slug):
    for url, params in (
        (f"{GAMMA_API}/events/slug/{slug}", None),
        (f"{GAMMA_API}/events", {"slug": slug}),
    ):
        data = await get_json(url, params=params)
        if isinstance(data, dict):
            return data
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return data[0]
    return None

def parse_market_from_event(event, expected_slug):
    if not isinstance(event, dict):
        return None

    raw_markets = event.get("markets")
    if not isinstance(raw_markets, list):
        return None

    for raw in raw_markets:
        if not isinstance(raw, dict):
            continue

        cid = str(raw.get("conditionId") or "")
        if not cid:
            continue

        outcomes = [str(x).strip().upper() for x in parse_jsonish(raw.get("outcomes"))]
        tokens = [str(x) for x in parse_jsonish(raw.get("clobTokenIds"))]

        if len(tokens) < 2:
            continue

        up_asset = None
        down_asset = None

        for i, outcome in enumerate(outcomes):
            if i >= len(tokens):
                break
            if outcome in {"UP", "YES"}:
                up_asset = tokens[i]
            elif outcome in {"DOWN", "NO"}:
                down_asset = tokens[i]

        up_asset = up_asset or tokens[0]
        down_asset = down_asset or tokens[1]

        slug = str(raw.get("slug") or event.get("slug") or expected_slug)
        start_ts = slot_start_from_slug(slug) or slot_start_from_slug(expected_slug)
        if not start_ts:
            continue

        return {
            "condition_id": cid,
            "slug": slug,
            "title": str(raw.get("question") or event.get("title") or slug),
            "start_ts": int(start_ts),
            "end_ts": int(start_ts) + 300,
            "up_asset": up_asset,
            "down_asset": down_asset,
        }

    return None

async def subscribe_asset(asset):
    if not asset or asset in subscribed_assets:
        return
    subscribed_assets.add(asset)
    await ws_send_queue.put({"operation": "subscribe", "assets_ids": [asset]})

async def add_market(m):
    cid = m["condition_id"]
    if cid in markets:
        return

    markets[cid] = m
    asset_to_market[m["up_asset"]] = cid
    asset_to_market[m["down_asset"]] = cid

    with db() as conn:
        conn.execute("""
            INSERT INTO markets(
                condition_id, slug, title, start_ts, end_ts,
                up_asset, down_asset, discovered_ms
            ) VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(condition_id) DO UPDATE SET
                slug=excluded.slug,
                title=excluded.title,
                start_ts=excluded.start_ts,
                end_ts=excluded.end_ts,
                up_asset=excluded.up_asset,
                down_asset=excluded.down_asset
        """, (
            cid, m["slug"], m["title"], m["start_ts"], m["end_ts"],
            m["up_asset"], m["down_asset"], now_ms(),
        ))
        conn.commit()

    await subscribe_asset(m["up_asset"])
    await subscribe_asset(m["down_asset"])

    log.info(
        "MARKET %s | %s -> %s",
        m["slug"],
        utc_iso(m["start_ts"]),
        utc_iso(m["end_ts"]),
    )

async def discovery_loop():
    while True:
        try:
            now = now_ts()
            current = (now // 300) * 300

            for slot in (current - 300, current, current + 300):
                slug = f"btc-updown-5m-{slot}"
                event = await fetch_event_by_slug(slug)
                if not event:
                    continue

                m = parse_market_from_event(event, slug)
                if m:
                    await add_market(m)

        except Exception:
            log.exception("Discovery failed")

        await asyncio.sleep(DISCOVERY_INTERVAL)

# ============================================================
# BOOK
# ============================================================

def level_map(rows):
    out = {}
    for x in rows or []:
        if not isinstance(x, dict):
            continue
        p = sf(x.get("price"), math.nan)
        q = sf(x.get("size"), 0)
        if not math.isnan(p) and q > 0:
            out[p] = q
    return out

def apply_book(asset, payload, source="ws"):
    books[asset] = {
        "bids": level_map(payload.get("bids")),
        "asks": level_map(payload.get("asks")),
        "received_ms": now_ms(),
        "source": source,
    }

def apply_price_change(payload):
    recv = now_ms()
    changes = payload.get("price_changes") or payload.get("priceChanges") or []

    for ch in changes:
        if not isinstance(ch, dict):
            continue

        asset = str(
            ch.get("asset_id")
            or ch.get("token_id")
            or ch.get("tokenId")
            or ""
        )
        if not asset:
            continue

        b = books.setdefault(asset, {
            "bids": {},
            "asks": {},
            "received_ms": recv,
            "source": "ws-delta",
        })

        p = sf(ch.get("price"), math.nan)
        q = sf(ch.get("size"), 0)
        side = str(ch.get("side", "")).upper()

        if math.isnan(p):
            continue

        target = b["bids"] if side == "BUY" else b["asks"]
        if q <= 0:
            target.pop(p, None)
        else:
            target[p] = q

        b["received_ms"] = recv
        b["source"] = "ws"

def best_bid(asset):
    b = books.get(asset)
    if not b or not b["bids"]:
        return None
    return max(b["bids"])

def best_ask(asset):
    b = books.get(asset)
    if not b or not b["asks"]:
        return None
    return min(b["asks"])

async def refresh_book(asset):
    data = await get_json(f"{CLOB_API}/book", params={"token_id": asset})
    if isinstance(data, dict):
        apply_book(asset, data, "rest")
        return True
    return False

async def ensure_fresh_book(asset):
    b = books.get(asset)
    if b and (b["asks"] or b["bids"]):
        age = now_ms() - b["received_ms"]
        if age <= MAX_BOOK_AGE_MS:
            return age

    await refresh_book(asset)
    b = books.get(asset)
    return (now_ms() - b["received_ms"]) if b else None

def classify_maker_buy(asset, leader_price):
    """
    Heuristic only: public Data API doesn't expose leader's resting order.
    For a BUY:
      - near current best bid -> MAKER_LIKELY
      - near/through current best ask -> TAKER_LIKELY
      - otherwise UNKNOWN
    """
    bid = best_bid(asset)
    ask = best_ask(asset)

    if bid is None or ask is None:
        return "UNKNOWN", bid, ask

    spread = max(0.0, ask - bid)
    tol = max(0.005, min(0.015, spread * 0.75))

    if abs(leader_price - bid) <= tol and leader_price < ask - 0.002:
        return "MAKER_LIKELY", bid, ask

    if leader_price >= ask - tol:
        return "TAKER_LIKELY", bid, ask

    return "UNKNOWN", bid, ask

# ============================================================
# WS
# ============================================================

def parse_ws(raw):
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "ignore")
    if raw in ("", "PING", "PONG"):
        return []
    try:
        x = json.loads(raw)
        return x if isinstance(x, list) else [x]
    except Exception:
        return []

async def ws_sender(ws):
    while True:
        msg = await ws_send_queue.get()
        try:
            await ws.send(jd(msg))
        except Exception:
            await ws_send_queue.put(msg)
            return

async def ws_ping(ws):
    while True:
        try:
            await ws.send("PING")
        except Exception:
            return
        await asyncio.sleep(10)

async def ws_loop():
    while True:
        try:
            if not subscribed_assets:
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

                log.info("WS connected | assets=%d", len(subscribed_assets))

                sender = asyncio.create_task(ws_sender(ws))
                ping = asyncio.create_task(ws_ping(ws))

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
                                    or ""
                                )
                                if asset:
                                    apply_book(asset, payload)

                            elif et == "price_change":
                                apply_price_change(payload)

                            elif et == "market_resolved":
                                await handle_ws_resolution(payload)

                finally:
                    sender.cancel()
                    ping.cancel()

        except Exception as e:
            log.warning("WS reconnect: %s", e)
            await asyncio.sleep(1)

# ============================================================
# PAPER EXECUTION
# ============================================================

def buy_for_budget(asset, budget):
    """
    Walk live asks and keep total cost (notional + taker fee) <= budget.
    """
    b = books.get(asset)
    if not b or not b["asks"]:
        return [], 0.0, 0.0, 0.0, budget

    remaining = budget
    fills = []
    total_gross = 0.0
    total_fee = 0.0
    shares = 0.0

    for price in sorted(b["asks"]):
        available = b["asks"][price]
        if available <= 0:
            continue

        fee_per_share = CRYPTO_FEE_RATE * price * (1.0 - price)
        all_in_per_share = price + fee_per_share

        if all_in_per_share <= 0:
            continue

        affordable = remaining / all_in_per_share
        take = min(available, affordable)

        if take <= 1e-10:
            break

        gross = take * price
        fee = fee_for(take, price)
        total = gross + fee

        # Protect against rounding pushing a fill a few micros over budget.
        if total > remaining + 1e-8:
            take *= max(0.0, remaining / total)
            gross = take * price
            fee = fee_for(take, price)
            total = gross + fee

        if take <= 1e-10:
            break

        fills.append((price, take))
        total_gross += gross
        total_fee += fee
        shares += take
        remaining -= total

        if remaining < 0.00001:
            break

    return fills, shares, total_gross, total_fee, max(0.0, remaining)

async def execute_variant(signal, threshold, maker_filter, delay_sec):
    if delay_sec > 0:
        await asyncio.sleep(delay_sec)

    cid = signal["condition_id"]

    # Eligibility is deterministic from the FIRST leader BUY only.
    eligible = signal["elapsed_sec"] >= threshold
    if maker_filter == "MAKER" and signal["maker_class"] != "MAKER_LIKELY":
        eligible = False

    if not eligible:
        with db() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO executions(
                    condition_id, threshold_sec, maker_filter, delay_sec,
                    eligible, attempted_ms, asset, outcome, maker_class,
                    requested_budget, filled_shares, avg_price, gross_cost,
                    fee, total_cost, unspent_budget, best_bid, best_ask,
                    book_age_ms, status, fills_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                cid, threshold, maker_filter, delay_sec,
                0, now_ms(), signal["asset"], signal["outcome"],
                signal["maker_class"], PAPER_BUDGET_USD,
                0, None, 0, 0, 0, PAPER_BUDGET_USD,
                best_bid(signal["asset"]), best_ask(signal["asset"]),
                None, "FILTERED_OUT", "[]",
            ))
            conn.commit()
        return

    asset = signal["asset"]
    age = await ensure_fresh_book(asset)
    bid = best_bid(asset)
    ask = best_ask(asset)

    fills, shares, gross, fee, unspent = buy_for_budget(asset, PAPER_BUDGET_USD)
    total = gross + fee
    avg = gross / shares if shares > 0 else None

    if shares <= 0:
        status = "NO_LIQUIDITY"
    elif unspent <= 0.01:
        status = "FULL_BUDGET"
    else:
        status = "PARTIAL_BUDGET"

    with db() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO executions(
                condition_id, threshold_sec, maker_filter, delay_sec,
                eligible, attempted_ms, asset, outcome, maker_class,
                requested_budget, filled_shares, avg_price, gross_cost,
                fee, total_cost, unspent_budget, best_bid, best_ask,
                book_age_ms, status, fills_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            cid, threshold, maker_filter, delay_sec,
            1, now_ms(), asset, signal["outcome"],
            signal["maker_class"], PAPER_BUDGET_USD,
            shares, avg, gross, fee, total, unspent,
            bid, ask, age, status,
            jd([{"price": p, "shares": q} for p, q in fills]),
        ))
        conn.commit()

    log.info(
        "PAPER T%d %s +%ds | %s | %s | price=%s shares=%.4f cost=%.2f",
        threshold,
        maker_filter,
        delay_sec,
        signal["outcome"],
        signal["maker_class"],
        f"{avg:.4f}" if avg is not None else "-",
        shares,
        total,
    )

# ============================================================
# LEADER FIRST-TRADE DETECTION
# ============================================================

def store_seen_trade(t, detected):
    uid = trade_uid(t)

    with db() as conn:
        cur = conn.execute("""
            INSERT OR IGNORE INTO seen_leader_trades(
                uid, leader_ts, detected_ms, condition_id, asset,
                outcome, side, price, size, raw_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            uid,
            si(t.get("timestamp")),
            detected,
            str(t.get("conditionId", "")),
            str(t.get("asset", "")),
            str(t.get("outcome", "")),
            str(t.get("side", "")).upper(),
            sf(t.get("price")),
            sf(t.get("size")),
            jd(t),
        ))
        conn.commit()
        return cur.rowcount > 0, uid

async def handle_first_buy(t, uid, detected):
    cid = str(t.get("conditionId", ""))
    asset = str(t.get("asset", ""))
    leader_ts = si(t.get("timestamp"))

    market = markets.get(cid)
    if not market:
        # Try infer via slug directly from trade.
        slug = str(t.get("slug") or t.get("eventSlug") or "")
        slot = slot_start_from_slug(slug)
        if not slot:
            return

        event = await fetch_event_by_slug(slug)
        m = parse_market_from_event(event, slug) if event else None
        if not m:
            return

        await add_market(m)
        market = m

    # If market started before this process launched, we don't know whether
    # an earlier leader buy was missed. These markets are deliberately ignored.
    launch_ts = si(state_get("launch_ts", "0"))
    if market["start_ts"] < launch_ts - 2:
        return

    elapsed = leader_ts - market["start_ts"]

    # Strictly only the first BUY in the 5-minute market.
    if cid in first_leader_buy:
        return

    await ensure_fresh_book(asset)

    maker_class, bid, ask = classify_maker_buy(asset, sf(t.get("price")))
    spread = (ask - bid) if (ask is not None and bid is not None) else None

    signal = {
        "condition_id": cid,
        "leader_uid": uid,
        "leader_ts": leader_ts,
        "detected_ms": detected,
        "detection_delay_ms": detected - leader_ts * 1000,
        "elapsed_sec": elapsed,
        "asset": asset,
        "outcome": str(t.get("outcome", "")),
        "leader_price": sf(t.get("price")),
        "leader_size": sf(t.get("size")),
        "best_bid": bid,
        "best_ask": ask,
        "spread": spread,
        "maker_class": maker_class,
        "market_slug": market["slug"],
        "title": market["title"],
    }

    first_leader_buy[cid] = signal

    with db() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO signals(
                condition_id, leader_uid, leader_ts, detected_ms,
                detection_delay_ms, elapsed_sec, asset, outcome,
                leader_price, leader_size, best_bid, best_ask,
                spread, maker_class, market_slug, title
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            cid, uid, leader_ts, detected,
            signal["detection_delay_ms"], elapsed, asset,
            signal["outcome"], signal["leader_price"],
            signal["leader_size"], bid, ask, spread,
            maker_class, market["slug"], market["title"],
        ))
        conn.commit()

    log.info(
        "FIRST BUY %s | t=%.0fs | %s @ %.4f | %s",
        market["slug"],
        elapsed,
        signal["outcome"],
        signal["leader_price"],
        maker_class,
    )

    # All 30 variants are created from the same first trade.
    for threshold in SIGNAL_THRESHOLDS:
        for maker_filter in ("ALL", "MAKER"):
            for delay in EXECUTION_DELAYS:
                task = asyncio.create_task(
                    execute_variant(signal, threshold, maker_filter, delay)
                )
                signal_tasks.add(task)
                task.add_done_callback(signal_tasks.discard)

async def leader_poller():
    log.info(
        "Leader poller started | %.2fs | wallet=%s",
        LEADER_POLL_INTERVAL,
        LEADER_WALLET,
    )

    while True:
        started = time.monotonic()

        try:
            # Tiny sliding window is enough; dedupe is by trade UID.
            rows = await get_json(
                f"{DATA_API}/trades",
                params={
                    "user": LEADER_WALLET,
                    "limit": 100,
                    "offset": 0,
                    "takerOnly": "false",
                    "side": "BUY",
                    "start": now_ts() - 20,
                    "end": now_ts() + 2,
                },
            )

            if isinstance(rows, list):
                # Timestamp is only second-resolution. Sort stably by timestamp.
                rows.sort(key=lambda x: (si(x.get("timestamp")), str(x.get("transactionHash", ""))))

                for t in rows:
                    if not is_btc_5m_trade(t):
                        continue

                    detected = now_ms()
                    inserted, uid = store_seen_trade(t, detected)

                    if not inserted:
                        continue

                    await handle_first_buy(t, uid, detected)

        except Exception:
            log.exception("Leader poller failed")

        elapsed = time.monotonic() - started
        await asyncio.sleep(max(0.0, LEADER_POLL_INTERVAL - elapsed))

# ============================================================
# RESOLUTION / PNL
# ============================================================

def resolved_winner_from_market(raw):
    if not isinstance(raw, dict):
        return None, None

    outcomes = [str(x) for x in parse_jsonish(raw.get("outcomes"))]
    tokens = [str(x) for x in parse_jsonish(raw.get("clobTokenIds"))]
    prices_raw = parse_jsonish(raw.get("outcomePrices"))

    if len(outcomes) >= 2 and len(tokens) >= 2 and len(prices_raw) >= 2:
        prices = [sf(x, -1) for x in prices_raw]
        idx = max(range(len(prices)), key=lambda i: prices[i])
        best = prices[idx]
        second = max(prices[i] for i in range(len(prices)) if i != idx)

        if best >= 0.999 and second <= 0.001:
            return tokens[idx], outcomes[idx]

    token_objs = raw.get("tokens")
    if isinstance(token_objs, list):
        for tok in token_objs:
            if isinstance(tok, dict) and bool(tok.get("winner", False)):
                asset = str(tok.get("token_id") or tok.get("tokenId") or tok.get("id") or "")
                outcome = str(tok.get("outcome") or tok.get("name") or "")
                if asset:
                    return asset, outcome

    return None, None

async def settle_market(cid, winning_asset, winning_outcome):
    with db() as conn:
        execs = conn.execute("""
            SELECT * FROM executions
            WHERE condition_id=? AND eligible=1
        """, (cid,)).fetchall()

        for e in execs:
            exists = conn.execute(
                "SELECT 1 FROM results WHERE execution_id=?",
                (e["id"],),
            ).fetchone()

            if exists:
                continue

            shares = sf(e["filled_shares"])
            total_cost = sf(e["total_cost"])
            won = 1 if (shares > 0 and str(e["asset"]) == winning_asset) else 0
            payout = shares if won else 0.0
            pnl = payout - total_cost
            roi = (pnl / total_cost * 100.0) if total_cost > 0 else 0.0

            conn.execute("""
                INSERT INTO results(
                    execution_id, condition_id, threshold_sec, maker_filter,
                    delay_sec, winning_asset, winning_outcome, won,
                    payout, pnl, roi_pct, settled_ms
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                e["id"], cid, e["threshold_sec"], e["maker_filter"],
                e["delay_sec"], winning_asset, winning_outcome, won,
                payout, pnl, roi, now_ms(),
            ))

        conn.execute("""
            UPDATE markets
            SET resolved=1, winning_asset=?, winning_outcome=?
            WHERE condition_id=?
        """, (winning_asset, winning_outcome, cid))
        conn.commit()

    log.info("RESOLVED %s | winner=%s", cid[-8:], winning_outcome)

async def handle_ws_resolution(payload):
    cid = str(payload.get("market") or payload.get("condition_id") or "")
    winning_asset = str(payload.get("winning_asset_id") or "")
    winning_outcome = str(payload.get("winning_outcome") or "")

    if cid and winning_asset:
        await settle_market(cid, winning_asset, winning_outcome)

async def resolution_loop():
    while True:
        try:
            cutoff = now_ts() - 10

            with db() as conn:
                rows = conn.execute("""
                    SELECT * FROM markets
                    WHERE resolved=0 AND end_ts < ?
                    ORDER BY end_ts
                    LIMIT 50
                """, (cutoff,)).fetchall()

            for row in rows:
                event = await fetch_event_by_slug(str(row["slug"]))
                if not isinstance(event, dict):
                    continue

                raw_markets = event.get("markets")
                if not isinstance(raw_markets, list):
                    continue

                raw = None
                for m in raw_markets:
                    if str(m.get("conditionId") or "") == str(row["condition_id"]):
                        raw = m
                        break

                if raw is None and len(raw_markets) == 1:
                    raw = raw_markets[0]

                winning_asset, winning_outcome = resolved_winner_from_market(raw)
                if winning_asset:
                    await settle_market(
                        str(row["condition_id"]),
                        winning_asset,
                        winning_outcome,
                    )

        except Exception:
            log.exception("Resolution loop failed")

        await asyncio.sleep(10)

# ============================================================
# REPORTING
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

def build_summary(start_ms=None, end_ms=None):
    where = ""
    params = []

    # Attribute results by market end time, not when fallback noticed resolution.
    if start_ms is not None and end_ms is not None:
        where = " AND (m.end_ts * 1000) >= ? AND (m.end_ts * 1000) < ? "
        params = [start_ms, end_ms]

    rows_out = []

    with db() as conn:
        for threshold in SIGNAL_THRESHOLDS:
            for maker_filter in ("ALL", "MAKER"):
                for delay in EXECUTION_DELAYS:
                    rows = conn.execute(f"""
                        SELECT r.*, e.total_cost, e.avg_price, e.filled_shares
                        FROM results r
                        JOIN executions e ON e.id = r.execution_id
                        JOIN markets m ON m.condition_id = r.condition_id
                        WHERE r.threshold_sec=?
                          AND r.maker_filter=?
                          AND r.delay_sec=?
                          {where}
                    """, [threshold, maker_filter, delay] + params).fetchall()

                    n = len(rows)
                    wins = sum(si(r["won"]) for r in rows)
                    cost = sum(sf(r["total_cost"]) for r in rows)
                    pnl = sum(sf(r["pnl"]) for r in rows)

                    rows_out.append({
                        "threshold_sec": threshold,
                        "maker_filter": maker_filter,
                        "delay_sec": delay,
                        "markets": n,
                        "wins": wins,
                        "losses": n - wins,
                        "win_rate_pct": round((wins / n * 100) if n else 0, 2),
                        "total_cost": round(cost, 4),
                        "pnl": round(pnl, 4),
                        "roi_pct": round((pnl / cost * 100) if cost else 0, 2),
                        "avg_pnl": round((pnl / n) if n else 0, 4),
                    })

    return sorted(
        rows_out,
        key=lambda x: (x["pnl"], x["markets"]),
        reverse=True,
    )

def make_hourly_report(start_ts, end_ts):
    sm = start_ts * 1000
    em = end_ts * 1000

    with db() as conn:
        signals = conn.execute("""
            SELECT * FROM signals
            WHERE detected_ms>=? AND detected_ms<?
            ORDER BY detected_ms
        """, (sm, em)).fetchall()

        executions = conn.execute("""
            SELECT * FROM executions
            WHERE attempted_ms>=? AND attempted_ms<?
            ORDER BY attempted_ms, threshold_sec, maker_filter, delay_sec
        """, (sm, em)).fetchall()

        results = conn.execute("""
            SELECT r.*, e.asset, e.outcome, e.avg_price, e.total_cost,
                   e.filled_shares, m.slug, m.title, m.end_ts
            FROM results r
            JOIN executions e ON e.id = r.execution_id
            JOIN markets m ON m.condition_id = r.condition_id
            WHERE (m.end_ts * 1000)>=? AND (m.end_ts * 1000)<?
            ORDER BY m.end_ts, r.threshold_sec, r.maker_filter, r.delay_sec
        """, (sm, em)).fetchall()

    hourly = build_summary(sm, em)
    cumulative = build_summary()

    qualified = [x for x in hourly if x["markets"] > 0]
    best = qualified[0] if qualified else None

    lines = [
        "POWERWINNER LATE SIGNAL PAPER BOT",
        "=" * 70,
        f"Period UTC: {utc_iso(start_ts)} -> {utc_iso(end_ts)}",
        f"Budget per signal: ${PAPER_BUDGET_USD:.2f} all-in",
        f"Thresholds: {SIGNAL_THRESHOLDS}",
        f"Execution delays: {EXECUTION_DELAYS}",
        f"Leader poll: {LEADER_POLL_INTERVAL:.2f}s",
        "",
        f"First-leader-BUY signals detected this hour: {len(signals)}",
        f"Paper execution rows this hour: {len(executions)}",
        f"Resolved paper results attributed to this hour: {len(results)}",
        "",
    ]

    if best:
        lines += [
            "BEST HOURLY VARIANT",
            (
                f"T{best['threshold_sec']} {best['maker_filter']} +{best['delay_sec']}s | "
                f"markets={best['markets']} | W/L={best['wins']}/{best['losses']} | "
                f"WR={best['win_rate_pct']:.1f}% | "
                f"PnL=${best['pnl']:+.2f} | ROI={best['roi_pct']:+.2f}%"
            ),
            "",
        ]

    lines += [
        "FILES",
        "signals.csv            - first Powerwinner BUY per BTC 5m market",
        "executions.csv         - all paper delay/filter variants",
        "results.csv            - settled PnL per paper execution",
        "hourly_summary.csv     - 30 variants ranked for this hour",
        "cumulative_summary.csv - all variants ranked since bot start",
        "",
        "MAKER_LIKELY is a public-book heuristic, not private order data.",
        "This bot places NO real orders.",
    ]

    d1 = datetime.fromtimestamp(start_ts, tz=timezone.utc)
    d2 = datetime.fromtimestamp(end_ts, tz=timezone.utc)
    path = REPORT_DIR / f"late_signal_{d1:%Y-%m-%d_%H-%M}_{d2:%H-%M}_UTC.zip"

    summary_cols = [
        "threshold_sec", "maker_filter", "delay_sec", "markets",
        "wins", "losses", "win_rate_pct", "total_cost",
        "pnl", "roi_pct", "avg_pnl",
    ]

    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("signals.csv", csv_bytes(signals))
        z.writestr("executions.csv", csv_bytes(executions))
        z.writestr("results.csv", csv_bytes(results))
        z.writestr("hourly_summary.csv", csv_bytes(hourly, summary_cols))
        z.writestr("cumulative_summary.csv", csv_bytes(cumulative, summary_cols))
        z.writestr("report.txt", "\n".join(lines).encode("utf-8"))

    return path, best

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
                path, best = make_hourly_report(start, end)

                if best:
                    best_line = (
                        f"T{best['threshold_sec']} {best['maker_filter']} "
                        f"+{best['delay_sec']}s | "
                        f"PnL ${best['pnl']:+.2f} | WR {best['win_rate_pct']:.1f}%"
                    )
                else:
                    best_line = "No resolved qualified signals yet"

                ok = await tg_file(
                    path,
                    (
                        "🎯 Powerwinner Late Signal Paper\n"
                        f"{utc_iso(start)} → {utc_iso(end)}\n"
                        f"{best_line}"
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
        sig = conn.execute("SELECT COUNT(*) c FROM signals").fetchone()["c"]
        exe = conn.execute("SELECT COUNT(*) c FROM executions WHERE eligible=1").fetchone()["c"]
        res = conn.execute("SELECT COUNT(*) c FROM results").fetchone()["c"]

    return web.json_response({
        "ok": True,
        "version": "1.0",
        "paper_only": True,
        "leader": LEADER_WALLET,
        "poll_interval_s": LEADER_POLL_INTERVAL,
        "budget_usd": PAPER_BUDGET_USD,
        "thresholds": SIGNAL_THRESHOLDS,
        "delays": EXECUTION_DELAYS,
        "markets_tracked": len(markets),
        "ws_assets": len(subscribed_assets),
        "books": len(books),
        "signals": sig,
        "eligible_executions": exe,
        "settled_results": res,
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

    # Mark launch time once per process. Markets that began before launch are
    # ignored for first-trade signals because their true first BUY may be missed.
    state_set("launch_ts", now_ts())

    session = aiohttp.ClientSession(headers={
        "User-Agent": "PowerwinnerLateSignalPaper/1.0",
        "Accept": "application/json",
    })

    tasks = [
        asyncio.create_task(web_server()),
        asyncio.create_task(discovery_loop()),
        asyncio.create_task(ws_loop()),
        asyncio.create_task(leader_poller()),
        asyncio.create_task(resolution_loop()),
        asyncio.create_task(reporter()),
    ]

    log.info(
        "Late Signal Paper Bot started | thresholds=%s | delays=%s | budget=$%.2f",
        SIGNAL_THRESHOLDS,
        EXECUTION_DELAYS,
        PAPER_BUDGET_USD,
    )

    try:
        await asyncio.gather(*tasks)
    finally:
        for t in tasks:
            t.cancel()

        for t in list(signal_tasks):
            t.cancel()

        if session:
            await session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
