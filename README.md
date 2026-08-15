# Powerwinner Late Signal Paper Bot

**Paper only. No real orders are placed.**

This bot tests the strongest current Powerwinner hypothesis:

> Ignore ordinary copy trading. Use only Powerwinner's **first BUY** in each BTC 5-minute Up/Down market as a directional signal, especially when that first BUY arrives late in the market.

## What is tested simultaneously

Signal thresholds:

- first BUY at or after 60 seconds
- first BUY at or after 75 seconds
- first BUY at or after 90 seconds

Filters:

- `ALL`
- `MAKER_LIKELY`

Execution delays:

- immediate
- +1 second
- +3 seconds
- +5 seconds
- +10 seconds

That is **30 paper variants** from the same first-trade stream.

## Execution model

Each eligible variant gets a fixed **$10 all-in paper budget**.

The bot:

1. keeps the BTC 5m Up/Down order books live over Polymarket Market WebSocket;
2. detects Powerwinner public trades from the Data API;
3. accepts only the **first BUY** of each market;
4. waits the requested delay;
5. walks the actual visible ask depth;
6. calculates average fill price;
7. applies the current crypto taker fee formula;
8. holds the paper position to resolution;
9. pays $1/share only if the selected outcome wins.

## Important maker note

`MAKER_LIKELY` is a public-book heuristic.

The public Data API does not reveal Powerwinner's private resting order. We classify a first BUY as maker-like when its execution price is close to the current best bid rather than the current best ask.

The `ALL` variants are therefore the most objective benchmark. `MAKER` variants are research filters.

## Hourly Telegram ZIP

Each hour, after a short settlement grace period:

- `signals.csv`
- `executions.csv`
- `results.csv`
- `hourly_summary.csv`
- `cumulative_summary.csv`
- `report.txt`

The summaries rank all 30 variants by actual paper PnL.

## Render setup

Use a **separate repository and separate Render service**.

Upload:

- `main.py`
- `requirements.txt`
- `render.yaml`
- `.env.example`
- `.gitignore`
- `README.md`

Build command:

`pip install -r requirements.txt`

Start command:

`python main.py`

Environment variables required:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

Recommended persistent disk:

`/var/data`

## What to expect in logs

Market discovery:

`MARKET btc-updown-5m-...`

WebSocket:

`WS connected | assets=...`

First Powerwinner BUY:

`FIRST BUY btc-updown-5m-... | t=82s | Up @ 0.6900 | MAKER_LIKELY`

Paper variants:

`PAPER T75 ALL +3s | Up | MAKER_LIKELY | price=...`

Settlement:

`RESOLVED ... | winner=Up`

## Research discipline

Do not judge the strategy from 3-5 markets. Let it collect at least dozens of qualified signals. The 75+ / MAKER results that motivated this bot came from a small retrospective sample and must be tested prospectively.
