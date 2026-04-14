"""
Multi-Market 5m Price Tick Collector (ETH, HYPE, DOGE, BNB)

Records every price change for 4 crypto UP/DOWN 5-minute markets concurrently.
One CSV file per market window, organized into per-market folders:
    csv/doge/doge-updown-5m-<epoch>.csv
    csv/hype/hype-updown-5m-<epoch>.csv
    csv/eth/eth-updown-5m-<epoch>.csv
    csv/bnb/bnb-updown-5m-<epoch>.csv

Data source: Polymarket CLOB WebSocket (book events, top-5 levels with sizes)
             Polymarket Chainlink WS (oracle prices — same source used for settlement)

Usage:
    python3 -m price_collector.main --windows 3
"""

import asyncio
import csv
import json
import ssl
import time
import argparse
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import certifi
import websockets

ROOT_DIR = Path(__file__).resolve().parent.parent
CSV_DIR = ROOT_DIR / "csv"
GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
CLOB_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
CHAINLINK_WS = "wss://ws-live-data.polymarket.com"
CHAINLINK_PING_INTERVAL = 4  # seconds
BOOK_DEPTH = 2  # top-N levels to record per side (p1 + p2 with sizes)

# Per-market config.
# - name:          short label used in CSV dir and log prefix
# - symbol:        Chainlink topic symbol
# - slug_prefix:   Polymarket market slug prefix (5-min UP/DOWN binaries)
MARKETS = [
    {"name": "doge", "symbol": "doge/usd", "slug_prefix": "doge-updown-5m"},
    {"name": "hype", "symbol": "hype/usd", "slug_prefix": "hype-updown-5m"},
    {"name": "eth",  "symbol": "eth/usd",  "slug_prefix": "eth-updown-5m"},
    {"name": "bnb",  "symbol": "bnb/usd",  "slug_prefix": "bnb-updown-5m"},
]


def ssl_ctx():
    return ssl.create_default_context(cafile=certifi.where())


class MarketWindow:
    """Tracks one 5-minute market window for a specific crypto."""

    def __init__(self, cfg, slug, condition_id, token_id_up, token_id_down, open_epoch):
        self.cfg = cfg
        self.name = cfg["name"]
        self.symbol = cfg["symbol"]
        self.slug = slug
        self.condition_id = condition_id
        self.token_id_up = token_id_up
        self.token_id_down = token_id_down
        self.open_epoch = open_epoch
        self.close_epoch = open_epoch + 300

        self.csv_dir = CSV_DIR / self.name
        self.csv_dir.mkdir(parents=True, exist_ok=True)
        self.csv_path = self.csv_dir / f"{slug}.csv"
        self.csv_file = None
        self.csv_writer = None
        self.tick_count = 0
        self.winner = None
        self._last_write = 0

        # Price column names in the CSV are per-symbol, e.g. "doge_price".
        self.price_col = f"{self.name}_price"
        self.oracle_ts_col = f"{self.name}_oracle_ts"

    def _build_header(self):
        cols = ["timestamp", "elapsed_sec"]
        for side in ("up", "down"):
            for book_side in ("bid", "ask"):
                for i in range(1, BOOK_DEPTH + 1):
                    cols.append(f"{side}_{book_side}_p{i}")
                    cols.append(f"{side}_{book_side}_s{i}")
        cols += ["up_spread", "down_spread", self.price_col, self.oracle_ts_col]
        return cols

    def open_csv(self):
        self.csv_file = open(self.csv_path, "w", newline="")
        self.csv_writer = csv.writer(self.csv_file)
        self.csv_writer.writerow(self._build_header())
        print(f"  [{self.name.upper()}] CSV opened: {self.csv_path.name}")

    def write_tick(self, state, oracle_state):
        """Append one row using current book state + latest oracle tick."""
        now = time.time()
        elapsed = round(now - self.open_epoch, 3)

        # Capture from t=0 to t=303 — 3s past the boundary to catch late ticks.
        # Lower bound removed so the initial write_tick() at market open
        # records whatever we have (REST-seeded book + latest Chainlink price)
        # instead of waiting for the next trigger.
        if elapsed < 0 or elapsed > 303.0:
            return
        # Rate limit: max 5 writes/sec.
        if now - self._last_write < 0.2:
            return
        self._last_write = now

        row = [round(now, 3), elapsed]
        # up_bid*, up_ask*, down_bid*, down_ask* — top-N prices and sizes
        for side in ("up", "down"):
            for book_side in ("bid", "ask"):
                levels = state[f"{side}_{book_side}_levels"]
                for i in range(BOOK_DEPTH):
                    if i < len(levels):
                        p, s = levels[i]
                        row.extend([p, s])
                    else:
                        row.extend([None, None])

        up_bid1 = state["up_bid_levels"][0][0] if state["up_bid_levels"] else None
        up_ask1 = state["up_ask_levels"][0][0] if state["up_ask_levels"] else None
        down_bid1 = state["down_bid_levels"][0][0] if state["down_bid_levels"] else None
        down_ask1 = state["down_ask_levels"][0][0] if state["down_ask_levels"] else None
        up_spread = round(up_ask1 - up_bid1, 4) if (up_bid1 is not None and up_ask1 is not None) else None
        down_spread = round(down_ask1 - down_bid1, 4) if (down_bid1 is not None and down_ask1 is not None) else None

        row += [up_spread, down_spread,
                oracle_state.get("price"), oracle_state.get("oracle_ts")]

        self.csv_writer.writerow(row)
        self.csv_file.flush()
        self.tick_count += 1

    def close_csv(self):
        if self.csv_file:
            self.csv_file.close()
            self.csv_file = None

    @property
    def seconds_until_open(self):
        return max(0, self.open_epoch - time.time())


async def fetch_market_by_slug(session, slug):
    url = f"{GAMMA_API}/markets"
    params = {"slug": slug}
    try:
        async with session.get(url, params=params,
                               timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            if not data:
                return None
            return data[0] if isinstance(data, list) else data
    except Exception as e:
        print(f"  API error for {slug}: {e}")
        return None


async def fetch_order_book(session, token_id):
    """Fetch full order book from REST — used to seed initial state."""
    url = f"{CLOB_API}/book"
    params = {"token_id": token_id}
    try:
        async with session.get(url, params=params,
                               timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status != 200:
                return [], []
            data = await resp.json()
            bids = [(float(b["price"]), float(b["size"])) for b in data.get("bids", [])]
            asks = [(float(a["price"]), float(a["size"])) for a in data.get("asks", [])]
            # Top levels: bids desc, asks asc
            bids.sort(key=lambda x: -x[0])
            asks.sort(key=lambda x: x[0])
            return bids, asks
    except Exception:
        return [], []


def parse_tokens(market_data):
    """Extract (up_token_id, down_token_id) from a Gamma market object."""
    token_id_up = token_id_down = None
    tokens = market_data.get("tokens", [])
    if tokens and len(tokens) >= 2:
        for tok in tokens:
            outcome = tok.get("outcome", "").lower()
            tid = tok.get("token_id", "")
            if outcome in ("up", "yes"):
                token_id_up = tid
            elif outcome in ("down", "no"):
                token_id_down = tid

    if not token_id_up or not token_id_down:
        clob_raw = market_data.get("clobTokenIds", "")
        outcomes_raw = market_data.get("outcomes", "")
        if isinstance(clob_raw, str) and clob_raw:
            try:
                clob_list = json.loads(clob_raw)
                outcomes_list = json.loads(outcomes_raw) if outcomes_raw else []
                if len(clob_list) >= 2:
                    if len(outcomes_list) >= 2:
                        for idx, outcome in enumerate(outcomes_list):
                            if outcome.lower() in ("up", "yes"):
                                token_id_up = str(clob_list[idx])
                            elif outcome.lower() in ("down", "no"):
                                token_id_down = str(clob_list[idx])
                    if not token_id_up or not token_id_down:
                        token_id_up = str(clob_list[0])
                        token_id_down = str(clob_list[1])
            except (json.JSONDecodeError, IndexError):
                pass

    return token_id_up, token_id_down


async def discover_next_markets(cfg, count):
    """Generate deterministic slugs for one market family and resolve via Gamma."""
    ssl_context = ssl_ctx()
    markets = []
    now = time.time()
    interval = 300
    current_boundary = int(now // interval) * interval

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=ssl_context),
        headers={"User-Agent": "Mozilla/5.0"},
    ) as session:
        for i in range(100):
            epoch = current_boundary + interval * i
            if epoch + 300 < now:
                continue
            slug = f"{cfg['slug_prefix']}-{epoch}"
            data = await fetch_market_by_slug(session, slug)
            if not data:
                continue
            cid = data.get("conditionId", "")
            if not cid:
                continue
            t_up, t_down = parse_tokens(data)
            if not t_up or not t_down:
                continue

            markets.append(MarketWindow(
                cfg=cfg, slug=slug, condition_id=cid,
                token_id_up=t_up, token_id_down=t_down,
                open_epoch=epoch,
            ))
            if len(markets) >= count + 2:
                break

    markets.sort(key=lambda m: m.open_epoch)
    return markets[:count + 2]


def get_winner_from_csv(market):
    """Resolve UP/DOWN using the Chainlink oracle ticks recorded in the CSV.

    Picks the row whose oracle_ts is closest to open_epoch and close_epoch
    (in ms) rather than first/last row — avoids boundary deviation ticks
    that arrive 1-2s after t=300. Ties resolve to 'Up' (Polymarket >= rule).
    """
    try:
        open_ms = int(market.open_epoch * 1000)
        close_ms = int(market.close_epoch * 1000)

        with open(market.csv_path, "r") as f:
            reader = csv.reader(f)
            header = next(reader)
            try:
                price_col = header.index(market.price_col)
            except ValueError:
                print(f"  No {market.price_col} column in {market.slug}")
                return "unknown"
            try:
                ts_col = header.index(market.oracle_ts_col)
                has_ts = True
            except ValueError:
                ts_col = None
                has_ts = False

            best_open = None
            best_close = None
            first_price = None
            last_price = None

            for row in reader:
                if not row or row[0].startswith("#"):
                    continue
                try:
                    price = float(row[price_col])
                except (ValueError, IndexError):
                    continue
                if price <= 0:
                    continue
                if first_price is None:
                    first_price = price
                last_price = price

                if has_ts:
                    try:
                        ts = float(row[ts_col])
                    except (ValueError, IndexError):
                        continue
                    if ts <= 0:
                        continue
                    od = abs(ts - open_ms)
                    cd = abs(ts - close_ms)
                    if best_open is None or od < best_open[0]:
                        best_open = (od, price)
                    if best_close is None or cd < best_close[0]:
                        best_close = (cd, price)

        if has_ts and best_open is not None and best_close is not None:
            open_price, close_price = best_open[1], best_close[1]
            print(f"  [{market.name.upper()}] Oracle: open={open_price} close={close_price} "
                  f"(open_dist={best_open[0]:.0f}ms close_dist={best_close[0]:.0f}ms)")
        elif first_price is not None and last_price is not None:
            open_price, close_price = first_price, last_price
            print(f"  [{market.name.upper()}] Oracle: open={open_price} close={close_price} "
                  f"(no oracle_ts, fallback to first/last)")
        else:
            print(f"  [{market.name.upper()}] No prices in CSV for {market.slug}")
            return "unknown"

        return "Up" if close_price >= open_price else "Down"
    except Exception as e:
        print(f"  [{market.name.upper()}] winner check failed: {e}")
        return "unknown"


async def chainlink_listener(oracle_state, callbacks, stop_event):
    """Single shared Chainlink WS. Dispatches ticks by symbol to per-market
    callbacks and maintains oracle_state[symbol] = {price, oracle_ts}."""
    ssl_context = ssl_ctx()
    watched = set(oracle_state.keys())
    print(f"  Chainlink feed starting for {sorted(watched)}")

    while not stop_event.is_set():
        try:
            async with websockets.connect(
                CHAINLINK_WS, ssl=ssl_context, ping_interval=None
            ) as ws:
                await ws.send("PING")
                sub_msg = {
                    "action": "subscribe",
                    "subscriptions": [{
                        "topic": "crypto_prices_chainlink",
                        "type": "update",
                        "filters": "",
                    }],
                }
                await ws.send(json.dumps(sub_msg))
                print("  Chainlink feed connected")
                last_ping = time.time()

                while not stop_event.is_set():
                    now_t = time.time()
                    if now_t - last_ping >= CHAINLINK_PING_INTERVAL:
                        await ws.send("PING")
                        last_ping = now_t

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                    except asyncio.TimeoutError:
                        continue

                    raw_str = raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")
                    stripped = raw_str.strip()
                    if not stripped or stripped.upper() in ("PONG", "PING"):
                        continue

                    try:
                        msg = json.loads(raw_str)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(msg, dict):
                        continue
                    if msg.get("topic", "") != "crypto_prices_chainlink":
                        continue
                    payload = msg.get("payload", {})
                    symbol = payload.get("symbol", "")
                    if symbol not in watched:
                        continue

                    price = payload.get("value")
                    oracle_ts = payload.get("timestamp")
                    if price is None:
                        continue
                    oracle_state[symbol]["price"] = round(float(price), 6)
                    if oracle_ts:
                        oracle_state[symbol]["oracle_ts"] = oracle_ts

                    cb = callbacks.get(symbol)
                    if cb is not None:
                        try:
                            cb()
                        except Exception as cb_err:
                            print(f"  chainlink cb error [{symbol}]: {cb_err}")

        except Exception as e:
            if stop_event.is_set():
                break
            print(f"  Chainlink WS error: {e}, reconnecting...")
            await asyncio.sleep(0.5)


def _apply_book_snapshot(state, side, bids, asks):
    """side = 'up' or 'down'. Populates top-N bids/asks in state."""
    b = sorted(((float(x["price"]), float(x["size"])) for x in bids),
               key=lambda p: -p[0])
    a = sorted(((float(x["price"]), float(x["size"])) for x in asks),
               key=lambda p: p[0])
    state[f"{side}_bid_levels"] = b[:BOOK_DEPTH]
    state[f"{side}_ask_levels"] = a[:BOOK_DEPTH]


async def seed_initial_state(market, state):
    """Fetch REST order books so CSV starts populated from t=0."""
    ssl_context = ssl_ctx()
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=ssl_context),
    ) as session:
        up_bids, up_asks = await fetch_order_book(session, market.token_id_up)
        down_bids, down_asks = await fetch_order_book(session, market.token_id_down)
        state["up_bid_levels"] = up_bids[:BOOK_DEPTH]
        state["up_ask_levels"] = up_asks[:BOOK_DEPTH]
        state["down_bid_levels"] = down_bids[:BOOK_DEPTH]
        state["down_ask_levels"] = down_asks[:BOOK_DEPTH]
    print(f"  [{market.name.upper()}] Seeded: "
          f"up {len(state['up_bid_levels'])}b/{len(state['up_ask_levels'])}a  "
          f"down {len(state['down_bid_levels'])}b/{len(state['down_ask_levels'])}a")


async def record_market(market, oracle_state, callbacks):
    """Subscribe to CLOB WS for this market's tokens and stream ticks to CSV.
    Also registers a Chainlink callback so oracle ticks trigger CSV writes."""
    ssl_context = ssl_ctx()

    wait_time = market.seconds_until_open - 10
    if wait_time > 0:
        print(f"  [{market.name.upper()}] waiting {wait_time:.0f}s for "
              f"{market.slug} to open...")
        await asyncio.sleep(wait_time)

    market.open_csv()

    state = {
        "up_bid_levels": [], "up_ask_levels": [],
        "down_bid_levels": [], "down_ask_levels": [],
    }

    # Callback: when Chainlink receives a tick for our symbol, write a row
    # using whatever book state we currently hold. Ensures every oracle tick
    # gets captured (within rate limit) without waiting for a CLOB event.
    sym = market.symbol
    def on_oracle_tick():
        if market.csv_writer is None:
            return
        market.write_tick(state, oracle_state[sym])
    callbacks[sym] = on_oracle_tick

    await seed_initial_state(market, state)

    sleep_until_open = market.open_epoch - time.time()
    if sleep_until_open > 0:
        await asyncio.sleep(sleep_until_open)

    market.write_tick(state, oracle_state[sym])

    token_map = {market.token_id_up: "up", market.token_id_down: "down"}

    try:
        # Reconnect loop — if the CLOB WS drops mid-window (common on thin
        # markets after periods of inactivity) we reconnect and keep recording
        # until the window ends. Chainlink callback stays registered across
        # reconnects so oracle price writes continue uninterrupted.
        while time.time() < market.close_epoch + 1:
            try:
                async with websockets.connect(
                    CLOB_WS, ssl=ssl_context, ping_interval=20
                ) as ws:
                    sub_msg = {
                        "type": "market",
                        "assets_ids": [market.token_id_up, market.token_id_down],
                    }
                    await ws.send(json.dumps(sub_msg))
                    print(f"  [{market.name.upper()}] WS subscribed for {market.slug}")

                    while time.time() < market.close_epoch + 1:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                        except asyncio.TimeoutError:
                            continue
                        try:
                            msgs = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(msgs, list):
                            msgs = [msgs]

                        for msg in msgs:
                            event_type = msg.get("event_type", "")
                            asset_id = msg.get("asset_id", "")
                            if asset_id not in token_map:
                                continue
                            side = token_map[asset_id]

                            if event_type == "book":
                                _apply_book_snapshot(state, side,
                                                     msg.get("bids", []), msg.get("asks", []))
                                market.write_tick(state, oracle_state[sym])
            except Exception as ws_err:
                # If the window's still open, reconnect. Otherwise exit cleanly.
                if time.time() < market.close_epoch + 1:
                    print(f"  [{market.name.upper()}] CLOB WS dropped: {ws_err}, "
                          f"reconnecting...")
                    await asyncio.sleep(0.5)
                else:
                    break
    finally:
        # Stop receiving oracle ticks for this symbol
        callbacks.pop(sym, None)
        market.close_csv()
        print(f"  [{market.name.upper()}] recorded {market.tick_count} ticks "
              f"for {market.slug}")


async def run_market_family(cfg, num_windows, oracle_state, callbacks):
    """One market family (e.g. DOGE): discover next N windows, record each
    sequentially (windows are back-to-back in time)."""
    try:
        markets = await discover_next_markets(cfg, num_windows)
    except Exception as e:
        print(f"  [{cfg['name'].upper()}] discovery failed: {e}")
        return []

    if not markets:
        print(f"  [{cfg['name'].upper()}] no upcoming markets found")
        return []

    print(f"  [{cfg['name'].upper()}] found {len(markets)} upcoming markets")
    now = time.time()
    to_record = [m for m in markets if m.open_epoch > now - 10][:num_windows]
    if not to_record:
        to_record = markets[-num_windows:]

    for m in to_record:
        open_str = datetime.fromtimestamp(m.open_epoch, tz=timezone.utc).strftime('%H:%M')
        close_str = datetime.fromtimestamp(m.close_epoch, tz=timezone.utc).strftime('%H:%M')
        print(f"  [{cfg['name'].upper()}] queued {m.slug} ({open_str} → {close_str} UTC)")

    # Record sequentially — windows don't overlap in time.
    # Each call returns when the window closes (~5 min).
    for m in to_record:
        try:
            await record_market(m, oracle_state, callbacks)
        except Exception as e:
            print(f"  [{cfg['name'].upper()}] record error on {m.slug}: {e}")

    return to_record


async def main():
    parser = argparse.ArgumentParser(description="Multi-Market 5m Price Tick Collector")
    parser.add_argument("--windows", type=int, default=3,
                        help="Number of 5-minute windows to record per market")
    parser.add_argument("--markets", type=str, default="",
                        help="Comma-separated subset (e.g. 'doge,hype'). Empty = all.")
    args = parser.parse_args()

    CSV_DIR.mkdir(parents=True, exist_ok=True)

    if args.markets:
        wanted = {m.strip().lower() for m in args.markets.split(",") if m.strip()}
        cfgs = [c for c in MARKETS if c["name"] in wanted]
    else:
        cfgs = list(MARKETS)

    print("=" * 60)
    print("Multi-Market 5m Tick Collector (Chainlink Oracle)")
    print("=" * 60)
    print(f"Start: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Markets: {[c['name'] for c in cfgs]}")
    print(f"Windows per market: {args.windows}")
    print()

    # Shared Chainlink state (one entry per symbol we watch)
    oracle_state = {c["symbol"]: {"price": None, "oracle_ts": None} for c in cfgs}
    callbacks = {}   # symbol -> fn (set per active window by record_market)
    cl_stop = asyncio.Event()
    cl_task = asyncio.create_task(
        chainlink_listener(oracle_state, callbacks, cl_stop)
    )

    try:
        # Run each market family concurrently (but within a family windows run sequentially).
        tasks = [
            asyncio.create_task(run_market_family(cfg, args.windows, oracle_state, callbacks))
            for cfg in cfgs
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        cl_stop.set()
        cl_task.cancel()
        try:
            await cl_task
        except asyncio.CancelledError:
            pass

    print("\nDetermining winners from Chainlink oracle data...")
    total = 0
    for res in results:
        if isinstance(res, Exception) or not res:
            continue
        for market in res:
            winner = get_winner_from_csv(market)
            market.winner = winner
            print(f"  [{market.name.upper()}] {market.slug} → Winner: {winner}")
            try:
                with open(market.csv_path, "a", newline="") as f:
                    w = csv.writer(f)
                    w.writerow([])
                    w.writerow(["# RESULT", f"winner={winner}", f"slug={market.slug}",
                                f"ticks={market.tick_count}"])
            except Exception as e:
                print(f"  [{market.name.upper()}] append result failed: {e}")
            total += 1

    print("\n" + "=" * 60)
    print(f"COLLECTION COMPLETE — {total} windows recorded")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
