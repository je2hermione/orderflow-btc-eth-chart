"""
Binance tick recorder -> Parquet.
Records live trades via WebSocket, flushes to Parquet per symbol per day.

Usage:
    python recorder.py [--symbols btcusdt,ethusdt] [--data-dir /data]

Output:
    /data/trades_{symbol}_{YYYY-MM-DD}.parquet
    Columns: ts (ms), price (float), qty (float), is_sell (bool)
             is_sell=True means the taker was a seller (maker buyer, 'm'=True in Binance trade stream).
"""
import argparse, asyncio, json, os, sys, time
from collections import defaultdict
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import websockets

BINANCE_WS = "wss://data-stream.binance.vision"
FALLBACK_WS = "wss://stream.binance.com:9443"

# flush every N trades or every M seconds, whichever first
FLUSH_TRADES = 20000
FLUSH_SECS = 60


class SymbolBuffer:
    def __init__(self, symbol, data_dir):
        self.symbol = symbol
        self.data_dir = data_dir
        self.rows = []  # (ts, price, qty, is_sell)
        self.last_flush = time.time()
        self.total = 0

    def add(self, ts, price, qty, is_sell):
        self.rows.append((ts, price, qty, is_sell))
        self.total += 1

    def should_flush(self):
        return len(self.rows) >= FLUSH_TRADES or (self.rows and time.time() - self.last_flush > FLUSH_SECS)

    def flush(self):
        if not self.rows:
            return
        # group by day (UTC)
        by_day = defaultdict(list)
        for ts, price, qty, is_sell in self.rows:
            day = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            by_day[day].append((ts, price, qty, is_sell))
        for day, rows in by_day.items():
            path = os.path.join(self.data_dir, f"trades_{self.symbol}_{day}.parquet")
            tbl = pa.table({
                "ts": [r[0] for r in rows],
                "price": [r[1] for r in rows],
                "qty": [r[2] for r in rows],
                "is_sell": [r[3] for r in rows],
            })
            if os.path.exists(path):
                # append: read existing, concat, rewrite
                old = pq.read_table(path)
                tbl = pa.concat_tables([old, tbl])
            pq.write_table(tbl, path, compression="snappy")
            print(f"[{self.symbol}] flushed {len(rows)} trades -> {os.path.basename(path)} (total {self.total})", flush=True)
        self.rows = []
        self.last_flush = time.time()


async def run_symbol(symbol, data_dir, stop_event):
    buf = SymbolBuffer(symbol, data_dir)
    stream = f"{symbol}@trade"
    url = f"{BINANCE_WS}/ws/{stream}"
    backoff = 1
    while not stop_event.is_set():
        try:
            print(f"[{symbol}] connecting {url}", flush=True)
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=10) as ws:
                print(f"[{symbol}] connected", flush=True)
                backoff = 1
                async for msg in ws:
                    if stop_event.is_set():
                        break
                    try:
                        d = json.loads(msg)
                        # Binance trade stream: T=trade time, p=price, q=qty, m=is buyer maker
                        # m=True -> seller was taker -> is_sell=True
                        buf.add(int(d["T"]), float(d["p"]), float(d["q"]), bool(d["m"]))
                    except (KeyError, ValueError):
                        continue
                    if buf.should_flush():
                        buf.flush()
        except Exception as e:
            print(f"[{symbol}] ws error: {e}, retry in {backoff}s", flush=True)
            # try fallback on first failure
            if url.startswith(BINANCE_WS):
                url = f"{FALLBACK_WS}/ws/{stream}"
                print(f"[{symbol}] trying fallback {url}", flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
    buf.flush()
    print(f"[{symbol}] stopped", flush=True)


async def main(symbols, data_dir):
    os.makedirs(data_dir, exist_ok=True)
    stop = asyncio.Event()
    tasks = [asyncio.create_task(run_symbol(s, data_dir, stop)) for s in symbols]
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        stop.set()
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default="btcusdt,ethusdt")
    ap.add_argument("--data-dir", default=os.environ.get("DATA_DIR", "/data"))
    a = ap.parse_args()
    syms = [s.strip().lower() for s in a.symbols.split(",") if s.strip()]
    asyncio.run(main(syms, a.data_dir))
