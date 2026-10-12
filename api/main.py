"""
Orderflow API server.
- GET /api/footprint?symbol=btcusdt&tf=5m&limit=200  -> footprint candles from Parquet
- GET /api/pm/event?slug=...                        -> Polymarket event (proxied, no CORS issue)
- GET /api/pm/book?token_id=...                     -> Polymarket orderbook (proxied)
- Serves frontend static files at /

Run: uvicorn main:app --host 0.0.0.0 --port 8000
"""
import glob
import json
import os
import urllib.request
from datetime import datetime, timezone

import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

DATA_DIR = os.environ.get("DATA_DIR", "/data")
WEB_DIR = os.environ.get("WEB_DIR", "/app/web")

TF_MS = {
    "1m": 60_000, "5m": 300_000, "15m": 900_000,
    "30m": 1_800_000, "1h": 3_600_000, "4h": 14_400_000,
}
# bucket size per symbol per tf (matches frontend)
BUCKETS = {
    "btcusdt": {"1m": 10, "5m": 10, "15m": 25, "30m": 25, "1h": 50, "4h": 50},
    "ethusdt": {"1m": 0.5, "5m": 0.5, "15m": 1, "30m": 1, "1h": 2.5, "4h": 2.5},
}

app = FastAPI(title="Orderflow API")


def _pq_files(symbol):
    return sorted(glob.glob(os.path.join(DATA_DIR, f"trades_{symbol}_*.parquet")))


@app.get("/api/health")
def health():
    files = glob.glob(os.path.join(DATA_DIR, "trades_*.parquet"))
    total_mb = sum(os.path.getsize(f) for f in files) / 1e6
    return {"ok": True, "files": len(files), "data_mb": round(total_mb, 1)}


@app.get("/api/footprint")
def footprint(
    symbol: str = Query(..., pattern="^(btcusdt|ethusdt)$"),
    tf: str = Query(..., pattern="^(1m|5m|15m|30m|1h|4h)$"),
    limit: int = Query(200, ge=1, le=1000),
    end: int = Query(None, description="end timestamp ms (exclusive), default now"),
):
    """Return footprint candles. Each candle: time, o/h/l/c, levels [[price, buy, sell]...]."""
    files = _pq_files(symbol)
    if not files:
        raise HTTPException(404, f"no trade data for {symbol} yet (recorder still warming up)")
    ms = TF_MS[tf]
    bucket = BUCKETS[symbol][tf]
    end_ms = end or int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    start_ms = end_ms - limit * ms

    con = duckdb.connect()
    try:
        # read trades in range, bucketize price levels in SQL
        q = """
        SELECT
            (ts - (ts % $ms)) AS bkt,
            CAST(FLOOR(price / $bucket) * $bucket AS DOUBLE) AS lvl,
            SUM(CASE WHEN is_sell THEN 0 ELSE qty END) AS buy,
            SUM(CASE WHEN is_sell THEN qty ELSE 0 END) AS sell,
            MIN(price) AS mn, MAX(price) AS mx,
            MIN(ts) AS first_ts, MAX(ts) AS last_ts
        FROM read_parquet($files)
        WHERE ts >= $start_ms AND ts < $end_ms
        GROUP BY bkt, lvl
        ORDER BY bkt, lvl
        """
        # first/last price per candle need ordered agg; do a second query for OHLC
        ohlc_q = """
        SELECT bkt, MIN(price) AS lo, MAX(price) AS hi
        FROM (
            SELECT (ts - (ts % $ms)) AS bkt, price, ts,
                   ROW_NUMBER() OVER (PARTITION BY (ts - (ts % $ms)) ORDER BY ts) AS rn_first,
                   ROW_NUMBER() OVER (PARTITION BY (ts - (ts % $ms)) ORDER BY ts DESC) AS rn_last
            FROM read_parquet($files)
            WHERE ts >= $start_ms AND ts < $end_ms
        )
        GROUP BY bkt
        """
        # simpler: get open/close via arg_min/arg_max on ts
        oc_q = """
        SELECT bkt,
               MIN(price) AS lo, MAX(price) AS hi,
               ARG_MIN(price, ts) AS op, ARG_MAX(price, ts) AS cl
        FROM (
            SELECT (ts - (ts % $ms)) AS bkt, price, ts
            FROM read_parquet($files)
            WHERE ts >= $start_ms AND ts < $end_ms
        )
        GROUP BY bkt
        ORDER BY bkt
        """
        params = {"ms": ms, "bucket": bucket, "files": files,
                  "start_ms": start_ms, "end_ms": end_ms}
        lv_rows = con.execute(q, params).fetchall()
        oc_params = {"ms": ms, "files": files, "start_ms": start_ms, "end_ms": end_ms}
        oc_rows = {r[0]: r[1:] for r in con.execute(oc_q, oc_params).fetchall()}
    finally:
        con.close()

    # assemble candles
    by_bkt = {}
    for bkt, lvl, buy, sell, _mn, _mx, _fts, _lts in lv_rows:
        by_bkt.setdefault(bkt, []).append([lvl, buy or 0, sell or 0])
    candles = []
    for bkt in sorted(by_bkt):
        lo, hi, op, cl = oc_rows.get(bkt, (None, None, None, None))
        if lo is None:
            continue
        candles.append({
            "time": bkt // 1000,
            "open": op, "high": hi, "low": lo, "close": cl,
            "levels": by_bkt[bkt],
        })
    return {"symbol": symbol, "tf": tf, "candles": candles[-limit:]}


def _http_get(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "orderflow-backend/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


@app.get("/api/pm/event")
def pm_event(slug: str = Query(...)):
    """Proxy Polymarket gamma event (avoids browser CORS)."""
    try:
        return _http_get(f"https://gamma-api.polymarket.com/events/slug/{slug}")
    except Exception as e:
        raise HTTPException(502, f"polymarket gamma error: {e}")


@app.get("/api/pm/book")
def pm_book(token_id: str = Query(...)):
    """Proxy Polymarket CLOB orderbook (avoids browser CORS)."""
    try:
        return _http_get(f"https://clob.polymarket.com/book?token_id={token_id}")
    except Exception as e:
        raise HTTPException(502, f"polymarket clob error: {e}")


# frontend static
if os.path.isdir(WEB_DIR):
    app.mount("/", StaticFiles(directory=WEB_DIR, html=True), name="web")
else:
    @app.get("/")
    def no_web():
        return {"ok": True, "note": "web dir not mounted"}
