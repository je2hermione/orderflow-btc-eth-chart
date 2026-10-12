"""
Orderflow API server.
- GET /api/footprint?symbol=btcusdt&tf=5m&limit=200  -> footprint candles from Parquet
- GET /api/pm/event?slug=...                        -> Polymarket event (proxied, no CORS issue)
- GET /api/pm/book?token_id=...                     -> Polymarket orderbook (proxied)
- Serves frontend static files at /

Run: uvicorn main:app --host 0.0.0.0 --port 8000

Security notes:
- Designed to run behind a reverse proxy (nginx) with auth (Authelia).
  Do NOT expose directly to WAN. docker-compose binds 127.0.0.1 by default.
- Polymarket proxy params are strictly validated (regex + length).
- Upstream responses are size-limited (1 MB) and redirects are NOT followed.
"""
import glob
import json
import logging
import os
import re
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

import duckdb
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

log = logging.getLogger("orderflow-api")

DATA_DIR = os.environ.get("DATA_DIR", "/data")
WEB_DIR = os.environ.get("WEB_DIR", "/app/web")
HEARTBEAT_FILE = os.path.join(DATA_DIR, ".recorder_heartbeat")
HEARTBEAT_MAX_AGE = 300  # seconds; recorder updates every flush (<=60s)

TF_MS = {
    "1m": 60_000, "5m": 300_000, "15m": 900_000,
    "30m": 1_800_000, "1h": 3_600_000, "4h": 14_400_000,
}
BUCKETS = {
    "btcusdt": {"1m": 10, "5m": 10, "15m": 25, "30m": 25, "1h": 50, "4h": 50},
    "ethusdt": {"1m": 0.5, "5m": 0.5, "15m": 1, "30m": 1, "1h": 2.5, "4h": 2.5},
}

# strict patterns for proxy params (SSRF mitigation)
SLUG_RE = re.compile(r"^[a-z0-9-]{1,120}$")
TOKEN_RE = re.compile(r"^[0-9]{1,78}$")
MAX_UPSTREAM_BYTES = 1_000_000

# simple in-memory rate limiter: 120 req/min per IP
RATE_LIMIT = 120
_rate = defaultdict(list)

app = FastAPI(title="Orderflow API")


@app.middleware("http")
async def rate_limit_mw(request: Request, call_next):
    ip = request.client.host if request.client else "?"
    now = time.time()
    q = _rate[ip]
    q[:] = [t for t in q if now - t < 60]
    if len(q) >= RATE_LIMIT:
        raise HTTPException(429, "rate limit exceeded")
    q.append(now)
    return await call_next(request)


def _pq_files(symbol):
    return sorted(glob.glob(os.path.join(DATA_DIR, f"trades_{symbol}_*.parquet")))


def _recorder_alive():
    try:
        age = time.time() - os.path.getmtime(HEARTBEAT_FILE)
        return age < HEARTBEAT_MAX_AGE
    except OSError:
        return False


@app.get("/api/health")
def health():
    files = glob.glob(os.path.join(DATA_DIR, "trades_*.parquet"))
    total_mb = sum(os.path.getsize(f) for f in files) / 1e6
    alive = _recorder_alive()
    return {"ok": True, "recorder_alive": alive,
            "files": len(files), "data_mb": round(total_mb, 1)}


@app.get("/api/footprint")
def footprint(
    symbol: str = Query(..., pattern="^(btcusdt|ethusdt)$"),
    tf: str = Query(..., pattern="^(1m|5m|15m|30m|1h|4h)$"),
    limit: int = Query(200, ge=1, le=500),
    end: int = Query(None, description="end timestamp ms (exclusive), default now"),
):
    """Return footprint candles. Each candle: time, o/h/l/c, levels [[price, buy, sell]...]."""
    files = _pq_files(symbol)
    if not files:
        raise HTTPException(404, "no trade data yet")
    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    end_ms = end or now_ms
    # validate end: not absurdly far future/past
    if end_ms > now_ms + 86_400_000 or end_ms < 1_577_836_800_000:  # >+1d or <2020-01-01
        raise HTTPException(400, "invalid end timestamp")
    ms = TF_MS[tf]
    bucket = BUCKETS[symbol][tf]
    start_ms = end_ms - limit * ms

    con = duckdb.connect()
    try:
        q = """
        SELECT
            (ts - (ts % $ms)) AS bkt,
            CAST(FLOOR(price / $bucket) * $bucket AS DOUBLE) AS lvl,
            SUM(CASE WHEN is_sell THEN 0 ELSE qty END) AS buy,
            SUM(CASE WHEN is_sell THEN qty ELSE 0 END) AS sell
        FROM read_parquet($files)
        WHERE ts >= $start_ms AND ts < $end_ms
        GROUP BY bkt, lvl
        ORDER BY bkt, lvl
        """
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

    by_bkt = {}
    for bkt, lvl, buy, sell in lv_rows:
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


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None  # do not follow redirects


_opener = urllib.request.build_opener(_NoRedirect)


def _http_get(url, timeout=15):
    """Fetch upstream with size limit, no redirects. Raises on error."""
    req = urllib.request.Request(url, headers={"User-Agent": "orderflow-backend/1.0"})
    with _opener.open(req, timeout=timeout) as r:
        chunks, total = [], 0
        while True:
            c = r.read(65536)
            if not c:
                break
            total += len(c)
            if total > MAX_UPSTREAM_BYTES:
                raise ValueError("upstream response too large")
            chunks.append(c)
        return json.loads(b"".join(chunks))


@app.get("/api/pm/event")
def pm_event(slug: str = Query(..., min_length=1, max_length=120)):
    """Proxy Polymarket gamma event (avoids browser CORS)."""
    if not SLUG_RE.match(slug):
        raise HTTPException(400, "invalid slug")
    url = "https://gamma-api.polymarket.com/events/slug/" + urllib.parse.quote(slug, safe="")
    try:
        return _http_get(url)
    except Exception as e:
        log.warning("pm event failed for %s: %s", slug, e)
        raise HTTPException(502, "polymarket upstream error")


@app.get("/api/pm/book")
def pm_book(token_id: str = Query(..., min_length=1, max_length=78)):
    """Proxy Polymarket CLOB orderbook (avoids browser CORS)."""
    if not TOKEN_RE.match(token_id):
        raise HTTPException(400, "invalid token_id")
    url = "https://clob.polymarket.com/book?token_id=" + urllib.parse.quote(token_id, safe="")
    try:
        return _http_get(url)
    except Exception as e:
        log.warning("pm book failed: %s", e)
        raise HTTPException(502, "polymarket upstream error")


if os.path.isdir(WEB_DIR):
    app.mount("/", StaticFiles(directory=WEB_DIR, html=True), name="web")
else:
    @app.get("/")
    def no_web():
        return {"ok": True, "note": "web dir not mounted"}
