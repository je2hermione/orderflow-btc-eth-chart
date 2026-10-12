"""
Seed Parquet trade data from the embedded 1h history in the prototype HTML.
This gives instant 8-day history on first deploy; the live recorder then
takes over for new data.

Reconstruction is approximate: per-level buy/sell volumes are spread as
synthetic trades uniformly across each hour. Good enough for footprint
visualization, POC, and delta — not tick-perfect.

Usage:
    python seed.py --html /app/web/live-legacy.html --data-dir /data
"""
import argparse, json, os, re, sys

import pyarrow as pa
import pyarrow.parquet as pq


def main(html_path, data_dir):
    html = open(html_path).read()
    idx = html.find("const HISTORY = ")
    if idx < 0:
        print("HISTORY not found in html", file=sys.stderr); sys.exit(1)
    dec = json.JSONDecoder()
    hist, _ = dec.raw_decode(html[idx + 16:])
    os.makedirs(data_dir, exist_ok=True)

    for symbol in ("btcusdt", "ethusdt"):
        arr = (hist.get(symbol) or {}).get("1h") or []
        # group synthetic trades by HOUR (matches recorder partitioning)
        by_hour = {}
        for h in arr:
            t0 = h["time"]  # seconds
            levels = h.get("levels") or []
            n_lv = max(1, len(levels))
            for li, (px, buy, sell) in enumerate(levels):
                for side, vol, is_sell in (("b", buy, False), ("s", sell, True)):
                    if vol <= 0:
                        continue
                    chunk = 0.5
                    n = max(1, int(vol / chunk))
                    per = vol / n
                    for k in range(n):
                        frac = (li / n_lv + k / n) % 1.0
                        ts = int((t0 + frac * 3600) * 1000)
                        hr = __import__("datetime").datetime.fromtimestamp(
                            ts / 1000, tz=__import__("datetime").timezone.utc).strftime("%Y-%m-%d_%H")
                        by_hour.setdefault(hr, []).append((ts, float(px), float(per), is_sell))
        for hr, rows in sorted(by_hour.items()):
            rows.sort()
            path = os.path.join(data_dir, f"trades_{symbol}_{hr}.parquet")
            tbl = pa.table({
                "ts": [r[0] for r in rows],
                "price": [r[1] for r in rows],
                "qty": [r[2] for r in rows],
                "is_sell": [r[3] for r in rows],
            })
            pq.write_table(tbl, path, compression="snappy")
            print(f"seeded {symbol} {hr}: {len(rows)} synthetic trades -> {os.path.basename(path)}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--html", required=True)
    ap.add_argument("--data-dir", default=os.environ.get("DATA_DIR", "/data"))
    a = ap.parse_args()
    main(a.html, a.data_dir)
