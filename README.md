# Orderflow Deployable

DIY ATAS-style orderflow footprint tool untuk Binance — versi deployable dengan backend.

Frontend: footprint 3-pane (footprint / volume / delta+CVD), prediksi POC-wall, panel orderbook Polymarket.
Backend: tick recorder (WebSocket → Parquet) + FastAPI (footprint agregat + proxy Polymarket).

## Arsitektur

```
┌─────────────┐     WS trade      ┌──────────────┐
│   Binance   │ ───────────────► │   recorder   │ ──► /data/*.parquet
└─────────────┘                  └──────────────┘
                                        │ DuckDB
                                        ▼
┌─────────────┐   /api/footprint  ┌──────────────┐   /api/pm/*   ┌────────────┐
│  Browser    │ ◄─────────────── │   FastAPI    │ ◄──────────── │ Polymarket │
│ (live.html) │   static + API    │   :8000      │   server-side │  (no CORS) │
└─────────────┘                  └──────────────┘               └────────────┘
```

- **Recorder** (`recorder/recorder.py`): langganan `xxx@trade` via WebSocket, flush ke Parquet per symbol per hari (`trades_{symbol}_{YYYY-MM-DD}.parquet`). Auto-reconnect + fallback endpoint.
- **API** (`api/main.py`): agregat footprint per TF via DuckDB langsung dari Parquet. Proxy Polymarket server-side (tidak perlu CORS proxy).
- **Seed** (`recorder/seed.py`): di first-run, convert history 8 hari dari `web/live-legacy.html` jadi Parquet supaya langsung ada data. Setelah itu recorder yang lanjutkan.

## Deploy

Butuh: Docker + Docker Compose di VPS.

```bash
git clone <repo-url> orderflow-deploy
cd orderflow-deploy
docker compose up -d --build
```

Buka `http://<vps-ip>:8000`.

### Konfigurasi

Edit `docker-compose.yml`:
- `SYMBOLS`: daftar symbol, misal `btcusdt,ethusdt,solusdt`
- `ports`: ganti port host kalau perlu
- Volume `orderflow-data` menyimpan Parquet (persist antar restart).

### Cek kesehatan

- `GET /api/health` → jumlah file Parquet + ukuran data
- `GET /api/footprint?symbol=btcusdt&tf=5m&limit=200` → candle footprint JSON
- Log recorder: `docker compose logs -f`

## Data

- Format Parquet: `ts` (ms), `price`, `qty`, `is_sell` (true = taker seller).
- Ukuran: ~50 MB/hari/symbol (BTC). 30 hari ≈ 1.5 GB/symbol.
- Flush tiap 60 detik / 20k trades. File per hari (UTC).

## API

| Endpoint | Deskripsi |
|---|---|
| `GET /api/footprint?symbol=&tf=&limit=&end=` | Candle footprint: `time,o,h,l,c,levels[[price,buy,sell]]` |
| `GET /api/pm/event?slug=` | Proxy Polymarket event (orderbook discovery) |
| `GET /api/pm/book?token_id=` | Proxy Polymarket CLOB orderbook |
| `GET /api/health` | Status recorder/data |

TF: `1m 5m 15m 30m 1h 4h`. Bucket otomatis per symbol/TF (sama seperti prototype).

## Catatan

- Data seed (8 hari) adalah rekonstruksi dari level agregat, bukan tick-perfect. Data recorder live 100% real.
- Prediksi POC-wall di frontend adalah heuristik, bukan sinyal trading teruji. Backtest 1h: sinyal "jelas" 73–83% (tapi jarang); 5m: tidak reliable.
- Jangan expose port 8000 ke publik tanpa reverse proxy + auth kalau tidak perlu.
