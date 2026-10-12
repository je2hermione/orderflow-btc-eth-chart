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

## Keamanan

- **Jangan expose langsung ke WAN.** `docker-compose.yml` bind ke `127.0.0.1:8000` saja.
  Pasang di belakang reverse proxy (nginx/Caddy) + auth (Authelia/basic auth).
  API tidak punya autentikasi bawaan.
- **Polymarket proxy** divalidasi ketat (regex slug/token_id, tanpa redirect, limit 1 MB).
- **Rate limit** 120 req/menit per IP (in-memory).
- **Retention** default 30 hari (`RETENTION_DAYS`), cegah disk penuh.
- `/api/health` lapor `recorder_alive` (heartbeat). Kalau false, cek `docker compose logs`.

## Deploy

Butuh: Docker + Docker Compose di VPS.

```bash
git clone <repo-url> orderflow-deploy
cd orderflow-deploy
docker compose up -d --build
```

Buka lewat nginx reverse proxy + Authelia yang sudah ada — service hanya listen di
`127.0.0.1:8000`, jadi **tidak bisa** diakses langsung via `http://<vps-ip>:8000`.

Contoh server block (sesuaikan `server_name`, sertifikat, dan blok Authelia dengan setup kamu):

```nginx
server {
    listen 443 ssl;
    server_name orderflow.contoh.com;   # ganti dengan subdomain kamu
    # ssl_certificate / ssl_certificate_key ... (sertifikat kamu)

    # Authelia: pakai auth_request / include yang sama seperti service lain
    # include snippets/authelia.conf;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        # Tidak perlu websocket upgrade: browser konek langsung ke Binance (wss://stream.binance.com),
        # nginx cuma proxy static frontend + REST /api/*
    }
}
```

### Konfigurasi

Edit `docker-compose.yml`:
- `SYMBOLS`: daftar symbol, misal `btcusdt,ethusdt,solusdt`
- `ports`: ganti port host kalau perlu
- Volume `orderflow-data` menyimpan Parquet (persist antar restart).

### Cek kesehatan

- `GET /api/health` → `{"ok":true,"recorder_alive":true,"files":N,"data_mb":X}` (cek `recorder_alive` — kalau false, WS recorder mati)
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
