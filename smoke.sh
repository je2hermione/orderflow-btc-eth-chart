#!/usr/bin/env bash
# smoke.sh — verifikasi pasca-deploy orderflow di VPS. Read-only, tidak mengubah apa pun.
# Jalankan di VPS setelah: docker compose up -d --build
# Keluar 0 kalau semua PASS, 1 kalau ada FAIL. WARN tidak menggagalkan.
set -u
PASS=0; WARN=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "PASS  $1"; }
warn() { WARN=$((WARN+1)); echo "WARN  $1"; }
fail() { FAIL=$((FAIL+1)); echo "FAIL  $1"; }

# 1. container jalan
if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx 'orderflow'; then
  ok "container 'orderflow' running"
else
  fail "container 'orderflow' tidak running (docker compose up -d --build dulu)"
fi

# 2. port 8000 HANYA di 127.0.0.1 (bukan 0.0.0.0) — temuan audit WAN exposure
BIND=$(ss -tlnp 2>/dev/null | grep ':8000 ' || true)
if echo "$BIND" | grep -q '127.0.0.1:8000'; then
  ok "port 8000 bind ke 127.0.0.1 saja"
else
  fail "port 8000 tidak di 127.0.0.1 — cek docker-compose.yml (temuan audit!)"
fi
if echo "$BIND" | grep -q '0.0.0.0:8000'; then
  fail "port 8000 TEREXPOSE ke 0.0.0.0 — tutup sebelum lanjut"
fi

# 3. /api/health
HEALTH=$(curl -s -m 10 http://127.0.0.1:8000/api/health || true)
if echo "$HEALTH" | grep -q '"ok":true'; then
  ok "GET /api/health -> $HEALTH"
  if echo "$HEALTH" | grep -q '"recorder_alive":true'; then
    ok "recorder_alive=true (WS Binance hidup)"
  else
    warn "recorder_alive=false — recorder baru start / WS reconnect; cek lagi 1-2 menit (docker compose logs)"
  fi
else
  fail "GET /api/health gagal: ${HEALTH:-no response}"
fi

# 4. /api/footprint mengembalikan candle (seed 8 hari harusnya langsung ada)
FP=$(curl -s -m 15 -o /dev/null -w '%{http_code}' 'http://127.0.0.1:8000/api/footprint?symbol=btcusdt&tf=5m&limit=5' || true)
if [ "$FP" = "200" ]; then
  ok "GET /api/footprint btcusdt 5m -> 200"
elif [ "$FP" = "404" ]; then
  warn "GET /api/footprint -> 404 no trade data yet (seed belum jalan / first-run)"
else
  fail "GET /api/footprint -> HTTP $FP"
fi

# 5. rate limit terpasang (lebih dari 120 req/menit dari 1 IP harus kena 429)
#    — dicek ringan: 130 request cepat, hitung berapa yang 429. (opsional, bisa lambat ~detik)
if [ "${SMOKE_RATELIMIT:-0}" = "1" ]; then
  R429=$(for i in $(seq 1 130); do curl -s -m 5 -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8000/api/health; done | grep -c '^429$' || true)
  if [ "$R429" -gt 0 ]; then ok "rate limit aktif ($R429x 429 dari 130 req)"; else warn "rate limit tidak ter-trigger (cek manual)"; fi
else
  echo "SKIP  rate-limit check (set SMOKE_RATELIMIT=1 untuk jalankan)"
fi

echo "---"
echo "ringkasan: $PASS pass, $WARN warn, $FAIL fail"
[ "$FAIL" -eq 0 ]
