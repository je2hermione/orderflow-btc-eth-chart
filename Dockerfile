FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    DATA_DIR=/data \
    WEB_DIR=/app/web

WORKDIR /app

RUN pip install --no-cache-dir \
    fastapi==0.115.* \
    uvicorn==0.30.* \
    websockets==13.* \
    pyarrow==17.* \
    duckdb==1.1.*

COPY recorder/ /app/recorder/
COPY api/ /app/api/
COPY web/ /app/web/

VOLUME ["/data"]
EXPOSE 8000

# seed history on first run (if no data), then start recorder + api
CMD sh -c 'if [ -z "$(ls -A /data 2>/dev/null)" ] && [ -f /app/web/live-legacy.html ]; then \
      echo "seeding history..."; python /app/recorder/seed.py --html /app/web/live-legacy.html --data-dir /data; \
    fi; \
    python /app/recorder/recorder.py --data-dir /data --symbols "${SYMBOLS:-btcusdt,ethusdt}" & \
    uvicorn api.main:app --host 0.0.0.0 --port 8000 --app-dir /app'
