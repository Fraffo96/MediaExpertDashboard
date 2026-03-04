# Dashboard Media Expert – nostra app (non Metabase)
FROM python:3.11-slim

WORKDIR /app

COPY app/requirements.txt ./app/
RUN pip install --no-cache-dir -r app/requirements.txt

COPY app/ ./app/
# Porta esposta per Cloud Run (usa variabile PORT)
ENV PORT=8080
EXPOSE 8080

# Cloud Run imposta PORT a runtime
CMD uvicorn app.main:app --host 0.0.0.0 --port ${PORT}
