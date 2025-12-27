#!/bin/bash

set -e

echo "[+] Starting MLFlow deployment"

# Validate required environment variables
if [ -z "$MLFLOW_BACKEND_STORE_URI" ]; then
    echo "[-] ERROR: MLFLOW_BACKEND_STORE_URI not set"
    exit 1
fi

if [ -z "$MLFLOW_DEFAULT_ARTIFACT_ROOT" ]; then
    echo "[-] ERROR: MLFLOW_DEFAULT_ARTIFACT_ROOT not set"
    exit 1
fi

# Start MLFlow server in background
echo "[+] Starting MLFlow server on port 5000"
mlflow server \
    --backend-store-uri "$MLFLOW_BACKEND_STORE_URI" \
    --default-artifact-root "$MLFLOW_DEFAULT_ARTIFACT_ROOT" \
    --serve-artifacts \
    --host 0.0.0.0 \
    --port 5000 \
    --gunicorn-opts "--timeout 300 --workers 2" &

MLFLOW_PID=$!
echo "[+] MLFlow server started (PID: $MLFLOW_PID)"

# Wait briefly for MLFlow to be reachable, but never fail the container startup.
# Cloud Run requires this container to start listening on $PORT; MLflow may take longer
# to initialize (e.g., Cloud SQL connector permissions / readiness).
echo "[+] Waiting briefly for MLFlow server to be reachable"
for i in {1..30}; do
    if curl -sf http://localhost:5000/ > /dev/null 2>&1; then
        echo "[+] MLFlow server is reachable"
        break
    fi
    sleep 2
done
if [ "$i" -eq 30 ]; then
    echo "[!] MLFlow server not reachable yet; starting auth proxy anyway"
fi

# Start authentication proxy
echo "[+] Starting authentication proxy on port ${PORT:-8080}"
exec gunicorn \
    --bind 0.0.0.0:"${PORT:-8080}" \
    --workers 2 \
    --timeout 300 \
    --access-logfile - \
    --error-logfile - \
    auth_wrapper:app
