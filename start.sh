#!/bin/bash
# ELVIN VPS API 起動スクリプト
# 使い方: bash start.sh

export DAEMON_SECRET="${DAEMON_SECRET:-changeme}"
export PORT="${PORT:-5050}"

echo "[ELVIN] Starting on port $PORT"
echo "[ELVIN] DAEMON_SECRET=$DAEMON_SECRET"

# screen セッションが既に存在する場合は停止
screen -S elvin -X quit 2>/dev/null

# screen で起動（ターミナルを閉じても動き続ける）
screen -dmS elvin bash -c "
  source venv/bin/activate 2>/dev/null || true
  python app.py
"

sleep 1
echo "[ELVIN] 起動確認..."
curl -s -o /dev/null -w "HTTP %{http_code}\n" \
  -H "X-Daemon-Secret: $DAEMON_SECRET" \
  http://localhost:$PORT/api/v1/status
