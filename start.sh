#!/bin/bash
# BrainTrust VPS API 起動スクリプト
# 使い方: bash start.sh

export DAEMON_SECRET="${DAEMON_SECRET:-changeme}"
export PORT="${PORT:-5050}"

echo "[BrainTrust] Starting on port $PORT"
echo "[BrainTrust] DAEMON_SECRET=$DAEMON_SECRET"

# screen セッションが既に存在する場合は停止
screen -S braintrust -X quit 2>/dev/null

# screen で起動（ターミナルを閉じても動き続ける）
screen -dmS braintrust bash -c "
  source venv/bin/activate 2>/dev/null || true
  python app.py
"

sleep 1
echo "[BrainTrust] 起動確認..."
curl -s -o /dev/null -w "HTTP %{http_code}\n" \
  -H "X-Daemon-Secret: $DAEMON_SECRET" \
  http://localhost:$PORT/api/v1/status
