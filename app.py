"""
BrainTrust VPS Task Queue API
日本ネオン株式会社 - ローカルエージェントへの指示置き場

起動: python app.py
環境変数:
  DAEMON_SECRET  daemonがタスクを投入する際の認証キー（デフォルト: changeme）
  PORT           リッスンポート（デフォルト: 5050）
"""

import base64
import hashlib
import hmac as _hmac
import json
import os
import sqlite3
import urllib.request as _urlreq
import uuid
from datetime import datetime, timezone
from functools import wraps

from flask import Flask, jsonify, request

app = Flask(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "tasks.db")
DAEMON_SECRET = os.environ.get("DAEMON_SECRET", "changeme")
PORT = int(os.environ.get("PORT", 5050))
LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")


# ── DB ────────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS clients (
                id         TEXT PRIMARY KEY,
                token      TEXT UNIQUE NOT NULL,
                name       TEXT,
                last_seen  TEXT,
                created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS tasks (
                id           TEXT PRIMARY KEY,
                client_id    TEXT NOT NULL,
                type         TEXT NOT NULL,
                payload      TEXT,
                status       TEXT DEFAULT 'pending',
                result       TEXT,
                error        TEXT,
                created_at   TEXT,
                completed_at TEXT,
                FOREIGN KEY (client_id) REFERENCES clients(id)
            );
            CREATE TABLE IF NOT EXISTS client_tools (
                id         TEXT PRIMARY KEY,
                client_id  TEXT NOT NULL,
                tool_name  TEXT NOT NULL,
                config     TEXT DEFAULT '{}',
                enabled    INTEGER DEFAULT 1,
                created_at TEXT,
                FOREIGN KEY (client_id) REFERENCES clients(id),
                UNIQUE(client_id, tool_name)
            );
        """)


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def line_reply(reply_token: str, text: str):
    if not LINE_CHANNEL_ACCESS_TOKEN or not reply_token:
        return
    body = json.dumps({
        "replyToken": reply_token,
        "messages": [{"type": "text", "text": text}]
    }).encode()
    req = _urlreq.Request(
        "https://api.line.me/v2/bot/message/reply",
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        },
        method="POST",
    )
    try:
        _urlreq.urlopen(req, timeout=10)
    except Exception as e:
        print(f"[LINE] reply error: {e}")


# ── 認証デコレータ ────────────────────────────────────────────────────────

def require_daemon(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        secret = request.headers.get("X-Daemon-Secret", "")
        if not secret and request.is_json:
            secret = request.json.get("secret", "")
        if secret != DAEMON_SECRET:
            return jsonify({"error": "unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated


def get_client_by_token(token):
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM clients WHERE token = ?", (token,)
        ).fetchone()


def client_token_from_request():
    return (
        request.headers.get("X-Client-Token")
        or request.args.get("token", "")
    )


# ── クライアント管理（daemon用） ──────────────────────────────────────────

@app.route("/api/v1/clients", methods=["POST"])
@require_daemon
def create_client():
    data = request.get_json(force=True)
    client_id = data.get("client_id") or str(uuid.uuid4())[:8]
    token = str(uuid.uuid4()).replace("-", "")
    with get_db() as conn:
        existing = conn.execute(
            "SELECT id FROM clients WHERE id = ?", (client_id,)
        ).fetchone()
        if existing:
            return jsonify({"error": "client_id already exists"}), 409
        conn.execute(
            "INSERT INTO clients (id, token, name, created_at) VALUES (?, ?, ?, ?)",
            (client_id, token, data.get("name", client_id), now_iso()),
        )
    return jsonify({"client_id": client_id, "token": token}), 201


@app.route("/api/v1/clients", methods=["GET"])
@require_daemon
def list_clients():
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, name, last_seen, created_at FROM clients ORDER BY created_at DESC"
        ).fetchall()
    return jsonify([dict(r) for r in rows])


# ── タスク投入（daemon → キュー） ─────────────────────────────────────────

@app.route("/api/v1/tasks", methods=["POST"])
@require_daemon
def push_task():
    data = request.get_json(force=True)
    client_id = data.get("client_id")
    task_type = data.get("type")
    if not client_id or not task_type:
        return jsonify({"error": "client_id and type are required"}), 400

    with get_db() as conn:
        client = conn.execute(
            "SELECT id FROM clients WHERE id = ?", (client_id,)
        ).fetchone()
        if not client:
            return jsonify({"error": "client not found"}), 404

        task_id = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO tasks (id, client_id, type, payload, status, created_at)"
            " VALUES (?, ?, ?, ?, 'pending', ?)",
            (task_id, client_id, task_type, json.dumps(data.get("payload", {})), now_iso()),
        )
    return jsonify({"task_id": task_id}), 201


# ── タスクポーリング（ローカルエージェント → キュー） ─────────────────────

@app.route("/api/v1/tasks/next", methods=["GET"])
def poll_task():
    token = client_token_from_request()
    client = get_client_by_token(token) if token else None
    if not client:
        return jsonify({"error": "invalid token"}), 401

    with get_db() as conn:
        conn.execute(
            "UPDATE clients SET last_seen = ? WHERE id = ?", (now_iso(), client["id"])
        )
        task = conn.execute(
            "SELECT * FROM tasks WHERE client_id = ? AND status = 'pending'"
            " ORDER BY created_at ASC LIMIT 1",
            (client["id"],),
        ).fetchone()
        if task:
            conn.execute(
                "UPDATE tasks SET status = 'running' WHERE id = ?", (task["id"],)
            )

    if not task:
        return jsonify({"task": None})

    return jsonify({
        "task": {
            "id": task["id"],
            "type": task["type"],
            "payload": json.loads(task["payload"]),
        }
    })


# ── タスク完了報告（ローカルエージェント → キュー） ───────────────────────

@app.route("/api/v1/tasks/<task_id>/complete", methods=["POST"])
def complete_task(task_id):
    token = client_token_from_request()
    client = get_client_by_token(token) if token else None
    if not client:
        return jsonify({"error": "invalid token"}), 401

    data = request.get_json(force=True)
    success = data.get("success", True)
    status = "completed" if success else "failed"
    task_row = None

    with get_db() as conn:
        task_row = conn.execute(
            "SELECT type, payload FROM tasks WHERE id = ? AND client_id = ?",
            (task_id, client["id"]),
        ).fetchone()
        conn.execute(
            "UPDATE tasks SET status = ?, result = ?, error = ?, completed_at = ?"
            " WHERE id = ? AND client_id = ?",
            (
                status,
                json.dumps(data.get("result", {})),
                data.get("error", ""),
                now_iso(),
                task_id,
                client["id"],
            ),
        )

    if success and task_row and task_row["type"] == "line_message":
        payload = json.loads(task_row["payload"])
        reply_text = data.get("result", {}).get("reply", "完了しました")
        line_reply(payload.get("reply_token", ""), reply_text)

    return jsonify({"ok": True})


# ── ツール管理 ───────────────────────────────────────────────────────────

@app.route("/api/v1/client/tools", methods=["GET"])
def get_my_tools():
    """agent_local.py が起動時に呼ぶ: 自分のクライアントの有効ツール一覧を返す"""
    token = client_token_from_request()
    client = get_client_by_token(token) if token else None
    if not client:
        return jsonify({"error": "invalid token"}), 401
    with get_db() as conn:
        rows = conn.execute(
            "SELECT tool_name, config FROM client_tools"
            " WHERE client_id = ? AND enabled = 1 ORDER BY tool_name",
            (client["id"],),
        ).fetchall()
    return jsonify([{"tool": r["tool_name"], "config": json.loads(r["config"])} for r in rows])


@app.route("/api/v1/clients/<client_id>/tools", methods=["GET"])
@require_daemon
def list_client_tools(client_id):
    """管理者: クライアントのツール一覧"""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT tool_name, config, enabled, created_at FROM client_tools"
            " WHERE client_id = ? ORDER BY tool_name",
            (client_id,),
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/v1/clients/<client_id>/tools", methods=["POST"])
@require_daemon
def add_client_tool(client_id):
    """管理者: ツールを追加"""
    data = request.get_json(force=True)
    tool_name = data.get("tool_name")
    if not tool_name:
        return jsonify({"error": "tool_name is required"}), 400
    config = json.dumps(data.get("config", {}))
    tool_id = str(uuid.uuid4())[:8]
    with get_db() as conn:
        try:
            conn.execute(
                "INSERT INTO client_tools (id, client_id, tool_name, config, enabled, created_at)"
                " VALUES (?, ?, ?, ?, 1, ?)",
                (tool_id, client_id, tool_name, config, now_iso()),
            )
        except sqlite3.IntegrityError:
            conn.execute(
                "UPDATE client_tools SET config = ?, enabled = 1"
                " WHERE client_id = ? AND tool_name = ?",
                (config, client_id, tool_name),
            )
    return jsonify({"ok": True, "tool_name": tool_name}), 201


@app.route("/api/v1/clients/<client_id>/tools/<tool_name>", methods=["DELETE"])
@require_daemon
def remove_client_tool(client_id, tool_name):
    """管理者: ツールを無効化"""
    with get_db() as conn:
        conn.execute(
            "UPDATE client_tools SET enabled = 0"
            " WHERE client_id = ? AND tool_name = ?",
            (client_id, tool_name),
        )
    return jsonify({"ok": True})


# ── ハートビート ─────────────────────────────────────────────────────────

@app.route("/api/v1/heartbeat", methods=["POST"])
def heartbeat():
    token = client_token_from_request()
    client = get_client_by_token(token) if token else None
    if not client:
        return jsonify({"error": "invalid token"}), 401
    with get_db() as conn:
        conn.execute(
            "UPDATE clients SET last_seen = ? WHERE id = ?", (now_iso(), client["id"])
        )
    return jsonify({"ok": True, "client_id": client["id"]})


# ── LINE Webhook ─────────────────────────────────────────────────────────

@app.route("/webhook", methods=["POST"])
def line_webhook():
    body = request.get_data()
    sig = request.headers.get("X-Line-Signature", "")

    if LINE_CHANNEL_SECRET:
        digest = _hmac.new(LINE_CHANNEL_SECRET.encode(), body, hashlib.sha256).digest()
        if base64.b64encode(digest).decode() != sig:
            return jsonify({"error": "invalid signature"}), 400

    data = request.get_json(force=True)
    for event in data.get("events", []):
        if event.get("type") != "message":
            continue
        msg = event.get("message", {})
        if msg.get("type") != "text":
            continue
        text = msg.get("text", "")
        reply_token = event.get("replyToken", "")
        with get_db() as conn:
            client = conn.execute(
                "SELECT id FROM clients ORDER BY created_at ASC LIMIT 1"
            ).fetchone()
            if client:
                task_id = str(uuid.uuid4())
                conn.execute(
                    "INSERT INTO tasks (id, client_id, type, payload, status, created_at)"
                    " VALUES (?, ?, ?, ?, 'pending', ?)",
                    (task_id, client["id"], "line_message",
                     json.dumps({"text": text, "reply_token": reply_token}),
                     now_iso()),
                )
    return jsonify({"ok": True})


# ── ステータス概要（管理者用） ────────────────────────────────────────────

@app.route("/api/v1/status", methods=["GET"])
@require_daemon
def status():
    with get_db() as conn:
        clients = conn.execute(
            "SELECT id, name, last_seen FROM clients ORDER BY last_seen DESC"
        ).fetchall()
        stats = conn.execute(
            "SELECT client_id, status, COUNT(*) as cnt FROM tasks"
            " GROUP BY client_id, status"
        ).fetchall()

    stat_map = {}
    for s in stats:
        stat_map.setdefault(s["client_id"], {})[s["status"]] = s["cnt"]

    return jsonify([
        {
            "id": c["id"],
            "name": c["name"],
            "last_seen": c["last_seen"],
            "tasks": stat_map.get(c["id"], {}),
        }
        for c in clients
    ])


# ── 起動 ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    print(f"[BrainTrust VPS API] http://0.0.0.0:{PORT}")
    print(f"[BrainTrust VPS API] DAEMON_SECRET={DAEMON_SECRET!r}")
    app.run(host="0.0.0.0", port=PORT, debug=False)
