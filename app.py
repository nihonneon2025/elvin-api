"""
ELVIN VPS Task Queue API
日本ネオン株式会社 - ローカルエージェントへの指示置き場

起動: python app.py
環境変数:
  DAEMON_SECRET  管理操作の認証キー（デフォルト: changeme）
  PORT           リッスンポート（デフォルト: 5050）
"""

import base64
import hashlib
import hmac as _hmac
import json
import os
import sqlite3
import time
import urllib.request as _urlreq
import uuid
from datetime import datetime, timezone
from contextlib import contextmanager
from functools import wraps

from flask import Flask, jsonify, request

app = Flask(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "tasks.db")
DAEMON_SECRET = os.environ.get("DAEMON_SECRET", "changeme")
PORT = int(os.environ.get("PORT", 5050))
LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")


# ── DB ────────────────────────────────────────────────────────────────────

@contextmanager
def get_db():
    conn = None
    for attempt in range(3):
        try:
            conn = sqlite3.connect(DB_PATH, timeout=20)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            break
        except sqlite3.OperationalError:
            if attempt == 2:
                raise
            time.sleep(0.5)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


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
            CREATE TABLE IF NOT EXISTS agents (
                id            TEXT PRIMARY KEY,
                client_id     TEXT NOT NULL,
                name          TEXT NOT NULL,
                role          TEXT,
                line_group_id TEXT,
                system_prompt TEXT,
                enabled       INTEGER DEFAULT 1,
                last_seen     TEXT,
                created_at    TEXT,
                FOREIGN KEY (client_id) REFERENCES clients(id)
            );
            CREATE TABLE IF NOT EXISTS agent_tools (
                id         TEXT PRIMARY KEY,
                agent_id   TEXT NOT NULL,
                tool_name  TEXT NOT NULL,
                config     TEXT DEFAULT '{}',
                enabled    INTEGER DEFAULT 1,
                created_at TEXT,
                FOREIGN KEY (agent_id) REFERENCES agents(id),
                UNIQUE(agent_id, tool_name)
            );
            CREATE TABLE IF NOT EXISTS tasks (
                id           TEXT PRIMARY KEY,
                client_id    TEXT NOT NULL,
                agent_id     TEXT,
                type         TEXT NOT NULL,
                payload      TEXT,
                status       TEXT DEFAULT 'pending',
                result       TEXT,
                error        TEXT,
                created_at   TEXT,
                completed_at TEXT,
                FOREIGN KEY (client_id) REFERENCES clients(id),
                FOREIGN KEY (agent_id) REFERENCES agents(id)
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


# ── クライアント管理 ──────────────────────────────────────────────────────

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


# ── エージェント管理 ──────────────────────────────────────────────────────

@app.route("/api/v1/clients/<client_id>/agents", methods=["POST"])
@require_daemon
def create_agent(client_id):
    """部署・AIエージェントを作成"""
    data = request.get_json(force=True)
    name = data.get("name")
    if not name:
        return jsonify({"error": "name is required"}), 400

    agent_id = data.get("agent_id") or f"{client_id}_{str(uuid.uuid4())[:6]}"
    with get_db() as conn:
        client = conn.execute(
            "SELECT id FROM clients WHERE id = ?", (client_id,)
        ).fetchone()
        if not client:
            return jsonify({"error": "client not found"}), 404
        existing = conn.execute(
            "SELECT id FROM agents WHERE id = ?", (agent_id,)
        ).fetchone()
        if existing:
            return jsonify({"error": "agent_id already exists"}), 409
        conn.execute(
            "INSERT INTO agents (id, client_id, name, role, line_group_id, system_prompt, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                agent_id,
                client_id,
                name,
                data.get("role", ""),
                data.get("line_group_id", ""),
                data.get("system_prompt", ""),
                now_iso(),
            ),
        )
    return jsonify({"agent_id": agent_id, "name": name}), 201


@app.route("/api/v1/clients/<client_id>/agents", methods=["GET"])
@require_daemon
def list_agents(client_id):
    """クライアントの全エージェント一覧"""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, name, role, line_group_id, enabled, last_seen, created_at"
            " FROM agents WHERE client_id = ? ORDER BY created_at ASC",
            (client_id,),
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/v1/agents/<agent_id>", methods=["PATCH"])
@require_daemon
def update_agent(agent_id):
    """エージェント情報を更新（name/role/line_group_id/system_prompt/enabled）"""
    data = request.get_json(force=True)
    fields = {k: v for k, v in data.items()
              if k in ("name", "role", "line_group_id", "system_prompt", "enabled")}
    if not fields:
        return jsonify({"error": "no updatable fields"}), 400
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    with get_db() as conn:
        conn.execute(
            f"UPDATE agents SET {set_clause} WHERE id = ?",
            (*fields.values(), agent_id),
        )
    return jsonify({"ok": True})


@app.route("/api/v1/agents/<agent_id>", methods=["DELETE"])
@require_daemon
def delete_agent(agent_id):
    with get_db() as conn:
        conn.execute("UPDATE agents SET enabled = 0 WHERE id = ?", (agent_id,))
    return jsonify({"ok": True})


# ── エージェントツール管理 ────────────────────────────────────────────────

@app.route("/api/v1/agents/<agent_id>/tools", methods=["GET"])
@require_daemon
def list_agent_tools(agent_id):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT tool_name, config, enabled, created_at FROM agent_tools"
            " WHERE agent_id = ? ORDER BY tool_name",
            (agent_id,),
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/v1/agents/<agent_id>/tools", methods=["POST"])
@require_daemon
def add_agent_tool(agent_id):
    data = request.get_json(force=True)
    tool_name = data.get("tool_name")
    if not tool_name:
        return jsonify({"error": "tool_name is required"}), 400
    config = json.dumps(data.get("config", {}))
    tool_id = str(uuid.uuid4())[:8]
    with get_db() as conn:
        try:
            conn.execute(
                "INSERT INTO agent_tools (id, agent_id, tool_name, config, enabled, created_at)"
                " VALUES (?, ?, ?, ?, 1, ?)",
                (tool_id, agent_id, tool_name, config, now_iso()),
            )
        except sqlite3.IntegrityError:
            conn.execute(
                "UPDATE agent_tools SET config = ?, enabled = 1"
                " WHERE agent_id = ? AND tool_name = ?",
                (config, agent_id, tool_name),
            )
    return jsonify({"ok": True, "tool_name": tool_name}), 201


@app.route("/api/v1/agents/<agent_id>/tools/<tool_name>", methods=["DELETE"])
@require_daemon
def remove_agent_tool(agent_id, tool_name):
    with get_db() as conn:
        conn.execute(
            "UPDATE agent_tools SET enabled = 0 WHERE agent_id = ? AND tool_name = ?",
            (agent_id, tool_name),
        )
    return jsonify({"ok": True})


# ── タスク投入（管理者 → キュー） ─────────────────────────────────────────

@app.route("/api/v1/tasks", methods=["POST"])
@require_daemon
def push_task():
    data = request.get_json(force=True)
    client_id = data.get("client_id")
    agent_id = data.get("agent_id")
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
            "INSERT INTO tasks (id, client_id, agent_id, type, payload, status, created_at)"
            " VALUES (?, ?, ?, ?, ?, 'pending', ?)",
            (task_id, client_id, agent_id, task_type,
             json.dumps(data.get("payload", {})), now_iso()),
        )
    return jsonify({"task_id": task_id}), 201


@app.route("/api/v1/tasks/delegate", methods=["POST"])
def delegate_task():
    """ローカルエージェントが自クライアント内の別エージェントにタスクを委託する"""
    token = client_token_from_request()
    client = get_client_by_token(token) if token else None
    if not client:
        return jsonify({"error": "invalid token"}), 401

    data = request.get_json(force=True)
    agent_id = data.get("agent_id")
    task_type = data.get("type")
    if not agent_id or not task_type:
        return jsonify({"error": "agent_id and type are required"}), 400

    with get_db() as conn:
        agent = conn.execute(
            "SELECT id FROM agents WHERE id = ? AND client_id = ? AND enabled = 1",
            (agent_id, client["id"]),
        ).fetchone()
        if not agent:
            return jsonify({"error": "agent not found"}), 404

        task_id = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO tasks (id, client_id, agent_id, type, payload, status, created_at)"
            " VALUES (?, ?, ?, ?, ?, 'pending', ?)",
            (task_id, client["id"], agent_id, task_type,
             json.dumps(data.get("payload", {})), now_iso()),
        )
    return jsonify({"task_id": task_id}), 201


# ── クライアント側エージェント管理 ────────────────────────────────────────

@app.route("/api/v1/manage/agents", methods=["GET"])
def manage_list_agents():
    token = client_token_from_request()
    client = get_client_by_token(token) if token else None
    if not client:
        return jsonify({"error": "invalid token"}), 401
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, name, role, system_prompt, enabled FROM agents"
            " WHERE client_id = ? ORDER BY created_at",
            (client["id"],),
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/v1/manage/agents", methods=["POST"])
def manage_add_agent():
    token = client_token_from_request()
    client = get_client_by_token(token) if token else None
    if not client:
        return jsonify({"error": "invalid token"}), 401
    data = request.get_json(force=True)
    name = (data.get("name") or "").strip()
    role = (data.get("role") or "").strip()
    system_prompt = (data.get("system_prompt") or "").strip()
    if not name or not system_prompt:
        return jsonify({"error": "name and system_prompt are required"}), 400
    agent_id = f"{client['id']}_{uuid.uuid4().hex[:8]}"
    with get_db() as conn:
        conn.execute(
            "INSERT INTO agents (id, client_id, name, role, system_prompt, enabled, created_at)"
            " VALUES (?, ?, ?, ?, ?, 1, ?)",
            (agent_id, client["id"], name, role, system_prompt, now_iso()),
        )
        conn.execute(
            "INSERT OR IGNORE INTO agent_tools (id, agent_id, tool_name, config, enabled, created_at)"
            " VALUES (?, ?, ?, '{}', 1, ?)",
            (str(uuid.uuid4()), agent_id, "claude_task", now_iso()),
        )
    return jsonify({"agent_id": agent_id, "name": name}), 201


@app.route("/api/v1/manage/agents/<agent_id>", methods=["PATCH"])
def manage_update_agent(agent_id):
    token = client_token_from_request()
    client = get_client_by_token(token) if token else None
    if not client:
        return jsonify({"error": "invalid token"}), 401
    data = request.get_json(force=True)
    updates = {k: data[k] for k in ("name", "role", "system_prompt") if k in data}
    if not updates:
        return jsonify({"error": "nothing to update"}), 400
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values()) + [agent_id, client["id"]]
    with get_db() as conn:
        cur = conn.execute(
            f"UPDATE agents SET {set_clause} WHERE id = ? AND client_id = ?",
            values,
        )
        if cur.rowcount == 0:
            return jsonify({"error": "agent not found"}), 404
    return jsonify({"updated": True, "agent_id": agent_id})


@app.route("/api/v1/manage/agents/<agent_id>", methods=["DELETE"])
def manage_delete_agent(agent_id):
    token = client_token_from_request()
    client = get_client_by_token(token) if token else None
    if not client:
        return jsonify({"error": "invalid token"}), 401
    with get_db() as conn:
        cur = conn.execute(
            "UPDATE agents SET enabled = 0 WHERE id = ? AND client_id = ?",
            (agent_id, client["id"]),
        )
        if cur.rowcount == 0:
            return jsonify({"error": "agent not found"}), 404
    return jsonify({"deleted": True, "agent_id": agent_id})


# ── タスクポーリング（ローカルエージェント → キュー） ─────────────────────

@app.route("/api/v1/tasks/next", methods=["GET"])
def poll_task():
    token = client_token_from_request()
    client = get_client_by_token(token) if token else None
    if not client:
        return jsonify({"error": "invalid token"}), 401

    agent_id = request.args.get("agent_id")

    with get_db() as conn:
        conn.execute(
            "UPDATE clients SET last_seen = ? WHERE id = ?", (now_iso(), client["id"])
        )
        if agent_id:
            conn.execute(
                "UPDATE agents SET last_seen = ? WHERE id = ? AND client_id = ?",
                (now_iso(), agent_id, client["id"]),
            )
            task = conn.execute(
                "SELECT * FROM tasks WHERE client_id = ? AND (agent_id = ? OR agent_id IS NULL) AND status = 'pending'"
                " ORDER BY created_at ASC LIMIT 1",
                (client["id"], agent_id),
            ).fetchone()
        else:
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
            "agent_id": task["agent_id"],
            "payload": json.loads(task["payload"]),
        }
    })


# ── タスク完了報告 ────────────────────────────────────────────────────────

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


# ── daemon用: 自分のエージェント一覧とツールを取得 ────────────────────────

@app.route("/api/v1/client/agents", methods=["GET"])
def get_my_agents():
    """agent_local.py が起動時に呼ぶ: 自分のクライアントの全エージェント+ツールを返す"""
    token = client_token_from_request()
    client = get_client_by_token(token) if token else None
    if not client:
        return jsonify({"error": "invalid token"}), 401

    with get_db() as conn:
        agents = conn.execute(
            "SELECT id, name, role, line_group_id, system_prompt FROM agents"
            " WHERE client_id = ? AND enabled = 1 ORDER BY created_at ASC",
            (client["id"],),
        ).fetchall()

        result = []
        for ag in agents:
            tools = conn.execute(
                "SELECT tool_name, config FROM agent_tools"
                " WHERE agent_id = ? AND enabled = 1 ORDER BY tool_name",
                (ag["id"],),
            ).fetchall()
            result.append({
                "agent_id": ag["id"],
                "name": ag["name"],
                "role": ag["role"],
                "line_group_id": ag["line_group_id"],
                "system_prompt": ag["system_prompt"],
                "tools": [{"tool": t["tool_name"], "config": json.loads(t["config"])} for t in tools],
            })

    return jsonify(result)


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
        group_id = event.get("source", {}).get("groupId", "")

        with get_db() as conn:
            # LINEグループIDでエージェントを特定
            agent = None
            if group_id:
                agent = conn.execute(
                    "SELECT id, client_id FROM agents WHERE line_group_id = ? AND enabled = 1",
                    (group_id,),
                ).fetchone()

            # エージェントが見つからなければデフォルト（最初のクライアントの最初のエージェント）
            if not agent:
                agent = conn.execute(
                    "SELECT id, client_id FROM agents WHERE enabled = 1"
                    " ORDER BY created_at ASC LIMIT 1"
                ).fetchone()

            if agent:
                task_id = str(uuid.uuid4())
                conn.execute(
                    "INSERT INTO tasks (id, client_id, agent_id, type, payload, status, created_at)"
                    " VALUES (?, ?, ?, ?, ?, 'pending', ?)",
                    (
                        task_id,
                        agent["client_id"],
                        agent["id"],
                        "line_message",
                        json.dumps({"text": text, "reply_token": reply_token, "group_id": group_id}),
                        now_iso(),
                    ),
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
        agents = conn.execute(
            "SELECT id, client_id, name, role, enabled, last_seen FROM agents ORDER BY client_id, created_at"
        ).fetchall()
        stats = conn.execute(
            "SELECT agent_id, status, COUNT(*) as cnt FROM tasks"
            " GROUP BY agent_id, status"
        ).fetchall()

    stat_map = {}
    for s in stats:
        stat_map.setdefault(s["agent_id"], {})[s["status"]] = s["cnt"]

    agent_map = {}
    for ag in agents:
        agent_map.setdefault(ag["client_id"], []).append({
            "id": ag["id"],
            "name": ag["name"],
            "role": ag["role"],
            "enabled": ag["enabled"],
            "last_seen": ag["last_seen"],
            "tasks": stat_map.get(ag["id"], {}),
        })

    return jsonify([
        {
            "id": c["id"],
            "name": c["name"],
            "last_seen": c["last_seen"],
            "agents": agent_map.get(c["id"], []),
        }
        for c in clients
    ])


# ── 起動 ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    print(f"[ELVIN VPS API] http://0.0.0.0:{PORT}")
    print(f"[ELVIN VPS API] DAEMON_SECRET={DAEMON_SECRET!r}")
    app.run(host="0.0.0.0", port=PORT, debug=False)
