"""
ELVIN VPS Task Queue API
日本ネオン株式会社 - ローカルエージェントへの指示置き場

起動: python app.py
環境変数:
  DAEMON_SECRET       管理操作の認証キー（デフォルト: changeme）
  PORT                リッスンポート（デフォルト: 5050）
  ANTHROPIC_API_KEY   設定するとVPS側でchat_messageを直接処理（EXE不要）
"""

import base64
import hashlib
import hmac as _hmac
import json
import os
import sqlite3
import threading
import time
import urllib.request as _urlreq
import uuid
from datetime import datetime, timezone
from contextlib import contextmanager
from functools import wraps

from flask import Flask, jsonify, request, send_file, send_from_directory

app = Flask(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "tasks.db")
DAEMON_SECRET = os.environ.get("DAEMON_SECRET", "changeme")
PORT = int(os.environ.get("PORT", 5050))
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-6")
LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")

# 概算料金レート（定期確認: https://www.anthropic.com/pricing）
# Claude Sonnet: 入力 $3 / 出力 $15 per 1Mトークン
_COST_INPUT_PER_1M = 3.0
_COST_OUTPUT_PER_1M = 15.0
_USD_TO_JPY = 155


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
                status     TEXT DEFAULT 'active',
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
                tokens_in    INTEGER DEFAULT 0,
                tokens_out   INTEGER DEFAULT 0,
                created_at   TEXT,
                completed_at TEXT,
                FOREIGN KEY (client_id) REFERENCES clients(id),
                FOREIGN KEY (agent_id) REFERENCES agents(id)
            );
            CREATE TABLE IF NOT EXISTS logs (
                id         TEXT PRIMARY KEY,
                client_id  TEXT,
                agent_id   TEXT,
                level      TEXT DEFAULT 'info',
                message    TEXT,
                created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS memories (
                id         TEXT PRIMARY KEY,
                client_id  TEXT NOT NULL,
                category   TEXT NOT NULL,
                key        TEXT NOT NULL,
                value      TEXT NOT NULL,
                updated_at TEXT,
                UNIQUE(client_id, category, key)
            );
            CREATE TABLE IF NOT EXISTS conversations (
                id         TEXT PRIMARY KEY,
                client_id  TEXT NOT NULL,
                agent_id   TEXT,
                role       TEXT NOT NULL,
                content    TEXT NOT NULL,
                created_at TEXT
            );
        """)
        # 既存DBへのカラム追加（冪等）
        for sql in [
            "ALTER TABLE clients ADD COLUMN status TEXT DEFAULT 'active'",
            "ALTER TABLE clients ADD COLUMN manager_status TEXT DEFAULT 'active'",
            "ALTER TABLE clients ADD COLUMN line_channel_access_token TEXT DEFAULT ''",
            "ALTER TABLE tasks ADD COLUMN tokens_in INTEGER DEFAULT 0",
            "ALTER TABLE tasks ADD COLUMN tokens_out INTEGER DEFAULT 0",
        ]:
            try:
                conn.execute(sql)
            except Exception:
                pass


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def line_reply(reply_token: str, text: str, access_token: str = ""):
    token = access_token or LINE_CHANNEL_ACCESS_TOKEN
    if not token or not reply_token:
        return
    body = json.dumps({
        "replyToken": reply_token,
        "messages": [{"type": "text", "text": text[:2000]}]
    }).encode()
    req = _urlreq.Request(
        "https://api.line.me/v2/bot/message/reply",
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
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
            "INSERT INTO clients (id, token, name, status, created_at) VALUES (?, ?, ?, 'active', ?)",
            (client_id, token, data.get("name", client_id), now_iso()),
        )
    return jsonify({"client_id": client_id, "token": token}), 201


@app.route("/api/v1/clients", methods=["GET"])
@require_daemon
def list_clients():
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, name, status, last_seen, created_at FROM clients ORDER BY created_at DESC"
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/v1/clients/<client_id>/status", methods=["PATCH"])
@require_daemon
def update_client_status(client_id):
    """AI（daemon）の停止・再開"""
    data = request.get_json(force=True)
    new_status = data.get("status")
    if new_status not in ("active", "suspended"):
        return jsonify({"error": "status must be active or suspended"}), 400
    with get_db() as conn:
        cur = conn.execute(
            "UPDATE clients SET status = ? WHERE id = ?", (new_status, client_id)
        )
        if cur.rowcount == 0:
            return jsonify({"error": "client not found"}), 404
    return jsonify({"ok": True, "client_id": client_id, "status": new_status})


@app.route("/api/v1/clients/<client_id>/manager_status", methods=["PATCH"])
@require_daemon
def update_manager_status(client_id):
    """ELVIN MANAGER（業務システム）の停止・再開"""
    data = request.get_json(force=True)
    new_status = data.get("status")
    if new_status not in ("active", "suspended"):
        return jsonify({"error": "status must be active or suspended"}), 400
    with get_db() as conn:
        cur = conn.execute(
            "UPDATE clients SET manager_status = ? WHERE id = ?", (new_status, client_id)
        )
        if cur.rowcount == 0:
            return jsonify({"error": "client not found"}), 404
    return jsonify({"ok": True, "client_id": client_id, "manager_status": new_status})


@app.route("/api/v1/clients/<client_id>/usage", methods=["GET"])
@require_daemon
def client_usage(client_id):
    """顧客ごとのトークン使用量と概算費用"""
    with get_db() as conn:
        row = conn.execute(
            "SELECT SUM(tokens_in) as total_in, SUM(tokens_out) as total_out,"
            " COUNT(*) as total_tasks,"
            " SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) as done_tasks"
            " FROM tasks WHERE client_id = ?",
            (client_id,)
        ).fetchone()
    total_in = row["total_in"] or 0
    total_out = row["total_out"] or 0
    cost_usd = (total_in * _COST_INPUT_PER_1M + total_out * _COST_OUTPUT_PER_1M) / 1_000_000
    cost_jpy = int(cost_usd * _USD_TO_JPY)
    return jsonify({
        "client_id": client_id,
        "tokens_in": total_in,
        "tokens_out": total_out,
        "total_tasks": row["total_tasks"] or 0,
        "done_tasks": row["done_tasks"] or 0,
        "cost_usd": round(cost_usd, 4),
        "cost_jpy": cost_jpy,
    })


@app.route("/api/v1/usage/all", methods=["GET"])
@require_daemon
def all_usage():
    """全顧客のトークン使用量・費用を一括返却"""
    with get_db() as conn:
        clients = conn.execute(
            "SELECT id, name FROM clients ORDER BY name"
        ).fetchall()
        rows = conn.execute(
            "SELECT client_id,"
            " SUM(tokens_in) as total_in, SUM(tokens_out) as total_out,"
            " COUNT(*) as total_tasks,"
            " SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) as done_tasks"
            " FROM tasks GROUP BY client_id"
        ).fetchall()
    usage_map = {r["client_id"]: dict(r) for r in rows}
    result = []
    for c in clients:
        u = usage_map.get(c["id"], {"total_in": 0, "total_out": 0, "total_tasks": 0, "done_tasks": 0})
        t_in = u["total_in"] or 0
        t_out = u["total_out"] or 0
        cost_usd = (t_in * _COST_INPUT_PER_1M + t_out * _COST_OUTPUT_PER_1M) / 1_000_000
        result.append({
            "client_id": c["id"],
            "client_name": c["name"],
            "tokens_in": t_in,
            "tokens_out": t_out,
            "total_tasks": u["total_tasks"] or 0,
            "done_tasks": u["done_tasks"] or 0,
            "cost_usd": round(cost_usd, 4),
            "cost_jpy": int(cost_usd * _USD_TO_JPY),
        })
    return jsonify(result)


# ── エージェント管理 ──────────────────────────────────────────────────────

@app.route("/api/v1/clients/<client_id>/agents", methods=["POST"])
@require_daemon
def create_agent(client_id):
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
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, name, role, line_group_id, system_prompt, enabled, last_seen, created_at"
            " FROM agents WHERE client_id = ? ORDER BY created_at ASC",
            (client_id,),
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/v1/agents/<agent_id>", methods=["GET"])
@require_daemon
def get_agent(agent_id):
    """エージェント詳細（system_prompt含む）"""
    with get_db() as conn:
        row = conn.execute(
            "SELECT id, client_id, name, role, line_group_id, system_prompt, enabled, last_seen, created_at"
            " FROM agents WHERE id = ?", (agent_id,)
        ).fetchone()
    if not row:
        return jsonify({"error": "not found"}), 404
    return jsonify(dict(row))


@app.route("/api/v1/agents/<agent_id>", methods=["PATCH"])
@require_daemon
def update_agent(agent_id):
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
            (str(uuid.uuid4()), agent_id, "ELVIN_task", now_iso()),
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

    # 停止中の顧客はタスクを返さない
    if (client["status"] or "active") == "suspended":
        return jsonify({"task": None, "suspended": True})

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
    tokens_in = int(data.get("tokens_in", 0))
    tokens_out = int(data.get("tokens_out", 0))
    task_row = None

    with get_db() as conn:
        task_row = conn.execute(
            "SELECT type, payload FROM tasks WHERE id = ? AND client_id = ?",
            (task_id, client["id"]),
        ).fetchone()
        conn.execute(
            "UPDATE tasks SET status = ?, result = ?, error = ?, completed_at = ?,"
            " tokens_in = ?, tokens_out = ?"
            " WHERE id = ? AND client_id = ?",
            (
                status,
                json.dumps(data.get("result", {})),
                data.get("error", ""),
                now_iso(),
                tokens_in,
                tokens_out,
                task_id,
                client["id"],
            ),
        )

    if success and task_row and task_row["type"] == "line_message":
        payload = json.loads(task_row["payload"])
        reply_text = (data.get("result") or {}).get("output") or (data.get("result") or {}).get("reply") or ""
        # LINE返信はdaemon側のlineworks_send.py（Playwright）が担う。VPS側からのLINE API呼び出しは行わない
        if reply_text and LINE_CHANNEL_ACCESS_TOKEN:
            line_reply(payload.get("reply_token", ""), reply_text)

    return jsonify({"ok": True})


# ── daemon用: 自分のエージェント一覧とツールを取得 ────────────────────────

@app.route("/api/v1/client/agents", methods=["GET"])
def get_my_agents():
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
        text = msg.get("text", "").strip()
        reply_token = event.get("replyToken", "")
        group_id = event.get("source", {}).get("groupId", "")

        # ELVIN管理コマンド: 「ELVIN登録:会社名」または「ELVIN登録:会社名:顧客ID」
        if text.startswith("ELVIN登録:"):
            parts = [p.strip() for p in text.split(":")]
            new_name = parts[1] if len(parts) > 1 else ""
            new_client_id = parts[2] if len(parts) > 2 and parts[2] else f"client_{uuid.uuid4().hex[:6]}"
            if new_name:
                new_token = str(uuid.uuid4()).replace("-", "")
                with get_db() as conn:
                    if conn.execute("SELECT id FROM clients WHERE id = ?", (new_client_id,)).fetchone():
                        line_reply(reply_token, f"❌ 顧客ID「{new_client_id}」は既に存在します")
                    else:
                        conn.execute(
                            "INSERT INTO clients (id, token, name, status, created_at) VALUES (?, ?, ?, 'active', ?)",
                            (new_client_id, new_token, new_name, now_iso()),
                        )
                        line_reply(reply_token,
                            f"✅ ELVIN登録完了\n"
                            f"会社名: {new_name}\n"
                            f"顧客ID: {new_client_id}\n"
                            f"トークン: {new_token}\n"
                            f"※ エージェントは管理画面から追加してください"
                        )
            else:
                line_reply(reply_token, "❌ 会社名を入力してください\n例: ELVIN登録:株式会社ABC")
            continue

        with get_db() as conn:
            agent = None
            if group_id:
                agent = conn.execute(
                    "SELECT id, client_id FROM agents WHERE line_group_id = ? AND enabled = 1",
                    (group_id,),
                ).fetchone()

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


# ── 顧客別 LINE webhook ───────────────────────────────────────────────────

@app.route("/webhook/<client_token>", methods=["POST"])
def line_webhook_client(client_token):
    """顧客ごとの LINE webhook。client_token でクライアントを識別する。"""
    client = get_client_by_token(client_token)
    if not client:
        return jsonify({"error": "invalid token"}), 401

    manager_status = client["manager_status"] if "manager_status" in client.keys() else "active"
    if (manager_status or "active") == "suspended":
        return jsonify({"ok": True, "skipped": "suspended"})

    data = request.get_json(force=True)
    for event in (data or {}).get("events", []):
        if event.get("type") != "message":
            continue
        msg = event.get("message", {})
        if msg.get("type") != "text":
            continue
        text = msg.get("text", "").strip()
        reply_token = event.get("replyToken", "")
        source = event.get("source", {})
        group_id = source.get("groupId", "")

        with get_db() as conn:
            agent = None
            if group_id:
                agent = conn.execute(
                    "SELECT id, client_id FROM agents"
                    " WHERE client_id = ? AND line_group_id = ? AND enabled = 1",
                    (client["id"], group_id),
                ).fetchone()
            if not agent:
                agent = conn.execute(
                    "SELECT id, client_id FROM agents"
                    " WHERE client_id = ? AND enabled = 1 ORDER BY created_at ASC LIMIT 1",
                    (client["id"],),
                ).fetchone()
            if agent:
                task_id = str(uuid.uuid4())
                conn.execute(
                    "INSERT INTO tasks (id, client_id, agent_id, type, payload, status, created_at)"
                    " VALUES (?, ?, ?, ?, ?, 'pending', ?)",
                    (
                        task_id, client["id"], agent["id"], "line_message",
                        json.dumps({"text": text, "reply_token": reply_token, "group_id": group_id},
                                   ensure_ascii=False),
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
            "SELECT id, name, status, manager_status, last_seen FROM clients ORDER BY last_seen DESC"
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
            "status": c["status"] or "active",
            "manager_status": c["manager_status"] or "active",
            "last_seen": c["last_seen"],
            "agents": agent_map.get(c["id"], []),
        }
        for c in clients
    ])


# ── ライセンス確認（ELVIN MANAGER → VPS） ───────────────────────────────

@app.route("/api/v1/license/check", methods=["GET"])
def license_check():
    """ELVIN MANAGERからのライセンス確認。manager_statusで制御。"""
    token = client_token_from_request()
    client = get_client_by_token(token) if token else None
    if not client:
        return jsonify({"active": False, "reason": "unknown token"}), 402
    manager_status = client["manager_status"] if "manager_status" in client.keys() else "active"
    if (manager_status or "active") == "suspended":
        return jsonify({"active": False, "reason": "suspended"}), 402
    return jsonify({"active": True, "name": client["name"]}), 200


# ── 管理画面 ─────────────────────────────────────────────────────────────

@app.route("/admin")
def admin_page():
    return send_file(os.path.join(os.path.dirname(__file__), "admin.html"))


# ── 初回セットアップ（顧客+エージェント一括作成） ─────────────────────────

@app.route("/api/v1/setup/client", methods=["POST"])
@require_daemon
def setup_client():
    data = request.get_json(force=True)
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "name is required"}), 400

    client_id = (data.get("client_id") or "").strip() or f"client_{uuid.uuid4().hex[:6]}"
    token = str(uuid.uuid4()).replace("-", "")
    # エージェントは自由指定（名前・役割・プロンプトを任意に設定）
    agents_input = data.get("agents", [])

    with get_db() as conn:
        if conn.execute("SELECT id FROM clients WHERE id = ?", (client_id,)).fetchone():
            return jsonify({"error": "client_id already exists"}), 409

        conn.execute(
            "INSERT INTO clients (id, token, name, status, created_at) VALUES (?, ?, ?, 'active', ?)",
            (client_id, token, name, now_iso()),
        )

        created = []
        for ag in agents_input:
            agent_name = (ag.get("name") or "").strip()
            if not agent_name:
                continue
            agent_id = f"{client_id}_{uuid.uuid4().hex[:6]}"
            conn.execute(
                "INSERT INTO agents (id, client_id, name, role, line_group_id, system_prompt, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    agent_id, client_id, agent_name,
                    ag.get("role", ""),
                    ag.get("line_group_id", ""),
                    ag.get("system_prompt", ""),
                    now_iso(),
                ),
            )
            conn.execute(
                "INSERT INTO agent_tools (id, agent_id, tool_name, config, enabled, created_at)"
                " VALUES (?, ?, 'ELVIN_task', '{}', 1, ?)",
                (str(uuid.uuid4()), agent_id, now_iso()),
            )
            created.append({"agent_id": agent_id, "name": agent_name, "role": ag.get("role", "")})

    return jsonify({"client_id": client_id, "token": token, "agents": created}), 201


# ── チャット用タスク投入（ELVIN CHAT → VPS） ──────────────────────────────

@app.route("/api/v1/chat/send", methods=["POST"])
def chat_send():
    """ELVIN CHATからのメッセージ受信。client_tokenで認証し、chat_messageタスクを投入する。
    X-Daemon-Secretは不要。JSから安全に呼び出せる。
    """
    token = client_token_from_request()
    client = get_client_by_token(token) if token else None
    if not client:
        return jsonify({"error": "invalid token"}), 401

    if (client["status"] or "active") == "suspended":
        return jsonify({"error": "client is suspended"}), 403

    data = request.get_json(force=True)
    message = (data.get("message") or "").strip()
    if not message:
        return jsonify({"error": "message is required"}), 400

    agent_id = data.get("agent_id")  # 省略可: 省略時はクライアントの最初のエージェント

    system_prompt = ""
    with get_db() as conn:
        # agent_idが指定されていない場合は最初の有効エージェントを選択
        if not agent_id:
            ag = conn.execute(
                "SELECT id FROM agents WHERE client_id = ? AND enabled = 1 ORDER BY created_at ASC LIMIT 1",
                (client["id"],),
            ).fetchone()
            if ag:
                agent_id = ag["id"]

        if agent_id:
            # 指定agent_idがこのclientのものかチェック
            ag_check = conn.execute(
                "SELECT id, name, system_prompt FROM agents WHERE id = ? AND client_id = ? AND enabled = 1",
                (agent_id, client["id"]),
            ).fetchone()
            if not ag_check:
                return jsonify({"error": "agent not found"}), 404
            agent_name = ag_check["name"]
            system_prompt = ag_check["system_prompt"] or ""
        else:
            agent_name = "ELVIN"

        task_id = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO tasks (id, client_id, agent_id, type, payload, status, created_at)"
            " VALUES (?, ?, ?, ?, ?, 'pending', ?)",
            (
                task_id,
                client["id"],
                agent_id,
                "chat_message",
                json.dumps({"text": message, "sender": data.get("sender", ""), "agent_name": agent_name},
                           ensure_ascii=False),
                now_iso(),
            ),
        )

    # ANTHROPIC_API_KEY があればVPS側で即時処理（バックグラウンド）
    if ANTHROPIC_API_KEY:
        t = threading.Thread(
            target=_vps_process_chat,
            args=(task_id, client["id"], agent_id, message, system_prompt),
            daemon=True,
        )
        t.start()

    return jsonify({"task_id": task_id, "agent_name": agent_name}), 201


@app.route("/api/v1/chat/tasks/<task_id>", methods=["GET"])
def chat_task_status(task_id):
    """ELVIN CHATからのタスク状態ポーリング。client_tokenで認証。"""
    token = client_token_from_request()
    client = get_client_by_token(token) if token else None
    if not client:
        return jsonify({"error": "invalid token"}), 401

    with get_db() as conn:
        row = conn.execute(
            "SELECT id, status, result, error, agent_id, created_at, completed_at"
            " FROM tasks WHERE id = ? AND client_id = ?",
            (task_id, client["id"]),
        ).fetchone()

    if not row:
        return jsonify({"error": "task not found"}), 404

    result_data = None
    if row["result"]:
        try:
            result_data = json.loads(row["result"])
        except Exception:
            result_data = {"output": row["result"]}

    return jsonify({
        "task_id": row["id"],
        "status": row["status"],
        "result": result_data,
        "error": row["error"],
        "agent_id": row["agent_id"],
        "created_at": row["created_at"],
        "completed_at": row["completed_at"],
    })


# ── メモリ・会話履歴 ヘルパー ────────────────────────────────────────────

def load_memories(client_id: str) -> str:
    """クライアントの全記憶をテキスト形式で返す（system promptに注入する）"""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT category, key, value FROM memories WHERE client_id = ? ORDER BY category, key",
            (client_id,),
        ).fetchall()
    if not rows:
        return ""
    lines = []
    current_cat = None
    for r in rows:
        if r["category"] != current_cat:
            lines.append(f"\n### {r['category']}")
            current_cat = r["category"]
        lines.append(f"- {r['key']}: {r['value']}")
    return "\n".join(lines)


def load_conversation_history(client_id: str, agent_id: str | None, limit: int = 20) -> list:
    """直近の会話履歴をAnthropicのmessages形式で返す"""
    with get_db() as conn:
        if agent_id:
            rows = conn.execute(
                "SELECT role, content FROM conversations"
                " WHERE client_id = ? AND agent_id = ?"
                " ORDER BY created_at DESC LIMIT ?",
                (client_id, agent_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT role, content FROM conversations"
                " WHERE client_id = ?"
                " ORDER BY created_at DESC LIMIT ?",
                (client_id, limit),
            ).fetchall()
    return [{"role": r["role"], "content": r["content"]} for r in reversed(rows)]


def save_conversation(client_id: str, agent_id: str | None, role: str, content: str):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO conversations (id, client_id, agent_id, role, content, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (str(uuid.uuid4()), client_id, agent_id, role, content, now_iso()),
        )


def upsert_memory(client_id: str, category: str, key: str, value: str):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO memories (id, client_id, category, key, value, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?)"
            " ON CONFLICT(client_id, category, key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            (str(uuid.uuid4()), client_id, category, key, value, now_iso()),
        )


_MEMORY_TOOL = {
    "name": "save_memory",
    "description": (
        "重要な情報を記憶に保存する。"
        "スタッフ情報・進行中案件・顧客の好み・繰り返し発生する業務パターンなど、"
        "次回以降の会話で役立つ情報を積極的に保存すること。"
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "category": {
                "type": "string",
                "description": "カテゴリ例: staff / projects / preferences / tasks / knowledge",
            },
            "key": {"type": "string", "description": "記憶のキー（短い識別子）"},
            "value": {"type": "string", "description": "保存する内容"},
        },
        "required": ["category", "key", "value"],
    },
}


def _vps_process_chat(task_id: str, client_id: str, agent_id: str | None,
                      message: str, system_prompt: str):
    """Anthropic API で chat_message タスクを VPS 側で即時処理（バックグラウンドスレッド）。
    記憶・会話履歴・メモリ保存ツールを注入してから呼び出す。
    ANTHROPIC_API_KEY が未設定の場合はローカルエージェントが拾う通常フローになる。
    """
    if not ANTHROPIC_API_KEY:
        return

    try:
        import anthropic as _ant

        ant = _ant.Anthropic(api_key=ANTHROPIC_API_KEY)

        # 記憶を読み込んでsystem promptに注入
        memories_text = load_memories(client_id)
        base_prompt = system_prompt or "あなたは ELVIN、業務アシスタントAIです。日本語で回答してください。"
        if memories_text:
            full_system = base_prompt + "\n\n## 蓄積された記憶\n" + memories_text
        else:
            full_system = base_prompt

        # 会話履歴を読み込む
        history = load_conversation_history(client_id, agent_id, limit=20)
        messages = history + [{"role": "user", "content": message}]

        resp = ant.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=4096,
            system=full_system,
            messages=messages,
            tools=[_MEMORY_TOOL],
        )

        # AIがsave_memoryツールを呼んだ場合は記憶を保存
        output_parts = []
        for block in resp.content:
            if block.type == "tool_use" and block.name == "save_memory":
                inp = block.input
                upsert_memory(client_id, inp.get("category", "general"), inp.get("key", ""), inp.get("value", ""))
            elif block.type == "text":
                output_parts.append(block.text)

        # tool_useのみで終わった場合（stop_reason="tool_use"）は続きを呼ぶ
        if resp.stop_reason == "tool_use":
            tool_result_msgs = messages + [
                {"role": "assistant", "content": resp.content},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": b.id, "content": "保存しました"}
                    for b in resp.content if b.type == "tool_use"
                ]},
            ]
            resp2 = ant.messages.create(
                model=ANTHROPIC_MODEL,
                max_tokens=2048,
                system=full_system,
                messages=tool_result_msgs,
            )
            for block in resp2.content:
                if block.type == "text":
                    output_parts.append(block.text)
            tokens_in = resp.usage.input_tokens + resp2.usage.input_tokens
            tokens_out = resp.usage.output_tokens + resp2.usage.output_tokens
        else:
            tokens_in = resp.usage.input_tokens
            tokens_out = resp.usage.output_tokens

        output = "\n".join(output_parts)

        # 会話履歴を保存
        save_conversation(client_id, agent_id, "user", message)
        save_conversation(client_id, agent_id, "assistant", output)

        with get_db() as conn:
            conn.execute(
                "UPDATE tasks SET status='completed', result=?, completed_at=?, tokens_in=?, tokens_out=?"
                " WHERE id=? AND client_id=?",
                (json.dumps({"output": output}, ensure_ascii=False),
                 now_iso(), tokens_in, tokens_out, task_id, client_id),
            )
    except Exception as e:
        print(f"[VPS_CHAT] Anthropic API エラー: {e}")
        with get_db() as conn:
            conn.execute(
                "UPDATE tasks SET status='failed', error=? WHERE id=? AND client_id=?",
                (str(e), task_id, client_id),
            )


@app.route("/api/v1/chat/agents", methods=["GET"])
def chat_list_agents():
    """ELVIN CHATからのエージェント一覧取得。client_tokenで認証。"""
    token = client_token_from_request()
    client = get_client_by_token(token) if token else None
    if not client:
        return jsonify({"error": "invalid token"}), 401

    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, name, role FROM agents WHERE client_id = ? AND enabled = 1 ORDER BY created_at ASC",
            (client["id"],),
        ).fetchall()

    return jsonify([dict(r) for r in rows])


# ── ログ ─────────────────────────────────────────────────────────────────

@app.route("/api/v1/logs", methods=["POST"])
def post_log():
    """デーモンからログを受信して保存"""
    token = client_token_from_request()
    client = get_client_by_token(token) if token else None
    if not client:
        return jsonify({"error": "invalid token"}), 401
    data = request.get_json(force=True)
    level = data.get("level", "info")
    message = (data.get("message") or "").strip()[:500]
    if not message:
        return jsonify({"ok": True})
    with get_db() as conn:
        conn.execute(
            "INSERT INTO logs (id, client_id, agent_id, level, message, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (str(uuid.uuid4()), client["id"], data.get("agent_id"), level, message, now_iso()),
        )
    return jsonify({"ok": True})


@app.route("/api/v1/logs", methods=["GET"])
@require_daemon
def get_logs():
    """ログ一覧を返す（管理画面用）"""
    client_id = request.args.get("client_id")
    limit = min(int(request.args.get("limit", 100)), 500)
    level = request.args.get("level")
    q = (
        "SELECT l.id, l.client_id, l.agent_id, l.level, l.message, l.created_at,"
        " c.name as client_name, a.name as agent_name"
        " FROM logs l"
        " LEFT JOIN clients c ON l.client_id = c.id"
        " LEFT JOIN agents a ON l.agent_id = a.id"
    )
    where, params = [], []
    if client_id:
        where.append("l.client_id = ?"); params.append(client_id)
    if level:
        where.append("l.level = ?"); params.append(level)
    if where:
        q += " WHERE " + " AND ".join(where)
    q += " ORDER BY l.created_at DESC LIMIT ?"
    params.append(limit)
    with get_db() as conn:
        rows = conn.execute(q, params).fetchall()
    return jsonify([dict(r) for r in rows])


# ── トークン統計（グラフ用） ──────────────────────────────────────────────

@app.route("/api/v1/stats/tokens", methods=["GET"])
@require_daemon
def stats_tokens():
    """時間帯別トークン使用量（グラフ用）"""
    client_id = request.args.get("client_id")  # 省略 or 'all' で全体
    days = min(int(request.args.get("days", 7)), 30)
    base_q = (
        "SELECT strftime('%Y-%m-%dT%H:00:00', created_at) as hour,"
        " SUM(tokens_in) as tokens_in, SUM(tokens_out) as tokens_out,"
        " COUNT(*) as task_count"
        " FROM tasks WHERE status = 'completed'"
        " AND created_at >= datetime('now', ?)"
    )
    params = [f"-{days} days"]
    if client_id and client_id != "all":
        base_q += " AND client_id = ?"
        params.append(client_id)
    base_q += " GROUP BY hour ORDER BY hour ASC"
    with get_db() as conn:
        rows = conn.execute(base_q, params).fetchall()
    result = []
    for r in rows:
        t_in = r["tokens_in"] or 0
        t_out = r["tokens_out"] or 0
        cost_usd = (t_in * _COST_INPUT_PER_1M + t_out * _COST_OUTPUT_PER_1M) / 1_000_000
        result.append({
            "hour": r["hour"],
            "tokens_in": t_in,
            "tokens_out": t_out,
            "task_count": r["task_count"],
            "cost_jpy": int(cost_usd * _USD_TO_JPY),
        })
    return jsonify(result)


# ── タスク履歴（管理画面用） ──────────────────────────────────────────────

@app.route("/api/v1/tasks/recent", methods=["GET"])
@require_daemon
def recent_tasks():
    limit = min(int(request.args.get("limit", 50)), 200)
    client_id = request.args.get("client_id")

    with get_db() as conn:
        if client_id:
            rows = conn.execute(
                "SELECT t.id, t.client_id, t.agent_id, a.name AS agent_name,"
                " t.type, t.status, t.error, t.result, t.tokens_in, t.tokens_out,"
                " t.created_at, t.completed_at"
                " FROM tasks t LEFT JOIN agents a ON t.agent_id = a.id"
                " WHERE t.client_id = ? ORDER BY t.created_at DESC LIMIT ?",
                (client_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT t.id, t.client_id, t.agent_id, a.name AS agent_name,"
                " t.type, t.status, t.error, t.result, t.tokens_in, t.tokens_out,"
                " t.created_at, t.completed_at"
                " FROM tasks t LEFT JOIN agents a ON t.agent_id = a.id"
                " ORDER BY t.created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()

    return jsonify([dict(r) for r in rows])


# ── メモリ管理 API（管理画面用） ─────────────────────────────────────────

@app.route("/api/v1/clients/<client_id>/memories", methods=["GET"])
@require_daemon
def list_memories(client_id):
    """クライアントの記憶一覧"""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, category, key, value, updated_at FROM memories"
            " WHERE client_id = ? ORDER BY category, key",
            (client_id,),
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/v1/clients/<client_id>/memories", methods=["POST"])
@require_daemon
def create_memory(client_id):
    """記憶を手動追加/更新"""
    data = request.get_json(force=True)
    category = (data.get("category") or "general").strip()
    key = (data.get("key") or "").strip()
    value = (data.get("value") or "").strip()
    if not key or not value:
        return jsonify({"error": "key and value required"}), 400
    upsert_memory(client_id, category, key, value)
    return jsonify({"ok": True})


@app.route("/api/v1/clients/<client_id>/memories/<category>/<key>", methods=["DELETE"])
@require_daemon
def delete_memory(client_id, category, key):
    """記憶を削除"""
    with get_db() as conn:
        conn.execute(
            "DELETE FROM memories WHERE client_id = ? AND category = ? AND key = ?",
            (client_id, category, key),
        )
    return jsonify({"ok": True})


@app.route("/api/v1/clients/<client_id>/conversations", methods=["GET"])
@require_daemon
def list_conversations(client_id):
    """会話履歴（直近N件）"""
    limit = min(int(request.args.get("limit", 50)), 200)
    agent_id = request.args.get("agent_id")
    if agent_id:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT id, agent_id, role, content, created_at FROM conversations"
                " WHERE client_id = ? AND agent_id = ?"
                " ORDER BY created_at DESC LIMIT ?",
                (client_id, agent_id, limit),
            ).fetchall()
    else:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT id, agent_id, role, content, created_at FROM conversations"
                " WHERE client_id = ?"
                " ORDER BY created_at DESC LIMIT ?",
                (client_id, limit),
            ).fetchall()
    return jsonify([dict(r) for r in reversed(rows)])


@app.route("/api/v1/clients/<client_id>/conversations", methods=["DELETE"])
@require_daemon
def clear_conversations(client_id):
    """会話履歴をリセット（エージェント指定可）"""
    agent_id = request.args.get("agent_id")
    if agent_id:
        with get_db() as conn:
            conn.execute(
                "DELETE FROM conversations WHERE client_id = ? AND agent_id = ?",
                (client_id, agent_id),
            )
    else:
        with get_db() as conn:
            conn.execute("DELETE FROM conversations WHERE client_id = ?", (client_id,))
    return jsonify({"ok": True})


# ── ELVIN CHAT 静的ファイル配信 ──────────────────────────────────────────

CHAT_DIR = os.path.join(os.path.dirname(__file__), "chat")


@app.route("/chat/")
@app.route("/chat")
def chat_index():
    return send_from_directory(CHAT_DIR, "index.html")


@app.route("/chat/<path:filename>")
def chat_static(filename):
    return send_from_directory(CHAT_DIR, filename)


# ── ELVIN ADMIN 静的ファイル配信（日本ネオン内部管理画面） ─────────────────

ADMIN_DIR = os.path.join(os.path.dirname(__file__), "admin")


@app.route("/admin/")
@app.route("/admin")
def admin_index():
    return send_from_directory(ADMIN_DIR, "index.html")


@app.route("/admin/<path:filename>")
def admin_static(filename):
    return send_from_directory(ADMIN_DIR, filename)


# ── 起動 ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    print(f"[ELVIN VPS API] http://0.0.0.0:{PORT}")
    print(f"[ELVIN VPS API] DAEMON_SECRET={DAEMON_SECRET!r}")
    app.run(host="0.0.0.0", port=PORT, debug=False)
