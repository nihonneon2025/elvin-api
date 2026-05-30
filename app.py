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
import re
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

@app.after_request
def add_cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Daemon-Secret, X-Client-Token"
    return response

@app.route("/api/v1/<path:p>", methods=["OPTIONS"])
@app.route("/api/<path:p>", methods=["OPTIONS"])
def handle_preflight(p=""):
    return "", 204

DB_PATH = os.path.join(os.path.dirname(__file__), "tasks.db")
DAEMON_SECRET = os.environ.get("DAEMON_SECRET", "changeme")
PORT = int(os.environ.get("PORT", 5050))
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-6")
GOOGLE_SEARCH_API_KEY = os.environ.get("GOOGLE_SEARCH_API_KEY", "")
GOOGLE_SEARCH_CX = os.environ.get("GOOGLE_SEARCH_CX", "")
LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")

# モデル別料金レート（公式: https://docs.anthropic.com/ja/docs/about-claude/models）
# (入力$/1M, 出力$/1M)
_MODEL_PRICING = {
    "claude-haiku-4-5-20251001": (1.0, 5.0),
    "claude-haiku-4-5":          (1.0, 5.0),
    "claude-sonnet-4-6":         (3.0, 15.0),
    "claude-opus-4-7":           (5.0, 25.0),
    "claude-opus-4-6":           (5.0, 25.0),
}
_DEFAULT_COST_INPUT_PER_1M  = 1.0   # 不明モデルはHaiku相当で概算
_DEFAULT_COST_OUTPUT_PER_1M = 5.0
_USD_TO_JPY = 155


def _pricing(model: str) -> tuple:
    return _MODEL_PRICING.get(model or "", (_DEFAULT_COST_INPUT_PER_1M, _DEFAULT_COST_OUTPUT_PER_1M))


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
                id           TEXT PRIMARY KEY,
                client_id    TEXT NOT NULL,
                agent_id     TEXT,
                role         TEXT NOT NULL,
                content      TEXT NOT NULL,
                requester_id TEXT,
                created_at   TEXT
            );
            CREATE TABLE IF NOT EXISTS staff (
                id            TEXT PRIMARY KEY,
                client_id     TEXT NOT NULL,
                line_user_id  TEXT NOT NULL,
                name          TEXT NOT NULL,
                created_at    TEXT,
                UNIQUE(client_id, line_user_id),
                FOREIGN KEY (client_id) REFERENCES clients(id)
            );
            CREATE TABLE IF NOT EXISTS order_master (
                id             TEXT PRIMARY KEY,
                client_id      TEXT NOT NULL,
                name           TEXT NOT NULL,
                category       TEXT DEFAULT '',
                lead_time_days INTEGER NOT NULL DEFAULT 14,
                unit           TEXT DEFAULT '',
                memo           TEXT DEFAULT '',
                created_at     TEXT,
                FOREIGN KEY (client_id) REFERENCES clients(id)
            );
            CREATE TABLE IF NOT EXISTS construction_alerts (
                id                TEXT PRIMARY KEY,
                client_id         TEXT NOT NULL,
                project_name      TEXT NOT NULL,
                construction_date TEXT NOT NULL,
                order_item_id     TEXT DEFAULT '',
                order_item_name   TEXT DEFAULT '',
                lead_time_days    INTEGER DEFAULT 14,
                lineworks_room    TEXT DEFAULT '',
                status            TEXT DEFAULT 'pending',
                notified_at       TEXT,
                created_at        TEXT,
                FOREIGN KEY (client_id) REFERENCES clients(id)
            );
            CREATE TABLE IF NOT EXISTS deadline_alerts (
                id                TEXT PRIMARY KEY,
                client_id         TEXT NOT NULL,
                project_name      TEXT NOT NULL,
                category          TEXT NOT NULL,
                deadline          TEXT NOT NULL,
                alert_days_before INTEGER DEFAULT 7,
                lineworks_room    TEXT DEFAULT '',
                status            TEXT DEFAULT 'pending',
                notified_at       TEXT,
                memo              TEXT DEFAULT '',
                created_at        TEXT,
                FOREIGN KEY (client_id) REFERENCES clients(id)
            );
            CREATE TABLE IF NOT EXISTS project_routines (
                id                TEXT PRIMARY KEY,
                client_id         TEXT NOT NULL,
                project_name      TEXT NOT NULL,
                construction_date TEXT NOT NULL,
                lineworks_room    TEXT DEFAULT '',
                created_at        TEXT,
                FOREIGN KEY (client_id) REFERENCES clients(id)
            );
            CREATE TABLE IF NOT EXISTS checklist_alerts (
                id                TEXT PRIMARY KEY,
                client_id         TEXT NOT NULL,
                construction_name TEXT NOT NULL,
                construction_date TEXT NOT NULL,
                lineworks_room    TEXT DEFAULT '',
                check_materials   INTEGER DEFAULT 0,
                check_contractor  INTEGER DEFAULT 0,
                check_instruction INTEGER DEFAULT 0,
                status            TEXT DEFAULT 'pending',
                notified_at       TEXT,
                created_at        TEXT,
                FOREIGN KEY (client_id) REFERENCES clients(id)
            );
        """)
        # 既存DBへのカラム追加（冪等）
        for sql in [
            "ALTER TABLE clients ADD COLUMN status TEXT DEFAULT 'active'",
            "ALTER TABLE clients ADD COLUMN manager_status TEXT DEFAULT 'active'",
            "ALTER TABLE clients ADD COLUMN anthropic_api_key TEXT DEFAULT ''",
            "ALTER TABLE clients ADD COLUMN anthropic_model TEXT DEFAULT ''",
            "ALTER TABLE tasks ADD COLUMN tokens_in INTEGER DEFAULT 0",
            "ALTER TABLE tasks ADD COLUMN tokens_out INTEGER DEFAULT 0",
            "ALTER TABLE tasks ADD COLUMN model TEXT DEFAULT ''",
            "ALTER TABLE conversations ADD COLUMN requester_id TEXT",
            "ALTER TABLE order_master ADD COLUMN unit_price INTEGER DEFAULT 0",
            "ALTER TABLE construction_alerts ADD COLUMN order_amount INTEGER DEFAULT 0",
            "ALTER TABLE construction_alerts ADD COLUMN delivered_at TEXT",
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


def _web_push(push_url: str, push_token: str, title: str, body: str, path: str = "/"):
    """Web Push通知をsubscribe.phpへ送信（daemon不在時のバックアップ通知）"""
    if not push_url or not push_token:
        return
    payload = json.dumps({
        "action": "send_all",
        "title": title,
        "body": body[:200],
        "url": path,
        "badge_count": 1,
    }).encode()
    req = _urlreq.Request(
        push_url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "X-AGO-Token": push_token,
        },
        method="POST",
    )
    try:
        _urlreq.urlopen(req, timeout=15)
    except Exception as e:
        print(f"[WEB_PUSH] error: {e}")


def _vps_elvin_task_fallback(task_id: str, payload: dict):
    """ELVIN_task投入後30秒経ってもdaemonが拾わない場合、VPS側でAI処理してWeb Pushで通知する。
    ファイル操作・Playwright等のデスクトップ操作は不可。テキスト回答のみ提供。
    """
    time.sleep(30)
    with get_db() as conn:
        row = conn.execute(
            "SELECT status, agent_id, client_id FROM tasks WHERE id = ?", (task_id,)
        ).fetchone()
    if not row or row["status"] != "pending":
        return  # daemon処理済み or 処理中

    client_id = row["client_id"] or ""
    agent_id = row["agent_id"] or ""
    prompt = payload.get("prompt", "")
    requester_name = payload.get("requester_name", "スタッフ")
    push_url = payload.get("web_push_url", "")
    push_token = payload.get("web_push_token", "")

    # エージェントのsystem_promptをDBから取得
    system_prompt = ""
    if agent_id:
        with get_db() as conn:
            ag = conn.execute(
                "SELECT system_prompt FROM agents WHERE id = ?", (agent_id,)
            ).fetchone()
        if ag:
            system_prompt = ag["system_prompt"] or ""

    FALLBACK_HEADER = "🔴 DAEMONが起動していません。VPSバックアップで対応中です。"

    # VPS側でAnthropicを呼んでテキスト回答を生成
    output = ""
    tokens_in = tokens_out = 0
    try:
        if ANTHROPIC_API_KEY and prompt:
            import anthropic as _ant
            with get_db() as conn:
                client_row = conn.execute(
                    "SELECT anthropic_api_key, anthropic_model FROM clients WHERE id = ?",
                    (client_id,)
                ).fetchone()
            api_key = (client_row["anthropic_api_key"] if client_row else "") or ANTHROPIC_API_KEY
            model = (client_row["anthropic_model"] if client_row else "") or ANTHROPIC_MODEL

            ant = _ant.Anthropic(api_key=api_key)
            vps_note = (
                f"【VPSバックアップモード】{FALLBACK_HEADER}\n"
                "ファイル操作・PowerShell実行・LINE WORKSへの送付はできません。\n"
                "回答の最初の1行に必ず「🔴 DAEMONが起動していません。VPSバックアップで対応中です。」と書いてから回答してください。"
            )
            full_system = f"{system_prompt}\n\n{vps_note}" if system_prompt else vps_note
            resp = ant.messages.create(
                model=model, max_tokens=512,
                system=full_system,
                messages=[{"role": "user", "content": prompt}],
            )
            output = "".join(b.text for b in resp.content if b.type == "text").strip()
            # AIがヘッダーを書き忘れた場合は強制付与
            if output and FALLBACK_HEADER not in output:
                output = f"{FALLBACK_HEADER}\n{output}"
            tokens_in = resp.usage.input_tokens
            tokens_out = resp.usage.output_tokens

        with get_db() as conn:
            conn.execute(
                "UPDATE tasks SET status='completed', result=?, completed_at=?,"
                " tokens_in=?, tokens_out=? WHERE id=? AND status='pending'",
                (json.dumps({"output": output, "source": "vps_fallback"}, ensure_ascii=False),
                 now_iso(), tokens_in, tokens_out, task_id),
            )
    except Exception as e:
        print(f"[VPS_FALLBACK] AI処理エラー: {e}")
        with get_db() as conn:
            conn.execute(
                "UPDATE tasks SET status='failed', error=?, completed_at=?"
                " WHERE id=? AND status='pending'",
                (f"vps_fallback_error: {str(e)[:200]}", now_iso(), task_id),
            )

    # Web Push でブラウザ通知（常にFALLBACK_HEADERを先頭に付与）
    if push_url and push_token:
        if output:
            push_body = f"{FALLBACK_HEADER}\n{requester_name}さんへ:\n{output.replace(FALLBACK_HEADER, '').strip()[:120]}"
        else:
            push_body = f"{FALLBACK_HEADER}\nしばらくしてから再度お試しください。"
        _web_push(push_url, push_token, "AGO SYSTEM MANAGER", push_body)

    print(f"[VPS_FALLBACK] daemon timeout 30s: task={task_id[:8]} output_len={len(output)}")


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


@app.route("/api/v1/clients/<client_id>/settings", methods=["PATCH"])
@require_daemon
def update_client_settings(client_id):
    """顧客のAnthropicAPIキー・モデルを更新"""
    data = request.get_json(force=True)
    allowed = {"anthropic_api_key", "anthropic_model"}
    updates = {k: v for k, v in data.items() if k in allowed}
    if not updates:
        return jsonify({"error": "no valid fields"}), 400
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    with get_db() as conn:
        cur = conn.execute(
            f"UPDATE clients SET {set_clause} WHERE id = ?",
            list(updates.values()) + [client_id],
        )
        if cur.rowcount == 0:
            return jsonify({"error": "client not found"}), 404
    return jsonify({"ok": True, "client_id": client_id, **updates})


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
    with get_db() as conn:
        model_rows = conn.execute(
            "SELECT model, SUM(tokens_in) as ti, SUM(tokens_out) as to_"
            " FROM tasks WHERE client_id = ? AND status='completed' GROUP BY model",
            (client_id,)
        ).fetchall()
    cost_usd = sum(
        (r["ti"] or 0) * _pricing(r["model"])[0] / 1_000_000 +
        (r["to_"] or 0) * _pricing(r["model"])[1] / 1_000_000
        for r in model_rows
    )
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
    with get_db() as conn:
        model_rows = conn.execute(
            "SELECT client_id, model, SUM(tokens_in) as ti, SUM(tokens_out) as to_"
            " FROM tasks WHERE status='completed' GROUP BY client_id, model"
        ).fetchall()
    cost_map = {}
    for r in model_rows:
        c_in, c_out = _pricing(r["model"])
        cost_map[r["client_id"]] = cost_map.get(r["client_id"], 0) + (
            (r["ti"] or 0) * c_in + (r["to_"] or 0) * c_out
        ) / 1_000_000
    result = []
    for c in clients:
        u = usage_map.get(c["id"], {"total_in": 0, "total_out": 0, "total_tasks": 0, "done_tasks": 0})
        t_in = u["total_in"] or 0
        t_out = u["total_out"] or 0
        cost_usd = cost_map.get(c["id"], 0)
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


@app.route("/api/v1/agents/<agent_id>/purge", methods=["DELETE"])
@require_daemon
def purge_agent(agent_id):
    """エージェントをDBから物理削除（タスク・ツール含む）"""
    with get_db() as conn:
        conn.execute("DELETE FROM tasks WHERE agent_id = ?", (agent_id,))
        conn.execute("DELETE FROM agent_tools WHERE agent_id = ?", (agent_id,))
        cur = conn.execute("DELETE FROM agents WHERE id = ?", (agent_id,))
        if cur.rowcount == 0:
            return jsonify({"error": "agent not found"}), 404
    return jsonify({"purged": True, "agent_id": agent_id})


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

    # daemon落ち時フォールバック: ELVIN_task かつ requester_id があれば30秒watchdog起動
    payload_data = data.get("payload", {})
    if task_type == "ELVIN_task" and payload_data.get("requester_id"):
        t = threading.Thread(
            target=_vps_elvin_task_fallback,
            args=(task_id, payload_data),
            daemon=True,
        )
        t.start()

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
    model = (data.get("model") or "").strip()
    task_row = None

    with get_db() as conn:
        task_row = conn.execute(
            "SELECT type, payload FROM tasks WHERE id = ? AND client_id = ?",
            (task_id, client["id"]),
        ).fetchone()
        conn.execute(
            "UPDATE tasks SET status = ?, result = ?, error = ?, completed_at = ?,"
            " tokens_in = ?, tokens_out = ?, model = ?"
            " WHERE id = ? AND client_id = ?",
            (
                status,
                json.dumps(data.get("result", {})),
                data.get("error", ""),
                now_iso(),
                tokens_in,
                tokens_out,
                model,
                task_id,
                client["id"],
            ),
        )

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
    return jsonify({
        "ok": True,
        "client_id": client["id"],
        "anthropic_api_key": client["anthropic_api_key"] or "",
        "anthropic_model": client["anthropic_model"] or "",
    })


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

        # LINE WORKS リプライ（引用）コンテキストを抽出
        _quote = msg.get("quote", msg.get("quotedMessage", {}))
        if isinstance(_quote, dict):
            _qt = (
                (_quote.get("contents") or {}).get("text")
                or _quote.get("contentPreview")
                or _quote.get("text")
                or ""
            )
            if _qt:
                text = f"[引用: {_qt[:300]}]\n{text}"

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
        line_user_id = source.get("userId", "")

        # LINE WORKS リプライ（引用）コンテキストを抽出
        quote_text = ""
        quote = msg.get("quote", msg.get("quotedMessage", {}))
        if isinstance(quote, dict):
            quote_text = (
                (quote.get("contents") or {}).get("text")
                or quote.get("contentPreview")
                or quote.get("text")
                or ""
            )
        if quote_text:
            text = f"[引用: {quote_text[:300]}]\n{text}"

        # 名前登録コマンド（スタッフが初回に送る）
        if text.startswith("名前登録:"):
            parts = text.split(":", 1)
            staff_name = parts[1].strip() if len(parts) > 1 else ""
            if staff_name and line_user_id:
                with get_db() as conn:
                    conn.execute(
                        "INSERT INTO staff (id, client_id, line_user_id, name, created_at)"
                        " VALUES (?, ?, ?, ?, ?)"
                        " ON CONFLICT(client_id, line_user_id) DO UPDATE SET name=excluded.name",
                        (str(uuid.uuid4()), client["id"], line_user_id, staff_name, now_iso())
                    )
                line_reply(reply_token, f"✅ 名前を「{staff_name}」として登録しました\n以降のメッセージは{staff_name}として処理されます")
            else:
                line_reply(reply_token, "❌ 名前を入力してください\n例: 名前登録:田中")
            continue

        with get_db() as conn:
            # スタッフ名を取得
            requester_name = ""
            if line_user_id:
                staff_row = conn.execute(
                    "SELECT name FROM staff WHERE client_id = ? AND line_user_id = ?",
                    (client["id"], line_user_id)
                ).fetchone()
                if staff_row:
                    requester_name = staff_row["name"]

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
                # 直近完了タスクをコンテキストとして注入（全エージェントから最新を取得）
                recent = conn.execute(
                    "SELECT result FROM tasks"
                    " WHERE client_id=? AND status='completed'"
                    " ORDER BY completed_at DESC LIMIT 1",
                    (client["id"],),
                ).fetchone()
                context_str = ""
                if recent and recent["result"]:
                    try:
                        recent_output = json.loads(recent["result"]).get("output", "")
                        if recent_output:
                            context_str = recent_output[:300]
                    except Exception:
                        pass

                # 曖昧指示（PDFにして/送って等）+ recent_contextにFILEパスがある場合、
                # VPS側でテキストを事前展開してURVANに明確な指示を渡す
                _ctx_file = re.search(r'\[FILE:([^\]]+)\]', context_str) if context_str else None
                if _ctx_file and text:
                    _fp = _ctx_file.group(1).strip()
                    if re.search(r'PDF|ＰＤＦ', text, re.IGNORECASE):
                        text = f"{_fp} をPDFに変換してAI事業グループに送って"
                    elif any(kw in text for kw in ["送って", "転送", "これ", "さっきの", "それ"]):
                        text = f"{_fp} をAI事業グループに送って"

                payload_dict = {"text": text, "reply_token": reply_token, "group_id": group_id}
                if requester_name:
                    payload_dict["requester_name"] = requester_name
                if context_str:
                    payload_dict["recent_context"] = context_str

                task_id = str(uuid.uuid4())
                conn.execute(
                    "INSERT INTO tasks (id, client_id, agent_id, type, payload, status, created_at)"
                    " VALUES (?, ?, ?, ?, ?, 'pending', ?)",
                    (
                        task_id, client["id"], agent["id"], "line_message",
                        json.dumps(payload_dict, ensure_ascii=False),
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
            "SELECT id, name, status, manager_status, last_seen, anthropic_model, anthropic_api_key FROM clients ORDER BY last_seen DESC"
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
            "anthropic_model": c["anthropic_model"] or "",
            "anthropic_api_key": c["anthropic_api_key"] or "",
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

    # ANTHROPIC_API_KEY があればVPS側で即時処理（runningに変えてからスレッド起動）
    # 'pending'のままだとデーモンが横取りするため必ず'running'にしてから開始する
    if ANTHROPIC_API_KEY:
        with get_db() as conn:
            conn.execute(
                "UPDATE tasks SET status = 'running' WHERE id = ?", (task_id,)
            )
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


def load_conversation_history(client_id: str, agent_id: str | None, limit: int = 20,
                              requester_id: str = None) -> list:
    """直近の会話履歴をAnthropicのmessages形式で返す。requester_id指定時はその送信者のみ取得"""
    with get_db() as conn:
        if agent_id and requester_id:
            rows = conn.execute(
                "SELECT role, content FROM conversations"
                " WHERE client_id = ? AND agent_id = ? AND requester_id = ?"
                " ORDER BY created_at DESC LIMIT ?",
                (client_id, agent_id, requester_id, limit),
            ).fetchall()
        elif agent_id:
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


def save_conversation(client_id: str, agent_id: str | None, role: str, content: str,
                      requester_id: str = None):
    with get_db() as conn:
        conn.execute(
            "INSERT INTO conversations (id, client_id, agent_id, role, content, requester_id, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (str(uuid.uuid4()), client_id, agent_id, role, content, requester_id, now_iso()),
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


# ── AI実行（ローカルエージェントからVPS SDK経由でAI呼び出し） ────────────

@app.route("/api/v1/ai/run", methods=["POST"])
def ai_run():
    """ローカルエージェントからプロンプトを受け取りAnthropicSDKで実行して返す。
    クライアント固有のAPIキー（DB）を優先し、未設定時はVPS環境変数にフォールバック。
    """
    token = client_token_from_request()
    client = get_client_by_token(token) if token else None
    if not client:
        return jsonify({"error": "invalid token"}), 401

    # クライアント固有キー優先、なければグローバルキー
    try:
        api_key = client["anthropic_api_key"] or ANTHROPIC_API_KEY
    except Exception:
        api_key = ANTHROPIC_API_KEY

    if not api_key:
        return jsonify({"error": "ANTHROPIC_API_KEY not configured"}), 503

    data = request.get_json(force=True)
    prompt = (data.get("prompt") or "").strip()
    system = (data.get("system") or "").strip()
    model = data.get("model") or ANTHROPIC_MODEL
    if not prompt:
        return jsonify({"error": "prompt is required"}), 400

    try:
        import anthropic as _ant
        ant = _ant.Anthropic(api_key=api_key)
        kwargs = {
            "model": model,
            "max_tokens": 4096,
            "messages": [{"role": "user", "content": prompt}],
        }
        if system:
            kwargs["system"] = system
        resp = ant.messages.create(**kwargs)
        output = "".join(b.text for b in resp.content if b.type == "text")
        return jsonify({
            "output": output,
            "tokens_in": resp.usage.input_tokens,
            "tokens_out": resp.usage.output_tokens,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Web検索プロキシ（顧客PC側にAPIキー不要） ─────────────────────────────

@app.route("/api/v1/search", methods=["POST"])
def web_search_proxy():
    """daemonからの検索リクエストをBrave Search APIに中継する。
    顧客側にBrave APIキーが不要になる。
    """
    token = client_token_from_request()
    client = get_client_by_token(token) if token else None
    if not client:
        return jsonify({"error": "invalid token"}), 401

    data = request.get_json(force=True)
    query = (data.get("query") or "").strip()
    count = min(int(data.get("count", 5)), 10)
    if not query:
        return jsonify({"error": "query is required"}), 400

    import urllib.request as _ur
    import urllib.parse as _up
    import json as _json

    import re as _re

    # Google Custom Search（最優先・結果がある場合のみ返す）
    if GOOGLE_SEARCH_API_KEY and GOOGLE_SEARCH_CX:
        try:
            url = (
                f"https://www.googleapis.com/customsearch/v1"
                f"?key={GOOGLE_SEARCH_API_KEY}&cx={GOOGLE_SEARCH_CX}"
                f"&q={_up.quote_plus(query)}&num={min(count, 10)}&lr=lang_ja"
            )
            with _ur.urlopen(_ur.Request(url), timeout=10) as resp:
                items = _json.loads(resp.read().decode()).get("items", [])
            lines = [f"・{r.get('title','')}\n  {r.get('link','')}\n  {r.get('snippet','')}" for r in items]
            if lines:
                return jsonify({"results": "\n\n".join(lines), "source": "google"})
        except Exception:
            pass  # フォールバックへ

    # DuckDuckGo HTML検索（本物の検索結果・APIキー不要）
    _ddg_err = ""
    try:
        ddg_req = _ur.Request(
            f"https://html.duckduckgo.com/html/?q={_up.quote_plus(query)}&kl=jp-jp",
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0",
                "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )
        with _ur.urlopen(ddg_req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")
        _ddg_err = f"html_len={len(html)},result__a={html.count('result__a')}"
        titles = _re.findall(r'class="result__a"[^>]*>(.*?)</a>', html)
        urls_found = _re.findall(r'class="result__url"[^>]*>\s*([^\s<]+)', html)
        snips = _re.findall(r'class="result__snippet"[^>]*>(.*?)</a>', html, _re.DOTALL)
        lines = []
        for i in range(min(len(titles), count)):
            title = _re.sub(r"<[^>]+>", "", titles[i]).strip()
            url_str = urls_found[i].strip() if i < len(urls_found) else ""
            snip = _re.sub(r"<[^>]+>", "", snips[i]).strip() if i < len(snips) else ""
            if title or snip:
                lines.append(f"・{title}\n  {url_str}\n  {snip}")
        if lines:
            return jsonify({"results": "\n\n".join(lines), "source": "duckduckgo_html"})
    except Exception as _e:
        _ddg_err = str(_e)[:200]

    # DuckDuckGo Instant Answers（最終フォールバック）
    try:
        req = _ur.Request(
            f"https://api.duckduckgo.com/?q={_up.quote_plus(query)}&format=json&no_html=1&skip_disambig=1",
            headers={"User-Agent": "ELVIN/1.0"},
        )
        with _ur.urlopen(req, timeout=8) as resp:
            data = _json.loads(resp.read().decode())
        snippets = []
        if data.get("Answer"):
            snippets.append(f"[回答] {data['Answer']}")
        if data.get("AbstractText"):
            snippets.append(data["AbstractText"][:400])
        for topic in data.get("RelatedTopics", [])[:count]:
            if isinstance(topic, dict) and topic.get("Text"):
                snippets.append(f"・{topic['Text'][:200]}")
        results_text = "\n".join(snippets[:count + 1]) if snippets else f"結果なし[ddg_debug:{_ddg_err}]"
        return jsonify({"results": results_text, "source": "duckduckgo"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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
        " model,"
        " SUM(tokens_in) as tokens_in, SUM(tokens_out) as tokens_out,"
        " COUNT(*) as task_count"
        " FROM tasks WHERE status = 'completed'"
        " AND created_at >= datetime('now', ?)"
    )
    params = [f"-{days} days"]
    if client_id and client_id != "all":
        base_q += " AND client_id = ?"
        params.append(client_id)
    base_q += " GROUP BY hour, model ORDER BY hour ASC"
    with get_db() as conn:
        rows = conn.execute(base_q, params).fetchall()
    # hour × model の行をhour単位に集約（モデル別料金でコスト計算）
    from collections import defaultdict
    hourly = defaultdict(lambda: {"tokens_in": 0, "tokens_out": 0, "task_count": 0, "cost_usd": 0.0, "models": {}})
    for r in rows:
        h = r["hour"]
        t_in = r["tokens_in"] or 0
        t_out = r["tokens_out"] or 0
        model = r["model"] or ""
        p_in, p_out = _pricing(model)
        cost_usd = (t_in * p_in + t_out * p_out) / 1_000_000
        hourly[h]["tokens_in"] += t_in
        hourly[h]["tokens_out"] += t_out
        hourly[h]["task_count"] += r["task_count"]
        hourly[h]["cost_usd"] += cost_usd
        short = model.replace("claude-", "").replace("-20251001", "").replace("-20250929", "") or "unknown"
        hourly[h]["models"][short] = hourly[h]["models"].get(short, 0) + r["task_count"]
    result = []
    for hour in sorted(hourly.keys()):
        d = hourly[hour]
        dominant = max(d["models"], key=d["models"].get) if d["models"] else "unknown"
        result.append({
            "hour": hour,
            "tokens_in": d["tokens_in"],
            "tokens_out": d["tokens_out"],
            "task_count": d["task_count"],
            "cost_jpy": int(d["cost_usd"] * _USD_TO_JPY),
            "model": dominant,
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


@app.route("/api/v1/clients/<client_id>/staff", methods=["GET"])
@require_daemon
def list_staff(client_id):
    """登録済みスタッフ一覧"""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, line_user_id, name, created_at FROM staff"
            " WHERE client_id = ? ORDER BY created_at ASC",
            (client_id,),
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/v1/clients/<client_id>/staff/<staff_id>", methods=["DELETE"])
@require_daemon
def delete_staff(client_id, staff_id):
    """スタッフ削除"""
    with get_db() as conn:
        conn.execute("DELETE FROM staff WHERE client_id = ? AND id = ?", (client_id, staff_id))
    return jsonify({"ok": True})


@app.route("/api/v1/staff", methods=["GET", "POST"])
def list_or_create_staff_by_token():
    """スタッフ一覧取得 or 登録（ELVIN MANAGER / line-webhook.php 用・X-Client-Token認証）"""
    token = client_token_from_request()
    client = get_client_by_token(token) if token else None
    if not client:
        return jsonify({"error": "invalid token"}), 401
    if request.method == "POST":
        data = request.get_json(force=True) or {}
        line_user_id = data.get("line_user_id", "")
        name = data.get("name", "").strip()
        if not line_user_id or not name:
            return jsonify({"error": "line_user_id and name are required"}), 400
        with get_db() as conn:
            conn.execute(
                "INSERT INTO staff (id, client_id, line_user_id, name, created_at)"
                " VALUES (?, ?, ?, ?, ?)"
                " ON CONFLICT(client_id, line_user_id) DO UPDATE SET name=excluded.name",
                (str(uuid.uuid4()), client["id"], line_user_id, name, now_iso()),
            )
        return jsonify({"ok": True, "name": name})
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, line_user_id, name, created_at FROM staff"
            " WHERE client_id = ? ORDER BY created_at ASC",
            (client["id"],),
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/v1/staff/<staff_id>", methods=["DELETE"])
def delete_staff_by_token(staff_id):
    """スタッフ削除（ELVIN MANAGER用・X-Client-Token認証）"""
    token = client_token_from_request()
    client = get_client_by_token(token) if token else None
    if not client:
        return jsonify({"error": "invalid token"}), 401
    with get_db() as conn:
        conn.execute(
            "DELETE FROM staff WHERE client_id = ? AND id = ?",
            (client["id"], staff_id),
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

# ── daemon向け 記憶・会話履歴 内部API ────────────────────────────────────
# client_token で認証（daemonはX-Client-Tokenを保持している）

def _require_own_client(client_id: str):
    """client_tokenが自分のclient_idと一致するか確認。一致しない場合はレスポンスを返す。"""
    token = client_token_from_request()
    client = get_client_by_token(token) if token else None
    if not client or client["id"] != client_id:
        return jsonify({"error": "unauthorized"}), 401
    return None


@app.route("/api/v1/internal/memories/<client_id>", methods=["GET"])
def internal_get_memories(client_id):
    err = _require_own_client(client_id)
    if err:
        return err
    return jsonify({"memories_text": load_memories(client_id)})


@app.route("/api/v1/internal/memories/<client_id>", methods=["POST"])
def internal_save_memory(client_id):
    err = _require_own_client(client_id)
    if err:
        return err
    data = request.get_json(force=True)
    upsert_memory(client_id, data.get("category", "general"), data.get("key", ""), data.get("value", ""))
    return jsonify({"ok": True})


@app.route("/api/v1/internal/conversations/<client_id>/<agent_id>", methods=["GET"])
def internal_get_conversations(client_id, agent_id):
    err = _require_own_client(client_id)
    if err:
        return err
    limit = min(int(request.args.get("limit", 20)), 50)
    requester_id = request.args.get("requester_id") or None
    history = load_conversation_history(client_id, agent_id, limit=limit, requester_id=requester_id)
    return jsonify(history)


@app.route("/api/v1/internal/conversations/<client_id>/<agent_id>", methods=["POST"])
def internal_save_conversation(client_id, agent_id):
    err = _require_own_client(client_id)
    if err:
        return err
    data = request.get_json(force=True)
    save_conversation(client_id, agent_id, data.get("role", "user"), data.get("content", ""),
                      requester_id=data.get("requester_id") or None)
    return jsonify({"ok": True})


# ── 発注アラート共通認証ヘルパー ─────────────────────────────────────────

def _auth_client_for_alerts():
    """client token OR daemon secret + client_id の両方を受け入れる"""
    token = client_token_from_request()
    if token:
        return get_client_by_token(token)
    secret = request.headers.get("X-Daemon-Secret", "")
    if secret == DAEMON_SECRET:
        data = request.get_json(force=True, silent=True) or {}
        client_id = request.args.get("client_id") or data.get("client_id", "")
        if client_id:
            with get_db() as conn:
                return conn.execute("SELECT * FROM clients WHERE id = ?", (client_id,)).fetchone()
    return None


# ── 発注品目マスター API ─────────────────────────────────────────────────

@app.route("/api/v1/client/order-master", methods=["GET"])
def get_order_master():
    client = _auth_client_for_alerts()
    if not client:
        return jsonify({"error": "unauthorized"}), 401
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM order_master WHERE client_id = ? ORDER BY created_at ASC",
            (client["id"],)
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/v1/client/order-master", methods=["POST"])
def create_order_master():
    client = _auth_client_for_alerts()
    if not client:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(force=True)
    name = data.get("name", "").strip()
    if not name:
        return jsonify({"error": "name is required"}), 400
    item_id = str(uuid.uuid4())[:12]
    with get_db() as conn:
        conn.execute(
            "INSERT INTO order_master (id, client_id, name, category, lead_time_days, unit, unit_price, memo, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (item_id, client["id"], name,
             data.get("category", ""), int(data.get("lead_time_days", 14)),
             data.get("unit", ""), int(data.get("unit_price", 0)),
             data.get("memo", ""), now_iso())
        )
    return jsonify({"id": item_id, "name": name}), 201


@app.route("/api/v1/client/order-master/<item_id>", methods=["PATCH"])
def update_order_master(item_id):
    client = _auth_client_for_alerts()
    if not client:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(force=True)
    allowed = {"name", "category", "lead_time_days", "unit", "unit_price", "memo"}
    fields = {k: v for k, v in data.items() if k in allowed}
    if not fields:
        return jsonify({"error": "no updatable fields"}), 400
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    with get_db() as conn:
        conn.execute(
            f"UPDATE order_master SET {set_clause} WHERE id = ? AND client_id = ?",
            (*fields.values(), item_id, client["id"])
        )
    return jsonify({"ok": True})


@app.route("/api/v1/client/order-master/<item_id>", methods=["DELETE"])
def delete_order_master(item_id):
    client = _auth_client_for_alerts()
    if not client:
        return jsonify({"error": "unauthorized"}), 401
    with get_db() as conn:
        conn.execute(
            "DELETE FROM order_master WHERE id = ? AND client_id = ?",
            (item_id, client["id"])
        )
    return jsonify({"ok": True})


# ── 着工アラート API ──────────────────────────────────────────────────────

@app.route("/api/v1/client/construction-alerts", methods=["GET"])
def get_construction_alerts():
    client = _auth_client_for_alerts()
    if not client:
        return jsonify({"error": "unauthorized"}), 401
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM construction_alerts WHERE client_id = ? ORDER BY construction_date ASC",
            (client["id"],)
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/v1/client/construction-alerts/upcoming", methods=["GET"])
def get_upcoming_construction_alerts():
    """発注期限が今日から30日以内のアラートを返す"""
    client = _auth_client_for_alerts()
    if not client:
        return jsonify({"error": "unauthorized"}), 401
    from datetime import date, timedelta
    today = date.today().isoformat()
    future = (date.today() + timedelta(days=30)).isoformat()
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM construction_alerts WHERE client_id = ? AND status = 'pending'"
            " ORDER BY construction_date ASC",
            (client["id"],)
        ).fetchall()
    result = []
    for r in rows:
        try:
            cd = r["construction_date"]
            ltd = r["lead_time_days"] or 14
            from datetime import date as _date
            cd_date = _date.fromisoformat(cd)
            order_date = cd_date - timedelta(days=ltd)
            order_date_str = order_date.isoformat()
            if order_date_str <= future:
                d = dict(r)
                d["order_date"] = order_date_str
                d["days_until_order"] = (order_date - _date.today()).days
                result.append(d)
        except Exception:
            pass
    return jsonify(result)


@app.route("/api/v1/client/construction-alerts", methods=["POST"])
def create_construction_alert():
    client = _auth_client_for_alerts()
    if not client:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(force=True)
    project_name = data.get("project_name", "").strip()
    construction_date = data.get("construction_date", "").strip()
    if not project_name or not construction_date:
        return jsonify({"error": "project_name and construction_date are required"}), 400
    alert_id = str(uuid.uuid4())[:12]
    lead_time = int(data.get("lead_time_days", 14))
    item_id = data.get("order_item_id", "")
    item_name = data.get("order_item_name", "")
    order_amount = int(data.get("order_amount", 0))
    if item_id and not item_name:
        with get_db() as conn:
            item = conn.execute("SELECT name FROM order_master WHERE id = ?", (item_id,)).fetchone()
            if item:
                item_name = item["name"]
                if not lead_time or lead_time == 14:
                    lt_row = conn.execute("SELECT lead_time_days FROM order_master WHERE id = ?", (item_id,)).fetchone()
                    if lt_row:
                        lead_time = lt_row["lead_time_days"]
    with get_db() as conn:
        conn.execute(
            "INSERT INTO construction_alerts"
            " (id, client_id, project_name, construction_date, order_item_id,"
            "  order_item_name, lead_time_days, lineworks_room, order_amount, status, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)",
            (alert_id, client["id"], project_name, construction_date,
             item_id, item_name, lead_time,
             data.get("lineworks_room", ""), order_amount, now_iso())
        )
    return jsonify({"id": alert_id, "project_name": project_name}), 201


@app.route("/api/v1/client/construction-alerts/<alert_id>", methods=["PATCH"])
def update_construction_alert(alert_id):
    client = _auth_client_for_alerts()
    if not client:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(force=True)
    allowed = {"project_name", "construction_date", "order_item_name",
               "lead_time_days", "lineworks_room", "status", "order_amount", "delivered_at"}
    fields = {k: v for k, v in data.items() if k in allowed}
    if not fields:
        return jsonify({"error": "no updatable fields"}), 400
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    with get_db() as conn:
        conn.execute(
            f"UPDATE construction_alerts SET {set_clause} WHERE id = ? AND client_id = ?",
            (*fields.values(), alert_id, client["id"])
        )
    return jsonify({"ok": True})


@app.route("/api/v1/client/construction-alerts/<alert_id>", methods=["DELETE"])
def delete_construction_alert(alert_id):
    client = _auth_client_for_alerts()
    if not client:
        return jsonify({"error": "unauthorized"}), 401
    with get_db() as conn:
        conn.execute(
            "DELETE FROM construction_alerts WHERE id = ? AND client_id = ?",
            (alert_id, client["id"])
        )
    return jsonify({"ok": True})


# ── 期限アラート CRUD ────────────────────────────────────────────────────

@app.route("/api/v1/client/deadline-alerts", methods=["GET"])
def get_deadline_alerts():
    client = _auth_client_for_alerts()
    if not client:
        return jsonify({"error": "unauthorized"}), 401
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM deadline_alerts WHERE client_id = ? ORDER BY deadline ASC",
            (client["id"],)
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/v1/client/deadline-alerts", methods=["POST"])
def create_deadline_alert():
    client = _auth_client_for_alerts()
    if not client:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(force=True)
    project_name = data.get("project_name", "").strip()
    category = data.get("category", "").strip()
    deadline = data.get("deadline", "").strip()
    lineworks_room = data.get("lineworks_room", "").strip()
    if not project_name or not category or not deadline or not lineworks_room:
        return jsonify({"error": "project_name, category, deadline, lineworks_room are required"}), 400
    alert_id = str(uuid.uuid4())[:12]
    with get_db() as conn:
        conn.execute(
            "INSERT INTO deadline_alerts"
            " (id, client_id, project_name, category, deadline, alert_days_before,"
            "  lineworks_room, status, memo, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)",
            (alert_id, client["id"], project_name, category, deadline,
             int(data.get("alert_days_before", 7)), lineworks_room,
             data.get("memo", ""), now_iso())
        )
    return jsonify({"id": alert_id, "project_name": project_name}), 201


@app.route("/api/v1/client/deadline-alerts/<alert_id>", methods=["PATCH"])
def update_deadline_alert(alert_id):
    client = _auth_client_for_alerts()
    if not client:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(force=True)
    allowed = {"project_name", "category", "deadline", "alert_days_before",
               "lineworks_room", "status", "memo"}
    fields = {k: v for k, v in data.items() if k in allowed}
    if not fields:
        return jsonify({"error": "no updatable fields"}), 400
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    with get_db() as conn:
        conn.execute(
            f"UPDATE deadline_alerts SET {set_clause} WHERE id = ? AND client_id = ?",
            (*fields.values(), alert_id, client["id"])
        )
    return jsonify({"ok": True})


@app.route("/api/v1/client/deadline-alerts/<alert_id>", methods=["DELETE"])
def delete_deadline_alert(alert_id):
    client = _auth_client_for_alerts()
    if not client:
        return jsonify({"error": "unauthorized"}), 401
    with get_db() as conn:
        conn.execute(
            "DELETE FROM deadline_alerts WHERE id = ? AND client_id = ?",
            (alert_id, client["id"])
        )
    return jsonify({"ok": True})


# ── 着工チェックリスト CRUD ──────────────────────────────────────────────

@app.route("/api/v1/client/checklist-alerts", methods=["GET"])
def get_checklist_alerts():
    client = _auth_client_for_alerts()
    if not client:
        return jsonify({"error": "unauthorized"}), 401
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM checklist_alerts WHERE client_id = ? ORDER BY construction_date ASC",
            (client["id"],)
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/v1/client/checklist-alerts", methods=["POST"])
def create_checklist_alert():
    client = _auth_client_for_alerts()
    if not client:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(force=True)
    construction_name = data.get("construction_name", "").strip()
    construction_date = data.get("construction_date", "").strip()
    lineworks_room = data.get("lineworks_room", "").strip()
    if not construction_name or not construction_date or not lineworks_room:
        return jsonify({"error": "construction_name, construction_date, lineworks_room are required"}), 400
    alert_id = str(uuid.uuid4())[:12]
    with get_db() as conn:
        conn.execute(
            "INSERT INTO checklist_alerts"
            " (id, client_id, construction_name, construction_date, lineworks_room,"
            "  check_materials, check_contractor, check_instruction, status, created_at)"
            " VALUES (?, ?, ?, ?, ?, 0, 0, 0, 'pending', ?)",
            (alert_id, client["id"], construction_name, construction_date, lineworks_room, now_iso())
        )
    return jsonify({"id": alert_id, "construction_name": construction_name}), 201


@app.route("/api/v1/client/checklist-alerts/<alert_id>", methods=["PATCH"])
def update_checklist_alert(alert_id):
    client = _auth_client_for_alerts()
    if not client:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(force=True)
    allowed = {"construction_name", "construction_date", "lineworks_room",
               "check_materials", "check_contractor", "check_instruction", "status"}
    fields = {k: v for k, v in data.items() if k in allowed}
    if not fields:
        return jsonify({"error": "no updatable fields"}), 400
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    with get_db() as conn:
        conn.execute(
            f"UPDATE checklist_alerts SET {set_clause} WHERE id = ? AND client_id = ?",
            (*fields.values(), alert_id, client["id"])
        )
    return jsonify({"ok": True})


@app.route("/api/v1/client/checklist-alerts/<alert_id>", methods=["DELETE"])
def delete_checklist_alert(alert_id):
    client = _auth_client_for_alerts()
    if not client:
        return jsonify({"error": "unauthorized"}), 401
    with get_db() as conn:
        conn.execute(
            "DELETE FROM checklist_alerts WHERE id = ? AND client_id = ?",
            (alert_id, client["id"])
        )
    return jsonify({"ok": True})


# ── 発注アラートスケジューラ ────────────────────────────────────────────

# ── 案件ルーティンテンプレート (C-18) ────────────────────────────────────

_ROUTINE_STEPS = [
    ("現調",               -60, "案件ルーティン"),
    ("現調内容共有",        -55, "案件ルーティン"),
    ("現調資料まとめ",      -50, "案件ルーティン"),
    ("図面・施工指示書作成",-45, "案件ルーティン"),
    ("見積・積算",          -40, "案件ルーティン"),
    ("契約",               -35, "案件ルーティン"),
    ("発注・申請",          -30, "案件ルーティン"),
    ("下請・職人手配",      -21, "案件ルーティン"),
    ("施工内容打合",        -14, "案件ルーティン"),
    ("着工",                 0,  "案件ルーティン"),
    ("監督情報共有",          3, "案件ルーティン"),
    ("現場調整協議",          7, "案件ルーティン"),
    ("完了",                14, "案件ルーティン"),
    ("監督検査",             21, "案件ルーティン"),
    ("施主検査",             28, "案件ルーティン"),
    ("竣工",                35, "案件ルーティン"),
    ("請求・入金確認",       42, "案件ルーティン"),
]


@app.route("/api/v1/client/project-routines/apply", methods=["POST"])
def apply_project_routine():
    client = _auth_client_for_alerts()
    if not client:
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(force=True)
    project_name = data.get("project_name", "").strip()
    construction_date = data.get("construction_date", "").strip()
    lineworks_room = data.get("lineworks_room", "").strip()
    alert_days_before = int(data.get("alert_days_before", 3))
    if not project_name or not construction_date or not lineworks_room:
        return jsonify({"error": "project_name, construction_date, lineworks_room are required"}), 400
    from datetime import date as _date, timedelta
    try:
        base = _date.fromisoformat(construction_date)
    except ValueError:
        return jsonify({"error": "invalid construction_date"}), 400
    routine_id = str(uuid.uuid4())[:12]
    created = []
    with get_db() as conn:
        conn.execute(
            "INSERT INTO project_routines (id, client_id, project_name, construction_date, lineworks_room, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (routine_id, client["id"], project_name, construction_date, lineworks_room, now_iso())
        )
        for step_name, offset_days, category in _ROUTINE_STEPS:
            step_date = (base + timedelta(days=offset_days)).isoformat()
            step_id = str(uuid.uuid4())[:12]
            conn.execute(
                "INSERT INTO deadline_alerts"
                " (id, client_id, project_name, category, deadline, alert_days_before,"
                "  lineworks_room, status, memo, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)",
                (step_id, client["id"], project_name, step_name, step_date,
                 alert_days_before, lineworks_room, "", now_iso())
            )
            created.append({"step": step_name, "deadline": step_date})
    return jsonify({"routine_id": routine_id, "steps_created": len(created), "steps": created}), 201


def _check_construction_alerts():
    """発注期限が到来した着工アラートをチェックして lineworks_send タスクを作成する"""
    from datetime import date, timedelta
    today = date.today()
    today_str = today.isoformat()

    with get_db() as conn:
        alerts = conn.execute(
            "SELECT ca.*, c.name as client_name FROM construction_alerts ca"
            " JOIN clients c ON ca.client_id = c.id"
            " WHERE ca.status = 'pending'"
        ).fetchall()

    for alert in alerts:
        try:
            cd = date.fromisoformat(alert["construction_date"])
            ltd = alert["lead_time_days"] or 14
            order_date = cd - timedelta(days=ltd)

            if today < order_date:
                continue

            last_notified = alert["notified_at"] or ""
            if last_notified and last_notified[:10] == today_str:
                continue

            item_name = alert["order_item_name"] or "指定品目"
            room = alert["lineworks_room"] or ""
            days_late = (today - order_date).days

            if days_late == 0:
                urgency = "⚠️ 本日が発注期限です"
            elif days_late > 0:
                urgency = f"🚨 発注期限を {days_late} 日超過しています"
            else:
                urgency = f"📅 発注期限まであと {-days_late} 日です"

            message = (
                f"【発注アラート】{urgency}\n"
                f"現場: {alert['project_name']}\n"
                f"品目: {item_name}（リードタイム: {ltd}日）\n"
                f"着工予定: {alert['construction_date']}\n"
                f"発注期限: {order_date.isoformat()}\n"
                f"※ 発注が完了したらELVIN ADMINで「発注済」に更新してください"
            )

            task_id = str(uuid.uuid4())
            payload = json.dumps({
                "message": message,
                "room_name": room,
            }, ensure_ascii=False)

            with get_db() as conn:
                conn.execute(
                    "INSERT INTO tasks (id, client_id, type, payload, status, created_at)"
                    " VALUES (?, ?, 'lineworks_send', ?, 'pending', ?)",
                    (task_id, alert["client_id"], payload, now_iso())
                )
                conn.execute(
                    "UPDATE construction_alerts SET notified_at = ? WHERE id = ?",
                    (now_iso(), alert["id"])
                )
            print(f"[SCHEDULER] アラート通知タスク作成: {alert['project_name']} / {item_name}")

        except Exception as e:
            print(f"[SCHEDULER] アラート処理エラー id={alert['id']}: {e}")


def _check_deadline_alerts():
    """期限が近づいた deadline_alerts をチェックして lineworks_send タスクを作成する"""
    from datetime import date, timedelta
    today = date.today()
    today_str = today.isoformat()

    with get_db() as conn:
        alerts = conn.execute(
            "SELECT da.*, c.name as client_name FROM deadline_alerts da"
            " JOIN clients c ON da.client_id = c.id"
            " WHERE da.status = 'pending'"
        ).fetchall()

    for alert in alerts:
        try:
            deadline = date.fromisoformat(alert["deadline"])
            days_before = alert["alert_days_before"] or 7
            notify_from = deadline - timedelta(days=days_before)

            if today < notify_from:
                continue

            last_notified = alert["notified_at"] or ""
            if last_notified and last_notified[:10] == today_str:
                continue

            days_left = (deadline - today).days
            if days_left > 0:
                urgency = f"📅 期限まであと {days_left} 日です"
            elif days_left == 0:
                urgency = "⚠️ 本日が期限です"
            else:
                urgency = f"🚨 期限を {-days_left} 日超過しています"

            message = (
                f"【期限アラート】{urgency}\n"
                f"現場: {alert['project_name']}\n"
                f"種別: {alert['category']}\n"
                f"期限: {alert['deadline']}\n"
                + (f"メモ: {alert['memo']}\n" if alert["memo"] else "")
                + f"※ 完了したらELVIN MANAGERで「完了」にしてください"
            )

            task_id = str(uuid.uuid4())
            payload = json.dumps({
                "message": message,
                "room_name": alert["lineworks_room"],
            }, ensure_ascii=False)

            with get_db() as conn:
                conn.execute(
                    "INSERT INTO tasks (id, client_id, type, payload, status, created_at)"
                    " VALUES (?, ?, 'lineworks_send', ?, 'pending', ?)",
                    (task_id, alert["client_id"], payload, now_iso())
                )
                conn.execute(
                    "UPDATE deadline_alerts SET notified_at = ? WHERE id = ?",
                    (now_iso(), alert["id"])
                )
            print(f"[SCHEDULER] 期限アラート通知タスク作成: {alert['project_name']} / {alert['category']}")

        except Exception as e:
            print(f"[SCHEDULER] 期限アラート処理エラー id={alert['id']}: {e}")


def _check_checklist_alerts():
    """着工7日前チェックリストをチェックして未確認項目をLINE通知する"""
    from datetime import date, timedelta
    today = date.today()
    today_str = today.isoformat()
    notify_threshold = today + timedelta(days=7)

    with get_db() as conn:
        alerts = conn.execute(
            "SELECT ca.*, c.name as client_name FROM checklist_alerts ca"
            " JOIN clients c ON ca.client_id = c.id"
            " WHERE ca.status = 'pending'"
        ).fetchall()

    for alert in alerts:
        try:
            cd = date.fromisoformat(alert["construction_date"])

            if cd > notify_threshold:
                continue

            last_notified = alert["notified_at"] or ""
            if last_notified and last_notified[:10] == today_str:
                continue

            unchecked = []
            if not alert["check_materials"]:
                unchecked.append("部材確定")
            if not alert["check_contractor"]:
                unchecked.append("下請確定")
            if not alert["check_instruction"]:
                unchecked.append("施工指示書配布")

            if not unchecked:
                with get_db() as conn:
                    conn.execute(
                        "UPDATE checklist_alerts SET status = 'completed', notified_at = ? WHERE id = ?",
                        (now_iso(), alert["id"])
                    )
                continue

            days_until = (cd - today).days
            if days_until > 0:
                timing = f"着工まであと {days_until} 日"
            elif days_until == 0:
                timing = "本日着工"
            else:
                timing = f"着工日を {-days_until} 日超過"

            message = (
                f"【着工チェックリスト】⚠️ 未確認項目があります\n"
                f"現場: {alert['construction_name']}\n"
                f"着工予定: {alert['construction_date']}（{timing}）\n"
                f"未確認: {' / '.join(unchecked)}\n"
                f"※ ELVIN MANAGERでチェックを入れてください"
            )

            task_id = str(uuid.uuid4())
            payload = json.dumps({
                "message": message,
                "room_name": alert["lineworks_room"],
            }, ensure_ascii=False)

            with get_db() as conn:
                conn.execute(
                    "INSERT INTO tasks (id, client_id, type, payload, status, created_at)"
                    " VALUES (?, ?, 'lineworks_send', ?, 'pending', ?)",
                    (task_id, alert["client_id"], payload, now_iso())
                )
                conn.execute(
                    "UPDATE checklist_alerts SET notified_at = ? WHERE id = ?",
                    (now_iso(), alert["id"])
                )
            print(f"[SCHEDULER] チェックリスト通知タスク作成: {alert['construction_name']} / 未確認: {', '.join(unchecked)}")

        except Exception as e:
            print(f"[SCHEDULER] チェックリスト処理エラー id={alert['id']}: {e}")


def _alert_scheduler_loop():
    import time as _time
    from datetime import datetime as _dt
    _time.sleep(10)  # 起動直後は待機
    while True:
        try:
            now_h = _dt.now().hour
            if 7 <= now_h <= 21:  # 業務時間帯のみ実行
                _check_construction_alerts()
                _check_deadline_alerts()
                _check_checklist_alerts()
        except Exception as e:
            print(f"[SCHEDULER] ループエラー: {e}")
        _time.sleep(3600)  # 1時間ごと


# ── 起動 ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    _sched_thread = threading.Thread(target=_alert_scheduler_loop, daemon=True, name="alert-scheduler")
    _sched_thread.start()
    print(f"[ELVIN VPS API] http://0.0.0.0:{PORT}")
    print(f"[ELVIN VPS API] DAEMON_SECRET={DAEMON_SECRET!r}")
    app.run(host="0.0.0.0", port=PORT, debug=False)
