"""
ELVIN ローカルエージェント
顧客PC常駐スクリプト / .exe配布版

設定方法:
  同じフォルダに elvin_config.json を置いてください:
  {
    "vps_url": "https://api.nihon-neon.jp",
    "client_token": "your_client_token",
    "poll_interval": 5,
    "work_dir": "lineworks_send.py を置いたフォルダのフルパス",
    "lineworks_room": "報告先 LINE WORKS グループ名"
  }

起動:
  elvin_agent.exe        （.exe版）
  python agent_local.py  （スクリプト版）

環境変数（任意・config.jsonより優先）:
  ELVIN_VPS_URL    VPS APIのURL
  ELVIN_TOKEN      顧客トークン
  POLL_INTERVAL    ポーリング間隔秒
"""

import base64
import json
import os
import platform
import re
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import requests

# ── 設定読み込み ──────────────────────────────────────────────────────────
# .exe 化時は sys.executable のあるフォルダ、スクリプト実行時は __file__ のフォルダ
_base_dir = Path(sys.executable).parent if getattr(sys, "frozen", False) else Path(__file__).parent
_config_path = _base_dir / "elvin_config.json"

_cfg: dict = {}
if _config_path.exists():
    try:
        _cfg = json.loads(_config_path.read_text(encoding="utf-8-sig"))
    except Exception as e:
        print(f"[WARN] elvin_config.json 読み込みエラー: {e}")

VPS_URL = (
    os.environ.get("ELVIN_VPS_URL")
    or os.environ.get("BRAINTRUST_VPS_URL")
    or _cfg.get("vps_url", "http://localhost:5050")
).rstrip("/")

CLIENT_TOKEN = (
    os.environ.get("ELVIN_TOKEN")
    or os.environ.get("BRAINTRUST_TOKEN")
    or _cfg.get("client_token", "")
)

POLL_INTERVAL = int(
    os.environ.get("POLL_INTERVAL")
    or _cfg.get("poll_interval", 5)
)

# work_dir: lineworks_send.py の置き場所 + Claude 実行の作業フォルダ
# 空の場合は .exe / .py と同じフォルダを使う
_work_dir_cfg = _cfg.get("work_dir", "").strip()
WORK_DIR = _work_dir_cfg if (_work_dir_cfg and Path(_work_dir_cfg).exists()) else str(_base_dir)

# LINE WORKS の報告先グループ名（システムプロンプトに埋め込む）
LINEWORKS_ROOM = _cfg.get("lineworks_room", "")

if not CLIENT_TOKEN:
    print("[ERROR] client_token が設定されていません")
    print(f"        elvin_config.json の場所: {_config_path}")
    sys.exit(1)

HEADERS = {"X-Client-Token": CLIENT_TOKEN}

# 起動時に取得するエージェント一覧 [{agent_id, name, role, system_prompt, tools}, ...]
AGENTS: list[dict] = []
CLIENT_ID: str = ""


def ts():
    return datetime.now().strftime("%H:%M:%S")


def vlog(message: str, level: str = "info", agent_id: str = None):
    """VPS のデーモンログAPIに非同期送信（失敗しても無視）"""
    def _send():
        try:
            requests.post(
                f"{VPS_URL}/api/v1/logs",
                headers=HEADERS,
                json={"level": level, "message": message, "agent_id": agent_id},
                timeout=5,
            )
        except Exception:
            pass
    threading.Thread(target=_send, daemon=True).start()


def _resolve_room(prompt: str, requester_id: str, work_dir: str) -> str:
    """返信先 LINE WORKS ルーム名を3段優先で決定する。
    1. プロンプト内の「返信先LINE WORKSルーム名:」ヘッダー
    2. lineworks-room-map.json[requester_id]
    3. config の LINEWORKS_ROOM（フォールバック）
    """
    m = re.search(r'返信先LINE\s*WORKSルーム名[:：]\s*(.+)', prompt)
    if m:
        return m.group(1).strip()
    if requester_id:
        room_map_path = Path(work_dir) / "lineworks-room-map.json"
        if room_map_path.exists():
            try:
                room_map = json.loads(room_map_path.read_text(encoding="utf-8"))
                room = room_map.get(requester_id)
                if room:
                    return room
            except Exception:
                pass
    return LINEWORKS_ROOM


def reload_agents():
    global AGENTS
    try:
        resp = requests.get(f"{VPS_URL}/api/v1/client/agents", headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            AGENTS = resp.json()
            print(f"[{ts()}] エージェント再読込: {len(AGENTS)}体")
    except Exception as e:
        print(f"[{ts()}] エージェント再読込失敗: {e}")


def _find_agent_id(name: str) -> str:
    for a in AGENTS:
        if a.get("name", "").lower() == name.lower():
            return a.get("agent_id", "")
    return ""


def handle_admin(cmd: str, requester_id: str, work_dir: str) -> str:
    try:
        data = json.loads(cmd)
    except json.JSONDecodeError as e:
        return f"JSON解析エラー: {e}"

    action = data.get("action", "")
    base = f"{VPS_URL}/api/v1/manage/agents"

    if action == "list":
        resp = requests.get(base, headers=HEADERS, timeout=10)
        agents = resp.json() if resp.status_code == 200 else []
        lines = [f"・{a['name']}（{a.get('role') or '役割未設定'}）[{a['id']}]" for a in agents]
        return "現在のエージェント一覧:\n" + "\n".join(lines) if lines else "エージェントなし"

    elif action == "add":
        name = data.get("name", "")
        sp = data.get("system_prompt", "")
        if not name or not sp:
            return "name と system_prompt が必要です"
        resp = requests.post(
            base, headers=HEADERS,
            json={"name": name, "role": data.get("role", ""), "system_prompt": sp},
            timeout=10,
        )
        if resp.status_code == 201:
            r = resp.json()
            reload_agents()
            return f"エージェント「{r['name']}」を追加しました（ID: {r['agent_id']}）"
        return f"追加失敗 ({resp.status_code}): {resp.text[:200]}"

    elif action == "update":
        aid = data.get("agent_id") or _find_agent_id(data.get("name", ""))
        if not aid:
            return "agent_id または name（既存エージェント名）が必要です"
        update_fields = {k: data[k] for k in ("name", "role", "system_prompt") if k in data and k != "action"}
        resp = requests.patch(f"{base}/{aid}", headers=HEADERS, json=update_fields, timeout=10)
        if resp.status_code == 200:
            reload_agents()
            return f"エージェント（{aid}）を更新しました"
        return f"更新失敗 ({resp.status_code}): {resp.text[:200]}"

    elif action == "remove":
        aid = data.get("agent_id") or _find_agent_id(data.get("name", ""))
        if not aid:
            return "agent_id または name（既存エージェント名）が必要です"
        resp = requests.delete(f"{base}/{aid}", headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            reload_agents()
            return f"エージェント（{aid}）を削除しました"
        return f"削除失敗 ({resp.status_code}): {resp.text[:200]}"

    return f"不明なアクション: {action!r}"


# ── タスク実行 ────────────────────────────────────────────────────────────

def execute(task: dict, agent: dict) -> dict:
    t = task["type"]
    p = task.get("payload", {})

    if t == "file_read":
        path = p["path"]
        encoding = p.get("encoding", "utf-8")
        content = Path(path).read_text(encoding=encoding)
        return {"content": content, "size": len(content)}

    elif t == "file_write":
        path = p["path"]
        content = p["content"]
        encoding = p.get("encoding", "utf-8")
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(content, encoding=encoding)
        return {"written": True, "path": str(path)}

    elif t == "file_list":
        path = p.get("path", str(Path.home()))
        entries = []
        for item in Path(path).iterdir():
            entries.append({
                "name": item.name,
                "is_dir": item.is_dir(),
                "size": item.stat().st_size if item.is_file() else None,
            })
        return {"path": str(path), "entries": sorted(entries, key=lambda x: x["name"])}

    elif t == "screenshot":
        try:
            import mss
            import mss.tools
        except ImportError:
            raise RuntimeError("mss がインストールされていません: pip install mss")
        with mss.mss() as sct:
            monitor_index = p.get("monitor", 1)
            monitor = sct.monitors[monitor_index]
            img = sct.grab(monitor)
            png_bytes = mss.tools.to_png(img.rgb, img.size)
        b64 = base64.b64encode(png_bytes).decode()
        return {"image_base64": b64, "width": img.width, "height": img.height}

    elif t == "toast_notify":
        title = p.get("title", agent.get("name", "ELVIN")).replace('"', '')
        message = p.get("message", "").replace('"', '')
        if platform.system() == "Windows":
            ps = (
                f'[void][Windows.UI.Notifications.ToastNotificationManager,'
                f'Windows.UI.Notifications,ContentType=WindowsRuntime];'
                f'$t=[Windows.UI.Notifications.ToastTemplateType]::ToastText02;'
                f'$x=[Windows.UI.Notifications.ToastNotificationManager]::GetTemplateContent($t);'
                f'$x.GetElementsByTagName("text")[0].AppendChild($x.CreateTextNode("{title}"));'
                f'$x.GetElementsByTagName("text")[1].AppendChild($x.CreateTextNode("{message}"));'
                f'$n=[Windows.UI.Notifications.ToastNotification]::new($x);'
                f'[Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier("ELVIN").Show($n);'
            )
            subprocess.Popen(["powershell", "-Command", ps])
        return {"notified": True, "platform": platform.system()}

    elif t == "system_info":
        return {
            "platform": platform.system(),
            "version": platform.version(),
            "machine": platform.machine(),
            "hostname": platform.node(),
            "python": sys.version,
        }

    elif t == "ELVIN_task":
        prompt = p.get("prompt", "")
        if not prompt:
            return {"output": "", "error": "no prompt"}

        # ウルバン向け: 管理キーワード検出時は専用プロンプトに差し替え
        _MGMT_KW = [
            "AIを追加", "エージェントを追加", "担当AIを追加",
            "AIを削除", "エージェントを削除", "担当AIを削除",
            "AIを変更", "エージェントを変更", "AIのプロンプト",
            "AI一覧", "エージェント一覧", "担当AI一覧",
        ]
        _is_dispatcher = ("DISPATCH:" in agent.get("system_prompt", "")
                          or "振り分け" in agent.get("system_prompt", ""))
        _prompt_stripped = prompt.replace("「", "").replace("」", "")
        if _is_dispatcher and any(kw in _prompt_stripped for kw in _MGMT_KW):
            full_prompt = (
                "エージェント管理コマンドを1行だけ出力してください。説明不要。\n\n"
                "形式:\n"
                'ADMIN:{"action":"add","name":"AI名","role":"役割","system_prompt":"業務指示"}\n'
                'ADMIN:{"action":"update","name":"既存AI名","system_prompt":"新業務指示"}\n'
                'ADMIN:{"action":"remove","name":"AI名"}\n'
                'ADMIN:{"action":"list"}\n\n'
                f"指示: {prompt}"
            )
        else:
            system_prompt = agent.get("system_prompt", "")
            if _is_dispatcher:
                # 担当AI一覧を現在のAGENTSから動的生成（追加・削除に自動追従）
                self_id = agent.get("agent_id", "")
                others = [a for a in AGENTS if a.get("agent_id") != self_id]
                agent_lines = "\n".join(
                    f"- {a['name']}（{a.get('role') or '汎用'}）"
                    for a in others
                )
                full_prompt = (
                    f"{system_prompt}\n\n"
                    f"【現在の担当AI一覧（最新）】以下のAIのみDISPATCH可能:\n{agent_lines}\n\n"
                    f"---\n\n{prompt}"
                )
            elif system_prompt:
                _req_id_hint = p.get("requester_id", "")
                room_name_hint = _resolve_room(prompt, _req_id_hint, WORK_DIR)
                _desktop = str(Path(WORK_DIR).parent)
                local_hint = (
                    f"\n\n【作業環境】\n"
                    f"・スクリプトの置き場所（作業基点）: {WORK_DIR}\n"
                    f"・Windowsデスクトップのパス: {_desktop}\n"
                    f"・「デスクトップ」と指定された場合は必ず {_desktop} 配下のローカルフォルダに保存する（GoogleドライブなどクラウドストレージへのアップロードはNG）\n"
                    f"・作業完了後は「何を・どこに・どんな名前で作ったか」を改行なしの1行テキストで出力すること"
                )
                room_prefix = f"返信先LINE WORKSルーム名: {room_name_hint}\n\n" if room_name_hint else ""
                full_prompt = f"{system_prompt}{local_hint}\n\n---\n\n{room_prefix}{prompt}"
            else:
                full_prompt = prompt

        work_dir = WORK_DIR
        # ディスパッチャーはCLAUDE.mdを読ませないためホームで実行
        claude_cwd = str(Path.home()) if _is_dispatcher else work_dir
        result = subprocess.run(
            ["claude", "--print", "--dangerously-skip-permissions", "-p", full_prompt],
            capture_output=True,
            text=True,
            timeout=600,
            cwd=claude_cwd,
            encoding="utf-8",
            errors="replace",
        )
        if platform.system() == "Windows":
            os.system("title ELVIN")
        output = result.stdout.strip()[:3000] if result.stdout else ""

        # DISPATCH: パターン検出 → 別エージェントに委託してリターン
        requester_id = p.get("requester_id", "")
        if output.startswith("DISPATCH:"):
            parts = output.split(":", 2)
            if len(parts) >= 3:
                target_name = parts[1].strip()
                task_content = parts[2].strip()
                target_agent = next((a for a in AGENTS if a["name"] == target_name), None)
                if target_agent:
                    try:
                        # 返信先ルーム名を委託先にも引き継ぐ
                        dispatch_room = _resolve_room(prompt, requester_id, work_dir)
                        room_prefix_d = f"返信先LINE WORKSルーム名: {dispatch_room}\n\n" if dispatch_room else ""
                        requests.post(
                            f"{VPS_URL}/api/v1/tasks/delegate",
                            headers=HEADERS,
                            json={
                                "agent_id": target_agent["agent_id"],
                                "type": "ELVIN_task",
                                "payload": {
                                    "prompt": f"{room_prefix_d}{task_content}",
                                    "requester_id": requester_id,
                                    "requester_name": p.get("requester_name", ""),
                                },
                            },
                            timeout=10,
                        )
                        print(f"[{ts()}] [{agent.get('name','URVAN')}] DISPATCH → {target_name}")
                        vlog(f"[{agent.get('name','URVAN')}] DISPATCH → {target_name}", agent_id=agent.get("agent_id"))
                    except Exception as e:
                        print(f"[{ts()}] DISPATCH失敗: {e}")
                        vlog(f"DISPATCH失敗: {e}", level="error", agent_id=agent.get("agent_id"))
                else:
                    print(f"[{ts()}] DISPATCHターゲット不明: {target_name!r}")
                    vlog(f"DISPATCHターゲット不明: {target_name!r}", level="warn", agent_id=agent.get("agent_id"))
            return {
                "output": output,
                "error": result.stderr.strip()[:500] if result.stderr else "",
                "exit_code": result.returncode,
            }

        # ADMIN: パターン検出 → エージェント管理
        if output.startswith("ADMIN:"):
            cmd_str = output[6:].strip()
            result_msg = handle_admin(cmd_str, requester_id, work_dir)
            print(f"[{ts()}] [{agent.get('name','URVAN')}] ADMIN: {result_msg[:80]}")
            room_name = _resolve_room(prompt, requester_id, work_dir) or None
            if room_name:
                ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")
                tmp = Path(work_dir) / f"管理報告_{ts_str}.txt"
                try:
                    tmp.write_text(f"【管理】\n{result_msg}", encoding="utf-8")
                    subprocess.run(
                        ["python", str(Path(work_dir) / "lineworks_send.py"),
                         room_name, str(tmp), "--file", "--headless"],
                        timeout=60, cwd=work_dir, capture_output=True,
                        encoding="utf-8", errors="replace",
                    )
                except Exception as e:
                    print(f"[{ts()}] ADMIN通知失敗: {e}")
                finally:
                    tmp.unlink(missing_ok=True)
            return {
                "output": output,
                "error": result.stderr.strip()[:500] if result.stderr else "",
                "exit_code": result.returncode,
            }

        # 完了後に LINE WORKS ルームへ通知（テキスト1行）
        if output:
            room_name = _resolve_room(prompt, requester_id, work_dir) or None
            if room_name:
                ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")
                a_name = re.sub(r'[\\/:*?"<>|]', '', agent.get("name", "ELVIN"))
                tmp = Path(work_dir) / f"完了報告_{a_name}_{ts_str}.txt"
                dept = agent.get("role", "")
                name = agent.get("name", "ELVIN")
                label = f"{dept} {name}".strip() if dept else name
                # 改行を全角スペースで置換して1行メッセージにする
                summary = output.replace("\r\n", "　").replace("\r", "　").replace("\n", "　").strip()
                # Claudeの出力が既に「完了:」で始まる場合は二重にしない
                if summary.startswith("完了:") or summary.startswith("完了："):
                    notify_body = f"【{label}】{summary}"
                else:
                    notify_body = f"【{label}】完了: {summary}"
                try:
                    tmp.write_text(notify_body, encoding="utf-8")
                    lw_result = subprocess.run(
                        ["python", str(Path(work_dir) / "lineworks_send.py"),
                         room_name, str(tmp), "--headless"],
                        timeout=120,
                        cwd=work_dir,
                        capture_output=True,
                        text=True,
                        encoding="utf-8",
                        errors="replace",
                    )
                    out = lw_result.stdout.strip()[:500] if lw_result.stdout else ""
                    if lw_result.returncode != 0:
                        print(f"[{ts()}] LINE WORKS通知失敗 (exit={lw_result.returncode}): {out}")
                        vlog(f"LINE WORKS通知失敗 (exit={lw_result.returncode}): {out}", level="error", agent_id=agent.get("agent_id"))
                    else:
                        print(f"[{ts()}] LINE WORKS通知OK（テキスト）: {room_name}")
                        vlog(f"LINE WORKS通知OK: {room_name}", agent_id=agent.get("agent_id"))
                except Exception as e:
                    print(f"[{ts()}] LINE WORKS通知失敗: {e}")
                    vlog(f"LINE WORKS通知失敗: {e}", level="error", agent_id=agent.get("agent_id"))
                finally:
                    tmp.unlink(missing_ok=True)

        return {
            "output": output,
            "error": result.stderr.strip()[:500] if result.stderr else "",
            "exit_code": result.returncode,
        }

    elif t == "lineworks_send":
        room_name = p.get("room_name", "") or LINEWORKS_ROOM
        message = p.get("message", "")
        if not room_name or not message:
            raise ValueError("room_name と message が必要です")
        work_dir = WORK_DIR
        import uuid as _uuid
        tmp = Path(work_dir) / f"_lw_send_{_uuid.uuid4().hex[:8]}.txt"
        try:
            tmp.write_text(message, encoding="utf-8")
            result = subprocess.run(
                ["python", str(Path(work_dir) / "lineworks_send.py"),
                 room_name, str(tmp), "--headless"],
                timeout=120, cwd=work_dir, capture_output=True,
                text=True, encoding="utf-8", errors="replace",
            )
            success = result.returncode == 0
            out = (result.stdout or "").strip()[:200]
            if not success:
                print(f"[{ts()}] lineworks_send 失敗 (exit={result.returncode}): {out}")
            return {"sent": success, "room": room_name, "exit_code": result.returncode}
        finally:
            tmp.unlink(missing_ok=True)

    elif t == "line_message":
        text = p.get("text", "")
        if not text:
            return {"output": ""}
        system_prompt = agent.get("system_prompt", "")
        full_prompt = f"{system_prompt}\n\n---\n\n{text}" if system_prompt else text
        result = subprocess.run(
            ["claude", "--print", "--dangerously-skip-permissions", "-p", full_prompt],
            capture_output=True, text=True, timeout=300,
            cwd=str(Path.home()), encoding="utf-8", errors="replace",
        )
        if platform.system() == "Windows":
            os.system("title ELVIN")
        output = result.stdout.strip()[:2000] if result.stdout else ""
        lm_req_id = p.get("requester_id", "")
        lm_room = _resolve_room(p.get("text", ""), lm_req_id, WORK_DIR) or None
        if output and lm_room:
            import uuid as _uuid
            tmp = Path(WORK_DIR) / f"_lw_line_{_uuid.uuid4().hex[:8]}.txt"
            try:
                tmp.write_text(output, encoding="utf-8")
                subprocess.run(
                    ["python", str(Path(WORK_DIR) / "lineworks_send.py"),
                     lm_room, str(tmp), "--headless"],
                    timeout=120, cwd=WORK_DIR, capture_output=True,
                    encoding="utf-8", errors="replace",
                )
            except Exception as e:
                print(f"[{ts()}] LINE WORKS通知失敗: {e}")
            finally:
                tmp.unlink(missing_ok=True)
        return {
            "output": output,
            "error": result.stderr.strip()[:500] if result.stderr else "",
            "exit_code": result.returncode,
        }

    else:
        raise ValueError(f"未対応のタスクタイプ: {t!r}")


# ── エージェント別ポーリング ──────────────────────────────────────────────

def poll_agent(agent: dict):
    agent_id = agent["agent_id"]
    agent_name = agent["name"]

    try:
        resp = requests.get(
            f"{VPS_URL}/api/v1/tasks/next",
            headers=HEADERS,
            params={"agent_id": agent_id},
            timeout=10,
        )
    except requests.RequestException as e:
        print(f"[{ts()}] [{agent_name}] VPS接続エラー: {e}")
        return

    if resp.status_code == 401:
        print(f"[{ts()}] 認証エラー: トークンを確認してください")
        return
    if resp.status_code != 200:
        print(f"[{ts()}] [{agent_name}] ポーリング失敗: HTTP {resp.status_code}")
        return

    task = resp.json().get("task")
    if not task:
        return

    task_id = task["id"]
    task_type = task["type"]
    payload = task.get("payload", {})

    if task_type == "ELVIN_task":
        sender = payload.get("requester_name", "不明")
        preview = payload.get("prompt", "")[:40].replace("\n", " ")
        print(f"[{ts()}] [{agent_name}] タスク受信: {task_type} [{sender}] 「{preview}...」")
        vlog(f"[{agent_name}] タスク受信 [{sender}] 「{preview}」", agent_id=agent_id)
    else:
        print(f"[{ts()}] [{agent_name}] タスク受信: {task_type} (id: {task_id[:8]}...)")
        vlog(f"[{agent_name}] タスク受信: {task_type}", agent_id=agent_id)

    try:
        result = execute(task, agent)
        requests.post(
            f"{VPS_URL}/api/v1/tasks/{task_id}/complete",
            headers=HEADERS,
            json={"success": True, "result": result},
            timeout=15,
        )
        print(f"[{ts()}] [{agent_name}] 完了: {task_type}")
        vlog(f"[{agent_name}] 完了: {task_type}", agent_id=agent_id)
    except Exception as e:
        error_msg = str(e)
        print(f"[{ts()}] [{agent_name}] 失敗: {task_type} — {error_msg}")
        vlog(f"[{agent_name}] 失敗: {task_type} — {error_msg}", level="error", agent_id=agent_id)
        try:
            requests.post(
                f"{VPS_URL}/api/v1/tasks/{task_id}/complete",
                headers=HEADERS,
                json={"success": False, "error": error_msg},
                timeout=10,
            )
        except Exception:
            pass


# ── 起動・メインループ ────────────────────────────────────────────────────

def main():
    global AGENTS, CLIENT_ID

    # ターミナルタイトルを変更（Claudeと知られないよう）
    if platform.system() == "Windows":
        os.system("title ELVIN")
    else:
        print("\033]0;ELVIN\007", end="", flush=True)

    print("=" * 50)
    print("  ELVIN ローカルエージェント")
    print(f"  VPS: {VPS_URL}")
    print(f"  ポーリング間隔: {POLL_INTERVAL}秒")
    print("=" * 50)

    # 接続確認
    try:
        resp = requests.post(
            f"{VPS_URL}/api/v1/heartbeat",
            headers=HEADERS,
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            CLIENT_ID = data.get("client_id", "")
            print(f"[{ts()}] 接続OK: client_id={CLIENT_ID}")
        else:
            print(f"[{ts()}] ハートビート失敗: HTTP {resp.status_code}")
    except requests.RequestException as e:
        print(f"[{ts()}] VPSに接続できません: {e}")

    # エージェント一覧を取得
    try:
        resp = requests.get(
            f"{VPS_URL}/api/v1/client/agents",
            headers=HEADERS,
            timeout=10,
        )
        if resp.status_code == 200:
            AGENTS = resp.json()
            if AGENTS:
                print(f"[{ts()}] エージェント ({len(AGENTS)}体):")
                for ag in AGENTS:
                    tools = ", ".join(t["tool"] for t in ag.get("tools", [])) or "なし"
                    role = ag.get("role") or "汎用"
                    print(f"         ・{ag['name']} ({role}) — ツール: {tools}")
            else:
                print(f"[{ts()}] エージェント未設定（VPSで登録してください）")
        else:
            print(f"[{ts()}] エージェント取得失敗: HTTP {resp.status_code}")
    except Exception as e:
        print(f"[{ts()}] エージェント取得エラー: {e}")

    print(f"[{ts()}] ポーリング開始...")
    while True:
        if AGENTS:
            for agent in AGENTS:
                poll_agent(agent)
        else:
            # エージェント未設定の場合は定期的に再取得を試みる
            try:
                resp = requests.get(
                    f"{VPS_URL}/api/v1/client/agents",
                    headers=HEADERS,
                    timeout=10,
                )
                if resp.status_code == 200:
                    AGENTS = resp.json()
                    if AGENTS:
                        print(f"[{ts()}] エージェントを検出: {len(AGENTS)}体")
            except Exception:
                pass
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
