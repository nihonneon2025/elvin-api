"""
BrainTrust ローカルエージェント
日本ネオン株式会社 - 顧客PC常駐スクリプト

起動:
  set BRAINTRUST_VPS_URL=https://your-vps.com
  set BRAINTRUST_TOKEN=your_client_token
  python agent.py

環境変数:
  BRAINTRUST_VPS_URL   VPS APIのURL（デフォルト: http://localhost:5050）
  BRAINTRUST_TOKEN     顧客トークン（必須）
  POLL_INTERVAL        ポーリング間隔秒（デフォルト: 5）
"""

import base64
import json
import os
import platform
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

# ── 設定 ──────────────────────────────────────────────────────────────────
VPS_URL = os.environ.get("BRAINTRUST_VPS_URL", "http://localhost:5050").rstrip("/")
CLIENT_TOKEN = os.environ.get("BRAINTRUST_TOKEN", "")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "5"))

if not CLIENT_TOKEN:
    print("[ERROR] 環境変数 BRAINTRUST_TOKEN が設定されていません")
    sys.exit(1)

HEADERS = {"X-Client-Token": CLIENT_TOKEN}


def ts():
    return datetime.now().strftime("%H:%M:%S")


# ── タスク実行 ────────────────────────────────────────────────────────────

def execute(task: dict) -> dict:
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
        title = p.get("title", "BrainTrust").replace('"', '')
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
                f'[Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier("BrainTrust").Show($n);'
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

    elif t == "claude_task":
        prompt = p.get("prompt", "")
        if not prompt:
            return {"output": "", "error": "no prompt"}
        cwd = r"C:\Users\Administrator\Desktop\AI版AGO"
        work_dir = cwd if Path(cwd).exists() else str(Path.home())
        result = subprocess.run(
            ["claude", "--print", "--dangerously-skip-permissions", "-p", prompt],
            capture_output=True,
            text=True,
            timeout=600,
            cwd=work_dir,
            encoding="utf-8",
            errors="replace",
        )
        output = result.stdout.strip()[:3000] if result.stdout else ""

        # 完了後に LINE WORKS ルームへ通知
        requester_id = p.get("requester_id", "")
        if requester_id and output:
            room_map_path = Path(work_dir) / "lineworks-room-map.json"
            room_name = None
            if room_map_path.exists():
                try:
                    room_map = json.loads(room_map_path.read_text(encoding="utf-8"))
                    room_name = room_map.get(requester_id)
                except Exception:
                    pass
            if room_name:
                tmp = Path(work_dir) / f"_lw_notify_{requester_id[:8]}.txt"
                try:
                    tmp.write_text(output, encoding="utf-8")
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
                    else:
                        print(f"[{ts()}] LINE WORKS通知OK: {room_name}")
                except Exception as e:
                    print(f"[{ts()}] LINE WORKS通知失敗: {e}")
                finally:
                    tmp.unlink(missing_ok=True)

        return {
            "output": output,
            "error": result.stderr.strip()[:500] if result.stderr else "",
            "exit_code": result.returncode,
        }

    elif t == "line_message":
        text = p.get("text", "")
        return {"reply": f"[AGO PC] 受信しました: {text}"}

    else:
        raise ValueError(f"未対応のタスクタイプ: {t!r}")


# ── ポーリングループ ───────────────────────────────────────────────────────

def poll():
    try:
        resp = requests.get(
            f"{VPS_URL}/api/v1/tasks/next",
            headers=HEADERS,
            timeout=10,
        )
    except requests.RequestException as e:
        print(f"[{ts()}] VPS接続エラー: {e}")
        return

    if resp.status_code == 401:
        print(f"[{ts()}] 認証エラー: トークンを確認してください")
        return
    if resp.status_code != 200:
        print(f"[{ts()}] ポーリング失敗: HTTP {resp.status_code}")
        return

    task = resp.json().get("task")
    if not task:
        return

    task_id = task["id"]
    task_type = task["type"]
    payload = task.get("payload", {})
    if task_type == "claude_task":
        sender = payload.get("requester_name", "不明")
        preview = payload.get("prompt", "")[:40].replace("\n", " ")
        print(f"[{ts()}] タスク受信: {task_type} [{sender}] 「{preview}...」 (id: {task_id[:8]}...)")
    else:
        print(f"[{ts()}] タスク受信: {task_type} (id: {task_id[:8]}...)")

    try:
        result = execute(task)
        requests.post(
            f"{VPS_URL}/api/v1/tasks/{task_id}/complete",
            headers=HEADERS,
            json={"success": True, "result": result},
            timeout=15,
        )
        print(f"[{ts()}] 完了: {task_type}")
    except Exception as e:
        error_msg = str(e)
        print(f"[{ts()}] 失敗: {task_type} — {error_msg}")
        try:
            requests.post(
                f"{VPS_URL}/api/v1/tasks/{task_id}/complete",
                headers=HEADERS,
                json={"success": False, "error": error_msg},
                timeout=10,
            )
        except Exception:
            pass


def main():
    print("=" * 50)
    print("  BrainTrust ローカルエージェント")
    print(f"  VPS: {VPS_URL}")
    print(f"  ポーリング間隔: {POLL_INTERVAL}秒")
    print("=" * 50)

    # 初回ハートビートで接続確認
    try:
        resp = requests.post(
            f"{VPS_URL}/api/v1/heartbeat",
            headers=HEADERS,
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            print(f"[{ts()}] 接続OK: client_id={data.get('client_id')}")
        else:
            print(f"[{ts()}] ハートビート失敗: HTTP {resp.status_code}")
    except requests.RequestException as e:
        print(f"[{ts()}] VPSに接続できません: {e}")
        print("         VPSが起動しているか、URLを確認してください")

    print(f"[{ts()}] ポーリング開始...")
    while True:
        poll()
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
