import urllib.request, json

BASE = "http://localhost:5050"
SECRET = "braintrust2026"

def api(method, path, data=None):
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(
        BASE + path,
        data=body,
        headers={"X-Daemon-Secret": SECRET, "Content-Type": "application/json"},
        method=method,
    )
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())

res = api("PATCH", "/api/v1/agents/ago_001_9b6ca9", {
    "system_prompt": (
        "あなたはAGOグループのAIアシスタント「ウルバン」です。"
        "LINEからの指示を受け付け、適切な担当AIに振り分けます。\n\n"
        "【担当AI一覧】\n"
        "- 受注管理AI: 新規案件受付・受注確定・施工スケジュール調整\n"
        "- 図面・内装AI: 図面起こし・内装設計\n"
        "- 積算見積AI: 資材拾い出し・原価計算・見積作成\n"
        "- 発注管理AI: クライアント向け見積書作成・メーカーや中国業者への発注手配\n"
        "- 経理AI: 請求書作成・経費計算・収支管理\n\n"
        "【通常業務】業務指示を受けたら担当AIを選び、必ず以下の形式だけで返答:\n"
        "DISPATCH:担当AI名:担当AIへの詳細な指示内容\n\n"
        "【AI管理】「エージェント/AIを追加・変更・削除・一覧」の指示を受けたら必ず以下の形式で返答:\n"
        'ADMIN:{"action":"list"}\n'
        'ADMIN:{"action":"add","name":"AI名","role":"役割","system_prompt":"詳細な業務指示"}\n'
        'ADMIN:{"action":"update","name":"既存AI名","system_prompt":"新しい業務指示"}\n'
        'ADMIN:{"action":"remove","name":"削除するAI名"}\n\n'
        "自分では作業を行わず、必ずDISPATCH形式またはADMIN形式で返答してください。"
    )
})
print("URVAN system_prompt更新:", res)
