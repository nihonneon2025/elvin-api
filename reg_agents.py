import urllib.request, json

BASE = "http://localhost:5050"
SECRET = "braintrust2026"
CLIENT = "ago_001"

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
        "担当AI一覧:\n"
        "- 受注管理AI: 新規案件受付・受注確定・施工スケジュール調整\n"
        "- 図面・内装AI: 図面起こし・内装設計\n"
        "- 積算見積AI: 資材拾い出し・原価計算・見積作成\n"
        "- 発注管理AI: クライアント向け見積書・メーカー/中国業者への発注\n"
        "- 経理AI: 請求書作成・経費計算・収支管理\n\n"
        "指示を受けたら適切な担当AIを選び、必ず以下の形式だけで返答してください:\n"
        "DISPATCH:担当AI名:担当AIへの詳細な指示内容\n\n"
        "自分では作業を行わず、必ずDISPATCH形式で振り分けてください。"
    )
})
print("URVAN更新:", res)

agents = [
    {
        "agent_id": "ago_001_juchu",
        "name": "受注管理AI",
        "role": "総務部 受注管理課",
        "system_prompt": (
            "あなたはAGOの総務部受注管理課のAIです。"
            "新規案件の受付・登録・社内案件化、受注確定後の案件管理、"
            "施工スケジュール調整・外注業者の手配を担当しています。"
            "依頼内容を確認し、必要な情報を整理して報告してください。"
        ),
    },
    {
        "agent_id": "ago_001_zumen",
        "name": "図面・内装AI",
        "role": "デザイン部",
        "system_prompt": (
            "あなたはAGOのデザイン部のAIです。"
            "案件の種別に応じて図面起こしおよび内装設計を担当しています。"
            "依頼内容をもとに設計方針や必要な情報を整理して報告してください。"
        ),
    },
    {
        "agent_id": "ago_001_sekisan",
        "name": "積算見積AI",
        "role": "デザイン部 積算見積担当",
        "system_prompt": (
            "あなたはAGOのデザイン部積算見積担当のAIです。"
            "図面をもとに資材の拾い出し・原価計算・見積作成を担当しています。"
            "依頼された案件の積算情報を整理して報告してください。"
        ),
    },
    {
        "agent_id": "ago_001_hacchu",
        "name": "発注管理AI",
        "role": "総務部 発注管理課",
        "system_prompt": (
            "あなたはAGOの総務部発注管理課のAIです。"
            "人件費・利益を乗せたクライアント向け見積書の作成、"
            "国内メーカー・中国業者への発注・やりとりを担当しています。"
            "依頼内容を確認し必要な対応を報告してください。"
        ),
    },
    {
        "agent_id": "ago_001_keiri",
        "name": "経理AI",
        "role": "経理部",
        "system_prompt": (
            "あなたはAGOの経理部のAIです。"
            "完工後の請求書作成・送付、請求済み案件の書類整理・"
            "経費計算・収支管理を担当しています。"
            "依頼内容を確認し必要な経理処理を報告してください。"
        ),
    },
]

for ag in agents:
    res = api("POST", f"/api/v1/clients/{CLIENT}/agents", ag)
    print(f"{ag['name']} 登録:", res)

for aid in ["ago_001_juchu", "ago_001_zumen", "ago_001_sekisan", "ago_001_hacchu", "ago_001_keiri"]:
    res = api("POST", f"/api/v1/agents/{aid}/tools", {"tool_name": "ELVIN_task"})
    print(f"{aid} ツール:", res)

print("完了")
