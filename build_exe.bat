@echo off
chcp 65001 > nul
echo [ELVIN] .exe ビルド開始

:: PyInstaller がなければインストール
pip show pyinstaller >nul 2>&1 || pip install pyinstaller

:: ビルド実行
pyinstaller --onefile --name elvin_agent --console agent_local.py

echo.
if exist dist\elvin_agent.exe (
    echo [OK] ビルド完了: dist\elvin_agent.exe
    echo.
    echo 配布パッケージに含めるファイル:
    echo   dist\elvin_agent.exe  ... 実行ファイル
    echo   elvin_config.json     ... 設定ファイル（顧客ごとに書き換え）
    echo.
    echo elvin_config.json のテンプレート:
    echo {
    echo   "vps_url": "https://api.nihon-neon.jp",
    echo   "client_token": "ここに顧客トークンを入力",
    echo   "poll_interval": 5
    echo }
) else (
    echo [ERROR] ビルド失敗。上のエラーを確認してください。
)
pause
