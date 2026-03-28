@echo off
chcp 65001 > nul
echo.
echo  ╔══════════════════════════════════════╗
echo  ║   주식 포트폴리오 업데이트 중...     ║
echo  ╚══════════════════════════════════════╝
echo.
cd /d "%~dp0"
python -X utf8 stock_manager.py
if %errorlevel% neq 0 (
    echo.
    echo  오류가 발생했습니다. Python 및 패키지 설치를 확인하세요.
    echo  pip install -r requirements.txt
    pause
)
