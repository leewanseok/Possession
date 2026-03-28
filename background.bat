@echo off
chcp 65001 > nul

:: 기존 포트 5000 프로세스 종료
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":5000 " ^| findstr "LISTENING" 2^>nul') do (
    taskkill /PID %%a /F >nul 2>&1
)
timeout /t 1 /nobreak > nul

:: 창 없이 완전 백그라운드 실행
wscript.exe "C:\Claude\Possession\run_hidden.vbs"

echo 백그라운드 서버 시작됨
echo 브라우저에서 http://localhost:5000 으로 접속 가능합니다
timeout /t 3 /nobreak > nul
