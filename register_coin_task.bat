@echo off
echo 코인 스냅샷 작업 스케줄러 등록 중...

schtasks /create ^
  /tn "CoinSnapshot_0900" ^
  /tr "\"c:\Claude\Possession\run_coin_snapshot.bat\"" ^
  /sc daily ^
  /st 09:00 ^
  /f ^
  /rl highest ^
  /ru "%USERNAME%"

if %errorlevel% == 0 (
    echo.
    echo [성공] 작업 스케줄러에 등록되었습니다.
    echo 작업명: CoinSnapshot_0900
    echo 실행시간: 매일 09:00
    echo 스크립트: c:\Claude\Possession\run_coin_snapshot.bat
) else (
    echo.
    echo [실패] 관리자 권한으로 다시 실행해 주세요.
    echo 이 파일을 마우스 오른쪽 클릭 - 관리자로 실행
)
pause
