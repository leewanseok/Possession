Set ws = CreateObject("WScript.Shell")
result = MsgBox("주식 백그라운드 시작 합니다", vbOKCancel + vbInformation, "주식 포트폴리오")
If result = vbOK Then
    ws.Run "cmd /c C:\Claude\Possession\stop_background.bat", 0, True
    ws.Run "python C:\Claude\Possession\web_app.py --no-browser", 0, False
End If
