Set ws = CreateObject("WScript.Shell")
result = MsgBox("주식 백그라운드 종료 합니다", vbOKCancel + vbExclamation, "주식 포트폴리오")
If result = vbOK Then
    ws.Run "cmd /c C:\Claude\Possession\stop_background.bat", 0, True
End If
