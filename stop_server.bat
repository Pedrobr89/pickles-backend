@echo off
setlocal
echo Buscando processos do servidor...

REM Stop python processes running backend/app.py
for /f "tokens=*" %%P in ('powershell -NoProfile -ExecutionPolicy Bypass -Command "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'backend.server.py' -and $_.Name -eq 'python.exe' } | Select-Object -ExpandProperty ProcessId"') do (
  echo Encerrando processo PID %%P
  powershell -NoProfile -ExecutionPolicy Bypass -Command "Stop-Process -Id %%P -Force"
)

echo Concluido.
endlocal
