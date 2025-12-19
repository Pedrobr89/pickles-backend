@echo off
setlocal
cd /d "%~dp0"

REM Activate virtual environment if it exists in backend/.venv
if exist "backend\.venv\Scripts\activate.bat" (
  call "backend\.venv\Scripts\activate.bat"
) else (
  echo Aviso: Ambiente virtual nao encontrado em backend\.venv
)

set FLASK_ENV=development
set API_HOST=127.0.0.1
set API_PORT=5001
set DEBUG=True

echo Iniciando servidor em http://%API_HOST%:%API_PORT%
python backend/server.py
endlocal
