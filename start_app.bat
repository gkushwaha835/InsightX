@echo off
setlocal

set "BASE_DIR=%~dp0"
cd /d "%BASE_DIR%"

set "PYTHONHOME=C:\Program Files\MySQL\MySQL Shell 8.0\lib\Python3.13"
set "PATH=C:\Program Files\MySQL\MySQL Shell 8.0\bin;%PATH%"
set "PYTHONPATH=%BASE_DIR%.venv\Lib\site-packages"
set "FLASK_ENV=development"
set "FLASK_DEBUG=1"
set "PORT=5000"
set "PYTHON_EXE=C:\Program Files\MySQL\MySQL Shell 8.0\lib\Python3.13\Lib\venv\scripts\nt\python.exe"

if not exist "%PYTHON_EXE%" (
  echo [ERROR] Python executable not found:
  echo         %PYTHON_EXE%
  pause
  exit /b 1
)

if not exist "%BASE_DIR%run.py" (
  echo [ERROR] run.py not found in:
  echo         %BASE_DIR%
  pause
  exit /b 1
)

echo Starting InsightX app on http://127.0.0.1:5000 ...
"%PYTHON_EXE%" -u run.py

endlocal

