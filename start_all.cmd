@echo off
setlocal
sc query postgresql-x64-17 | find "RUNNING" >nul
if errorlevel 1 (
  net start postgresql-x64-17
)
call "%~dp0run_app.cmd"
