@echo off
setlocal EnableDelayedExpansion

set "FOUND=0"
set "KILLED=0"
for /f "tokens=5" %%P in ('netstat -aon ^| findstr LISTENING ^| findstr :5000') do (
  set "FOUND=1"
  echo Stopping PID %%P ...
  taskkill /PID %%P /F >nul 2>&1
  if !errorlevel! EQU 0 (
    set "KILLED=1"
  ) else (
    echo Could not stop PID %%P - already exited or permission denied.
  )
)

if "%FOUND%"=="0" (
  echo No app process found on port 5000.
) else (
  if "%KILLED%"=="1" (
    echo App stopped.
  ) else (
    echo Stop command ran, but no process was terminated.
  )
)

endlocal
