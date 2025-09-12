@echo off
setlocal
set TASK_NAME=MarmBot

echo Restarting scheduled task "%TASK_NAME%"...
schtasks /end /tn "%TASK_NAME%" >nul 2>&1
timeout /t 1 >nul
schtasks /run /tn "%TASK_NAME%"
if %ERRORLEVEL% NEQ 0 (
  echo Failed to start task "%TASK_NAME%". Ensure it is installed.
  exit /b %ERRORLEVEL%
)
echo Restarted.

