@echo off
setlocal
set TASK_NAME=MarmBot

echo Starting scheduled task "%TASK_NAME%"...
schtasks /run /tn "%TASK_NAME%"
if %ERRORLEVEL% NEQ 0 (
  echo Failed to start task "%TASK_NAME%". Ensure it is installed.
  exit /b %ERRORLEVEL%
)
echo Started.

