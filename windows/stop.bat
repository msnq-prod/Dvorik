@echo off
setlocal
set TASK_NAME=MarmBot

echo Stopping scheduled task "%TASK_NAME%" (if running)...
schtasks /end /tn "%TASK_NAME%"
exit /b 0

