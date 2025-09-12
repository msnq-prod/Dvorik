@echo off
setlocal
rem Uninstall MarmBot Scheduled Task
powershell -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0uninstall_marmbot.ps1"
pause

