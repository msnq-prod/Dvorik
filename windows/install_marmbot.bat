@echo off
setlocal
rem Install MarmBot Scheduled Task and setup venv/deps
powershell -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0install_marmbot.ps1"
if %ERRORLEVEL% NEQ 0 (
  echo Installation failed. See messages above.
  pause
  exit /b %ERRORLEVEL%
)
echo.
echo MarmBot task installed. You can start it now with:
echo   %~dp0start_now.bat
pause

