# Installs MarmBot as a Windows Scheduled Task with auto-restart and
# triggers on logon, workstation unlock, and resume from sleep.
# Also ensures a fresh venv and installs requirements.

param(
    [string]$TaskName = "MarmBot"
)

$ErrorActionPreference = 'Stop'

function Write-Info($msg) { Write-Host "[INFO] $msg" -ForegroundColor Cyan }
function Write-Ok($msg)   { Write-Host "[ OK ] $msg" -ForegroundColor Green }
function Write-Warn($msg) { Write-Host "[WARN] $msg" -ForegroundColor Yellow }
function Write-Err($msg)  { Write-Host "[ERR ] $msg" -ForegroundColor Red }

# Resolve repo root (this script is in windows\)
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot  = Split-Path -Parent $ScriptDir
Set-Location $RepoRoot

Write-Info "Repo: $RepoRoot"

# 1) Ensure Python and venv
function Get-PythonCmd() {
    try {
        $py = (& py -3 -c "import sys;print(sys.executable)" 2>$null)
        if ($LASTEXITCODE -eq 0 -and $py) { return 'py -3' }
    } catch { }
    try {
        $pyexe = (& python -c "import sys;print(sys.executable)" 2>$null)
        if ($LASTEXITCODE -eq 0 -and $pyexe) { return 'python' }
    } catch { }
    throw "Python 3 not found. Install Python from https://www.python.org/downloads/ (enable 'Add to PATH')."
}

$PythonSel = Get-PythonCmd
$VenvPython = Join-Path $RepoRoot 'venv\Scripts\python.exe'

if (-not (Test-Path $VenvPython)) {
    Write-Info "Creating virtualenv: venv"
    iex "$PythonSel -m venv `"$RepoRoot\venv`""
} else {
    # venv created on non-Windows is not compatible; recreate if no Scripts\python.exe
    if (-not (Test-Path $VenvPython)) {
        Write-Info "Recreating incompatible venv"
        Remove-Item -Recurse -Force "$RepoRoot\venv" -ErrorAction SilentlyContinue
        iex "$PythonSel -m venv `"$RepoRoot\venv`""
    }
}

if (-not (Test-Path $VenvPython)) {
    throw "Failed to create virtualenv at $RepoRoot\venv"
}
Write-Ok "Venv ready: $VenvPython"

Write-Info "Upgrading pip and installing requirements"
& $VenvPython -m pip install --upgrade pip wheel setuptools
& $VenvPython -m pip install -r (Join-Path $RepoRoot 'requirements.txt')
Write-Ok "Dependencies installed"

# 2) Prepare Scheduled Task
$PythonExe = $VenvPython
$BotScript = Join-Path $RepoRoot 'marm_bot.py'
if (-not (Test-Path $BotScript)) { throw "Entry file not found: $BotScript" }

$Action = New-ScheduledTaskAction -Execute $PythonExe -Argument "`"$BotScript`"" -WorkingDirectory $RepoRoot

$T_Logon   = New-ScheduledTaskTrigger -AtLogOn
$T_Unlock  = New-ScheduledTaskTrigger -OnWorkstationUnlock

# Resume from sleep trigger via Event: Microsoft-Windows-Power-Troubleshooter / Event ID 1
$ResumeQuery = @"
<QueryList>
  <Query Id="0" Path="System">
    <Select Path="System">*[System[Provider[@Name='Microsoft-Windows-Power-Troubleshooter'] and (EventID=1)]]</Select>
  </Query>
</QueryList>
"@
$T_Resume  = New-ScheduledTaskTrigger -OnEvent -Subscription $ResumeQuery

$Settings = New-ScheduledTaskSettingsSet \
    -StartWhenAvailable \
    -AllowStartIfOnBatteries \
    -DontStopIfGoingOnBatteries \
    -RestartCount 999 \
    -RestartInterval (New-TimeSpan -Seconds 10) \
    -MultipleInstances StopExisting

$CurrentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$Principal = New-ScheduledTaskPrincipal -UserId $CurrentUser -RunLevel Highest -LogonType Interactive

Write-Info "Registering Scheduled Task '$TaskName'"
try {
    # Remove old task if exists
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue | Out-Null
} catch { }

Register-ScheduledTask \
    -TaskName $TaskName \
    -Action $Action \
    -Trigger @($T_Logon, $T_Unlock, $T_Resume) \
    -Settings $Settings \
    -Principal $Principal \
    -Description "MarmBot: auto-start on logon/unlock/resume; auto-restart on failure" \
    | Out-Null

Write-Ok "Scheduled Task '$TaskName' installed"

try {
    Write-Info "Starting task now"
    Start-ScheduledTask -TaskName $TaskName
    Write-Ok "Task '$TaskName' started"
} catch {
    Write-Warn "Couldn't start task automatically. You can start it with: schtasks /run /tn `"$TaskName`""
}
