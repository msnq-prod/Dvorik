param(
    [string]$TaskName = "MarmBot"
)

$ErrorActionPreference = 'Stop'

function Write-Info($msg) { Write-Host "[INFO] $msg" -ForegroundColor Cyan }
function Write-Ok($msg)   { Write-Host "[ OK ] $msg" -ForegroundColor Green }
function Write-Warn($msg) { Write-Host "[WARN] $msg" -ForegroundColor Yellow }

Write-Info "Removing Scheduled Task '$TaskName' if present"
try {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction Stop | Out-Null
    Write-Ok "Removed task '$TaskName'"
} catch {
    Write-Warn "Task '$TaskName' not found or already removed"
}

