# run_etl_dev.ps1
# Wrapper for running the ETL code on the 'dev' environment

# FIX: Get Project Root first to avoid Resolve-Path errors on non-existent subfolders (like logs)
$projectRoot = (Resolve-Path "$PSScriptRoot\..").Path

# Construct paths using Join-Path (safe string manipulation)
$configPath = Join-Path $projectRoot "configs\dev\settings.json"
$scriptPath = Join-Path $projectRoot "src\pyspark\main.py"
$logDir = Join-Path $projectRoot "logs"
$vscodeSettingsPath = Join-Path $projectRoot ".vscode\settings.json"

# Ensure log directory exists
if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
}

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logFile = Join-Path $logDir "etl_dev_$timestamp.log"

# --- SMART PYTHON DETECTION ---
$pythonCmd = "python" # Default fallback
$useCondaRun = $false

if (Test-Path $vscodeSettingsPath) {
    try {
        $json = Get-Content $vscodeSettingsPath -Raw | ConvertFrom-Json
        if ($json.'python.defaultInterpreterPath') {
            $pythonCmd = $json.'python.defaultInterpreterPath'
            Write-Host ">>> Auto-detected Python from VS Code settings: $pythonCmd" -ForegroundColor Cyan
        }
    } catch {
        Write-Host ">>> Warning: Could not parse VS Code settings." -ForegroundColor Yellow
    }
} 

if ($pythonCmd -eq "python") {
    if (Get-Command "conda" -ErrorAction SilentlyContinue) {
        Write-Host ">>> VS Code settings not found. Using 'conda run -n gus_etl'..." -ForegroundColor Cyan
        $useCondaRun = $true
    } else {
        Write-Host ">>> Warning: Neither VS Code settings nor Conda found. Using system default 'python'." -ForegroundColor Yellow
    }
}
# ------------------------------

Write-Host ">>> Starting ETL (DEV)..." -ForegroundColor Cyan
Write-Host ">>> Log file: $logFile" -ForegroundColor Gray
Write-Host ">>> (Console output hidden for performance. Running in background...)" -ForegroundColor Yellow

# Construct the command line
if ($useCondaRun) {
    $cmdLine = "conda run -n gus_etl python ""$scriptPath"" --config ""$configPath"" > ""$logFile"" 2>&1"
} else {
    $cmdLine = """$pythonCmd"" ""$scriptPath"" --config ""$configPath"" > ""$logFile"" 2>&1"
}

# Execute via cmd /c
cmd /c $cmdLine

if ($LASTEXITCODE -ne 0) {
    Write-Host "`n--- LAST LOG LINES (ERROR) ---" -ForegroundColor Red
    if (Test-Path $logFile) {
        Get-Content $logFile -Tail 20
    } else {
        Write-Host "Log file was not created due to a critical error launching the command."
    }
    Write-Error "ERROR: ETL process (DEV) returned exit code $LASTEXITCODE."
    exit 1
} else {
    Write-Host "`n--- LAST LOG LINES (SUCCESS) ---" -ForegroundColor Gray
    if (Test-Path $logFile) {
        Get-Content $logFile -Tail 5
    }
    Write-Host ">>> ETL (DEV) completed successfully." -ForegroundColor Green
    # Log deletion removed for safety/debugging purposes
}