param(
[string]$AzuriteDataPath = "C:\azurite",
[int]$BlobPort = 10000,
[int]$QueuePort = 10001,
[int]$TablePort = 10002
)

$ErrorActionPreference = "Stop"
$log = "..\logs\start_all_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"

# Ensure log directory exists
New-Item -ItemType Directory -Force -Path (Split-Path $log) | Out-Null

function Write-Log($msg) {
    # Use ToString() on DateTime for reliable formatting
    $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss.fff")
    "$ts :: $msg" | Tee-Object -FilePath $log -Append
}

Write-Log "== START ALL SERVICES =="

# 1) Environment diagnostics: Node & Azurite
Write-Log "Environment diagnostics..."
try {
    $nodev = node -v
    Write-Log "Node: $nodev"
    $npmm = npm -v
    Write-Log "npm: $npmm"
} catch { Write-Log "ERROR: Node/npm not found. Install Node.js LTS"; exit 1 }
try {
    $azv = (azurite --version | Select-Object -First 1).Trim()
    Write-Log "Azurite: $azv"
} catch { Write-Log "ERROR: Azurite not found. Run: npm install -g azurite"; exit 1 }


# 2) Java check (required by Spark) - OMITTED, relying on PySpark to confirm Java's availability.

# 3) Python check (for Spark)
try {
    $pyv = python -V
    Write-Log "Python: $pyv"
} catch { Write-Log "ERROR: Python not found. Check PATH/Conda"; exit 1 }

Write-Log "--- End of diagnostics ---"

# 4) Start Azurite in background with explicit arguments
New-Item -ItemType Directory -Force -Path $AzuriteDataPath | Out-Null
Write-Log "Starting Azurite (data: $AzuriteDataPath)..."

# FIX: Use cmd /c to reliably run the node-based Azurite executable 
# ADDED: --blobHost 0.0.0.0 to allow LAN connections from other computers
$azuriteArgs = "azurite -s -l `"$AzuriteDataPath`" --blobPort $BlobPort --queuePort $QueuePort --tablePort $TablePort --blobHost 0.0.0.0 --queueHost 0.0.0.0 --tableHost 0.0.0.0"

Start-Process -FilePath "cmd" `
    -ArgumentList "/c", $azuriteArgs `
    -NoNewWindow -PassThru | Out-Null

Write-Log "Azurite started (LAN access enabled). Waiting 3 seconds..."
Start-Sleep -Seconds 3

# 5) Simple health-check: check Blob port
try {
    # Using Invoke-WebRequest
    # FIX: Changed -TimeoutSeconds to -TimeoutSec for compatibility
    $resp = Invoke-WebRequest -Uri "http://127.0.0.1:$BlobPort/" -UseBasicParsing -Method GET -TimeoutSec 5
    Write-Log "Azurite reachable on :$BlobPort (status: $($resp.StatusCode))"
} catch { 
    # Status code 400 (Bad Request) is expected for GET on Azurite root
    if ($_.Exception.Response.StatusCode -eq 400) {
        Write-Log "Azurite reachable on :$BlobPort (status: 400 Bad Request - OK)"
    } else {
        Write-Log "WARNING: Health-check failed. Check if Azurite is running. ($($_.Exception.Message))"
    }
}

Write-Log "== ALL SERVICES STARTED =="