$ErrorActionPreference = "SilentlyContinue"
$log = "..\logs\stop_all_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"

New-Item -ItemType Directory -Force -Path (Split-Path $log) | Out-Null

function Write-Log($msg) {
    $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss.fff")
    "$ts :: $msg" | Tee-Object -FilePath $log -Append
}

Write-Log "== STOP ALL SERVICES =="
Write-Log "Shutting down Azurite processes (node)..."

# Close Azurite windows: kill node processes with the "azurite" argument
Get-Process | Where-Object { $_.ProcessName -match "node" } | ForEach-Object {
    try {
        # Retrieve CommandLine to verify if it is indeed Azurite
        $cmdline = (Get-CimInstance Win32_Process -Filter "ProcessId=$($_.Id)").CommandLine
        
        if ($cmdline -match "azurite") {
            Write-Log ""Killing PID $($_.Id) : $cmdline""
            Stop-Process -Id $_.Id -Force
        }
    } catch {
        # Ignore errors if the process disappeared in the meantime
        Write-Log "Warning: Failed to check/kill node process with ID $($_.Id)."
    }
}

Write-Log "Node processes running Azurite closed."
Write-Log "== ALL SERVICES STOPPED =="