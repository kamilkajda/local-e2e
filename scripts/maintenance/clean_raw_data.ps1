# Script to cleanly remove all raw data downloaded from API.
# Use this when changing metric names in config to avoid stale folders.

$projectRoot = Resolve-Path "$PSScriptRoot\..\.."
$rawPath = Join-Path $projectRoot "data\raw"

Write-Host "--- RAW DATA CLEANUP ---" -ForegroundColor Cyan

if (Test-Path $rawPath) {
    Write-Host "Cleaning directory: $rawPath" -ForegroundColor Yellow
    
    # Remove all items inside data/raw
    Get-ChildItem -Path $rawPath -Recurse | Remove-Item -Recurse -Force
    
    # Restore .gitkeep to maintain git structure
    $gitKeepPath = Join-Path $rawPath ".gitkeep"
    New-Item -ItemType File -Force -Path $gitKeepPath | Out-Null
    
    Write-Host "   [OK] Raw data removed. Directory structure preserved." -ForegroundColor Green
} else {
    Write-Host "   [INFO] Directory not found." -ForegroundColor Gray
}