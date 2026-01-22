# Script to initialize .gitkeep files in data and log directories.
# This ensures the directory structure is tracked by Git while keeping the actual data ignored.

$projectRoot = Resolve-Path "$PSScriptRoot\..\.."

# List of directories that need to be tracked but kept empty of data
$directoriesToKeep = @(
    "data\raw",
    "data\processed",
    "data\curated",
    "data\staging",
    "logs"
)

Write-Host "--- Initializing .gitkeep files ---" -ForegroundColor Cyan

foreach ($dir in $directoriesToKeep) {
    $fullPath = Join-Path $projectRoot $dir
    
    # 1. Ensure directory exists
    if (-not (Test-Path $fullPath)) {
        New-Item -ItemType Directory -Force -Path $fullPath | Out-Null
        Write-Host "Created directory: $dir" -ForegroundColor Yellow
    }

    # 2. Create .gitkeep file
    $gitKeepPath = Join-Path $fullPath ".gitkeep"
    if (-not (Test-Path $gitKeepPath)) {
        New-Item -ItemType File -Force -Path $gitKeepPath | Out-Null
        Write-Host "   [+] Created .gitkeep in $dir" -ForegroundColor Green
    } else {
        Write-Host "   [.] .gitkeep already exists in $dir" -ForegroundColor Gray
    }
}

Write-Host "`nStructure initialization complete." -ForegroundColor Green