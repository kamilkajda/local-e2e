# Script to create a clean backup (template) of the current project.
# Use this to save the project "skeleton" for future use.

# FIX: Path adjusted for scripts/maintenance location (go up 2 levels)
$sourceDir = Resolve-Path "$PSScriptRoot\..\.."
# The template will be created next to your project folder
$destDir = "$sourceDir-Template" 

Write-Host ">>> Creating project template in: $destDir" -ForegroundColor Cyan
Write-Host ">>> Source: $sourceDir" -ForegroundColor Gray

# 1. Create destination directory
if (Test-Path $destDir) {
    Write-Host "WARNING: Destination folder already exists. Are you sure you want to overwrite it? (Y/N)" -ForegroundColor Yellow
    $confirm = Read-Host
    if ($confirm -ne 'Y' -and $confirm -ne 'y') {
        Write-Host "Cancelled."
        exit
    }
} else {
    New-Item -ItemType Directory -Force -Path $destDir | Out-Null
}

# 2. List of folders/files to copy (valuable assets)
$itemsToCopy = @(
    "src",       # Source code (main.py, utils.py, etc.)
    "scripts",   # Execution scripts (start_all, run_etl)
    "configs",   # Configuration files
    "hadoop",    # IMPORTANT: Configured WinUtils (we don't want to fetch them again)
    ".gitignore",
    "README.md"
)

# 3. Copy items excluding junk
foreach ($item in $itemsToCopy) {
    $srcPath = Join-Path $sourceDir $item
    $dstPath = Join-Path $destDir $item

    if (Test-Path $srcPath) {
        Write-Host "Copying: $item..."
        Copy-Item -Path $srcPath -Destination $dstPath -Recurse -Force
    } else {
        Write-Host "Skipped (not found): $item" -ForegroundColor DarkGray
    }
}

# 4. Clean the template (remove python cache from the copy)
Get-ChildItem -Path $destDir -Include "__pycache__", "*.pyc" -Recurse -Force | Remove-Item -Recurse -Force

# 5. Create empty data structure folders (without files)
$emptyDirs = @(
    "data\raw",
    "data\processed",
    "data\curated",
    "logs",
    "exploration"
)

foreach ($dir in $emptyDirs) {
    $dirPath = Join-Path $destDir $dir
    New-Item -ItemType Directory -Force -Path $dirPath | Out-Null
}

Write-Host "`n>>> SUCCESS! Template saved in: $destDir" -ForegroundColor Green
Write-Host "To start a new project, simply rename this folder and begin working." -ForegroundColor Gray