# final_repo_polish.ps1
# Performs a final cleanup of research artifacts and temporary files.
# Ensures files are removed from Git index to maintain a professional repository state.

$projectRoot = (Resolve-Path "$PSScriptRoot\..\..").Path
Set-Location $projectRoot

Write-Host "--- FINAL REPOSITORY POLISH ---" -ForegroundColor Cyan

# 1. Targeted Legacy Files (Polish artifacts and obsolete debug tools)
$filesToDelete = @(
    "exploration\checklist.txt",
    "exploration\metrics_checklist.txt",
    "exploration\debug_api_structure.py",
    "exploration\fetch_gus_data.py"
)

# 2. Execution of physical and Git removal
foreach ($file in $filesToDelete) {
    $filePath = Join-Path $projectRoot $file
    
    if (Test-Path $filePath) {
        # Check if the file is tracked by Git
        $gitStatus = git ls-files $file
        
        if ($gitStatus) {
            Write-Host "   [Git] Removing from index: $file" -ForegroundColor Yellow
            git rm --cached $file -f | Out-Null
        }
        
        # Physical removal
        Remove-Item -Path $filePath -Force
        Write-Host "   [Local] Deleted: $file" -ForegroundColor Green
    }
}

# 3. Synchronize .gitkeep files (Crucial for folder structure preservation)
$maintenancePath = Join-Path $projectRoot "scripts\maintenance\init_gitkeep.ps1"
if (Test-Path $maintenancePath) {
    Write-Host "`n   [Status] Refreshing .gitkeep structure..."
    & $maintenancePath
}

# 4. Final Data/Staging Purge
$cleanupPath = Join-Path $projectRoot "scripts\maintenance\clean_staging.ps1"
if (Test-Path $cleanupPath) {
    Write-Host "   [Status] Clearing transient Spark artifacts..."
    & $cleanupPath
}

# 5. Git Status Summary
Write-Host "`n--- GIT REPOSITORY STATUS ---" -ForegroundColor Cyan
git status --short

Write-Host "`n>>> SUCCESS: Repository is clean and professional." -ForegroundColor Green
Write-Host ">>> ACTION REQUIRED: Run 'git commit -m `"docs: remove legacy research artifacts`"' to finalize." -ForegroundColor Gray