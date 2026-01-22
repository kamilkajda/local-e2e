# Script to organize the 'exploration' folder into logical subdirectories.

$explorationPath = Resolve-Path "$PSScriptRoot\..\..\exploration"

Write-Host "--- Organizing Exploration Folder ---" -ForegroundColor Cyan
Write-Host "Target: $explorationPath"

# Define categories and patterns
$categories = @{
    "debug"      = @("debug_*.py")
    "tools"      = @("find_gus_id.py", "reset_azurite.py", "inspect_*.py", "test_*.py")
    "playground" = @("fetch_gus_data.py", "transform_playground.py", "*.ipynb")
}

foreach ($category in $categories.Keys) {
    $targetDir = Join-Path $explorationPath $category
    
    # Create subfolder if missing
    if (-not (Test-Path $targetDir)) {
        New-Item -ItemType Directory -Force -Path $targetDir | Out-Null
        Write-Host "Created folder: $category" -ForegroundColor Yellow
    }

    # Move files matching patterns
    foreach ($pattern in $categories[$category]) {
        $files = Get-ChildItem -Path $explorationPath -Filter $pattern -File
        
        foreach ($file in $files) {
            $destination = Join-Path $targetDir $file.Name
            Move-Item -Path $file.FullName -Destination $destination -Force
            Write-Host "Moved: $($file.Name) -> $category"
        }
    }
}

Write-Host "`n[SUCCESS] Exploration folder organized." -ForegroundColor Green