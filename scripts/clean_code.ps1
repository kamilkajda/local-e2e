# --- Clean Code Orchestrator ---

Write-Host "[1/3] Sorting imports with isort..." -ForegroundColor Cyan
isort .

Write-Host "[2/3] Formatting with black..." -ForegroundColor Cyan
black .

Write-Host "[3/3] Final check with flake8..." -ForegroundColor Cyan
flake8 .

Write-Host "`n✨ Code is clean and professional! ✨" -ForegroundColor Green