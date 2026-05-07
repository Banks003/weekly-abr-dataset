# deploy_frontend.ps1 — upload frontend/index.html to the R2 bucket so it's
# served at https://gazetteer.au/abns/index.html (and /abns/ if directory
# index serving is enabled on the bucket).

$ErrorActionPreference = "Stop"
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$frontend = Join-Path $repoRoot "frontend\index.html"
if (-not (Test-Path $frontend)) {
    Write-Error "frontend/index.html not found"
    exit 1
}

Write-Host "==> Uploading $frontend to weekly-abr-dataset/abns/index.html..." -ForegroundColor Cyan
npx wrangler r2 object put weekly-abr-dataset/abns/index.html `
    --file=$frontend `
    --content-type="text/html; charset=utf-8" `
    --remote
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host ""
Write-Host "Live at:" -ForegroundColor Green
Write-Host "  https://gazetteer.au/abns/index.html"
Write-Host "  https://gazetteer.au/abns/   (if R2 has directory-index enabled)"
