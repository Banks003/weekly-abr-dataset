# validate_worker.ps1
#
# One-shot local validation for the abr-api Cloudflare Worker.
# - Installs npm deps if needed
# - Starts `wrangler dev` in the background
# - Polls until the dev server is ready
# - Hits each endpoint with timing and prints results
# - Cleans up the wrangler process on exit (success OR failure)
#
# Run from repo root:
#   .\scripts\validate_worker.ps1

$ErrorActionPreference = "Stop"
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$workerDir = Join-Path $repoRoot "worker"

if (-not (Test-Path $workerDir)) {
    Write-Error "worker directory not found at $workerDir"
    exit 1
}

Push-Location $workerDir
$wranglerProcess = $null

try {
    # 1. Install deps if needed.
    if (-not (Test-Path (Join-Path $workerDir "node_modules"))) {
        Write-Host "==> Installing dependencies (first run, ~50 MB)..." -ForegroundColor Cyan
        npm install --silent
        if ($LASTEXITCODE -ne 0) {
            Write-Error "npm install failed (exit $LASTEXITCODE)"
            exit 1
        }
    } else {
        Write-Host "==> node_modules present; skipping npm install." -ForegroundColor DarkGray
    }

    # 2. Start wrangler dev in the background. Pipe output to a log file.
    $logFile = Join-Path $env:TEMP "wrangler-dev-$(Get-Date -Format 'yyyyMMdd-HHmmss').log"
    Write-Host "==> Starting wrangler dev (log: $logFile)..." -ForegroundColor Cyan
    $wranglerProcess = Start-Process -FilePath "npx" `
        -ArgumentList "wrangler", "dev", "--port", "8787" `
        -WorkingDirectory $workerDir `
        -RedirectStandardOutput $logFile `
        -RedirectStandardError "$logFile.err" `
        -PassThru -NoNewWindow

    # 3. Poll until ready (max 60s).
    $base = "http://localhost:8787"
    $ready = $false
    for ($i = 0; $i -lt 60; $i++) {
        Start-Sleep -Seconds 1
        try {
            $r = Invoke-WebRequest -Uri "$base/" -Method GET -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop
            if ($r.StatusCode -eq 200) { $ready = $true; break }
        } catch {
            # not yet up
        }
        if ($wranglerProcess.HasExited) {
            Write-Host "==> wrangler dev exited unexpectedly. Last log lines:" -ForegroundColor Red
            if (Test-Path $logFile) { Get-Content $logFile -Tail 30 }
            if (Test-Path "$logFile.err") { Get-Content "$logFile.err" -Tail 30 }
            exit 1
        }
    }
    if (-not $ready) {
        Write-Host "==> Timeout waiting for wrangler dev. Last log lines:" -ForegroundColor Red
        if (Test-Path $logFile) { Get-Content $logFile -Tail 30 }
        exit 1
    }
    Write-Host "==> Dev server ready at $base`n" -ForegroundColor Green

    # 4. Run probes against each endpoint.
    $probes = @(
        @{ Name = "discovery";    Url = "$base/" }
        @{ Name = "manifest";     Url = "$base/manifest" }
        @{ Name = "profile (DuckDB-WASM cold)"; Url = "$base/abn/16009661901" }
        @{ Name = "profile (warm)"; Url = "$base/abn/11000013098" }
        @{ Name = "search qantas"; Url = "$base/search?q=qantas&limit=3" }
        @{ Name = "trends by_state"; Url = "$base/trends/by_state" }
        @{ Name = "trends registrations"; Url = "$base/trends/registrations?since=2024-01-01&by=month" }
    )

    foreach ($p in $probes) {
        Write-Host ("--- {0} ---" -f $p.Name) -ForegroundColor Yellow
        Write-Host "GET $($p.Url)"
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        try {
            $resp = Invoke-WebRequest -Uri $p.Url -Method GET -UseBasicParsing -TimeoutSec 60 -ErrorAction Stop
            $sw.Stop()
            $bodyPreview = if ($resp.Content.Length -gt 400) {
                $resp.Content.Substring(0, 400) + "..."
            } else {
                $resp.Content
            }
            Write-Host ("  status={0}  elapsed={1:N1}s  bytes={2}" -f $resp.StatusCode, $sw.Elapsed.TotalSeconds, $resp.Content.Length) -ForegroundColor Green
            Write-Host "  body: $bodyPreview" -ForegroundColor DarkGray
        } catch {
            $sw.Stop()
            $statusCode = if ($_.Exception.Response) { $_.Exception.Response.StatusCode.value__ } else { "n/a" }
            $body = ""
            try {
                if ($_.Exception.Response) {
                    $stream = $_.Exception.Response.GetResponseStream()
                    $reader = New-Object System.IO.StreamReader($stream)
                    $body = $reader.ReadToEnd()
                }
            } catch {}
            Write-Host ("  status={0}  elapsed={1:N1}s  ERROR" -f $statusCode, $sw.Elapsed.TotalSeconds) -ForegroundColor Red
            Write-Host "  body: $body" -ForegroundColor Red
        }
        Write-Host ""
    }

    Write-Host "==> Validation complete. Wrangler log file: $logFile" -ForegroundColor Cyan
}
finally {
    if ($wranglerProcess -and -not $wranglerProcess.HasExited) {
        Write-Host "==> Stopping wrangler dev (PID $($wranglerProcess.Id))..." -ForegroundColor DarkGray
        try {
            Stop-Process -Id $wranglerProcess.Id -Force -ErrorAction SilentlyContinue
            # Also kill child node processes wrangler may have spawned
            Get-CimInstance Win32_Process -Filter "ParentProcessId = $($wranglerProcess.Id)" |
                ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
        } catch {}
    }
    Pop-Location
}
