# Healthcare Data Engineering - Automated Startup Script
# Starts all services with CDC-based ETL automation

Write-Host "`n" -NoNewline
Write-Host ("="*70) -ForegroundColor Cyan
Write-Host "  HEALTHCARE DATA ENGINEERING PLATFORM - AUTOMATED STARTUP" -ForegroundColor Yellow
Write-Host ("="*70) -ForegroundColor Cyan
Write-Host "`n"

# Step 1: Start Docker Infrastructure
Write-Host "[1/4] " -NoNewline -ForegroundColor White
Write-Host "Starting Docker Infrastructure..." -ForegroundColor Green
Write-Host "      MongoDB, PostgreSQL, Superset, Airflow" -ForegroundColor Gray
Set-Location "docker"
docker-compose up -d
Set-Location ".."
Write-Host "      ✓ Docker services started`n" -ForegroundColor Green
Start-Sleep -Seconds 5

# Step 2: Wait for MongoDB to be ready
Write-Host "[2/4] " -NoNewline -ForegroundColor White
Write-Host "Waiting for MongoDB to be ready..." -ForegroundColor Green
$retries = 0
while ($retries -lt 30) {
    $result = docker exec mongodb mongosh --quiet --eval "db.adminCommand('ping')" 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "      ✓ MongoDB is ready`n" -ForegroundColor Green
        break
    }
    Start-Sleep -Seconds 2
    $retries++
}

# Step 3: Start REST API Server
Write-Host "[3/4] " -NoNewline -ForegroundColor White
Write-Host "Starting REST API Server..." -ForegroundColor Green
Write-Host "      Endpoint: http://localhost:3000" -ForegroundColor Gray
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$PWD'; node server.js" -WindowStyle Minimized
Start-Sleep -Seconds 3
Write-Host "      ✓ REST API started`n" -ForegroundColor Green

# Step 4: Start CDC ETL Watcher
Write-Host "[4/4] " -NoNewline -ForegroundColor White
Write-Host "Starting CDC ETL Watcher..." -ForegroundColor Green
Write-Host "      Monitoring MongoDB for changes" -ForegroundColor Gray
Write-Host "      Auto-triggers ETL on new data" -ForegroundColor Gray
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$PWD'; node cdc_etl_watcher.js"

Write-Host "`n"
Write-Host ("="*70) -ForegroundColor Cyan
Write-Host "  ✓ ALL SERVICES STARTED SUCCESSFULLY!" -ForegroundColor Green
Write-Host ("="*70) -ForegroundColor Cyan
Write-Host "`n"

Write-Host "📊 SERVICE ENDPOINTS:" -ForegroundColor Yellow
Write-Host "   • REST API:        http://localhost:3000" -ForegroundColor White
Write-Host "   • Superset:        http://localhost:8088 (admin/admin)" -ForegroundColor White
Write-Host "   • Airflow:         http://localhost:8082 (admin/admin)" -ForegroundColor White
Write-Host "   • MongoDB:         mongodb://localhost:27017/healthcare" -ForegroundColor White
Write-Host "   • PostgreSQL:      localhost:5433/healthcare_dw (dataeng/dataeng123)" -ForegroundColor White
Write-Host "`n"

Write-Host "🔄 AUTOMATION STATUS:" -ForegroundColor Yellow
Write-Host "   • CDC Watcher:     Active (monitoring MongoDB)" -ForegroundColor Green
Write-Host "   • ETL Pipeline:    Auto-triggered on data changes" -ForegroundColor Green
Write-Host "   • Dashboard:       http://localhost:8088/superset/dashboard/2/" -ForegroundColor Green
Write-Host "`n"

Write-Host "📝 QUICK ACTIONS:" -ForegroundColor Yellow
Write-Host "   • Add healthcare plan: POST http://localhost:3000/plan" -ForegroundColor White
Write-Host "   • View dashboard:      Open http://localhost:8088/superset/dashboard/2/" -ForegroundColor White
Write-Host "   • Manual ETL:          node etl_runner.js" -ForegroundColor White
Write-Host "`n"

Write-Host ("="*70) -ForegroundColor Cyan
Write-Host "  System is ready! Add data via REST API to see CDC in action." -ForegroundColor Cyan
Write-Host ("="*70) -ForegroundColor Cyan
Write-Host "`n"

# Keep this window open
Write-Host "Press Ctrl+C to view this summary again, or close to stop monitoring." -ForegroundColor Gray
while ($true) {
    Start-Sleep -Seconds 3600
}
