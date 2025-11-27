# ETL Runner Script - PowerShell Version
# Executes ETL pipeline inside Docker container

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "🚀 Running Healthcare Data ETL Pipeline" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host ""

# Check if containers are running
Write-Host "🔍 Checking container status..." -ForegroundColor Yellow

$mongoStatus = docker inspect -f '{{.State.Running}}' mongodb 2>$null
$pgStatus = docker inspect -f '{{.State.Running}}' postgres-warehouse 2>$null

if ($mongoStatus -ne "true" -or $pgStatus -ne "true") {
    Write-Host "❌ Error: Required containers are not running" -ForegroundColor Red
    Write-Host "   MongoDB: $mongoStatus" -ForegroundColor Red
    Write-Host "   PostgreSQL: $pgStatus" -ForegroundColor Red
    Write-Host ""
    Write-Host "💡 Start containers with: cd docker; docker-compose up -d" -ForegroundColor Yellow
    exit 1
}

Write-Host "✅ All required containers are running" -ForegroundColor Green
Write-Host ""

# Install Python dependencies in warehouse container
Write-Host "📦 Installing Python dependencies..." -ForegroundColor Yellow
docker exec postgres-warehouse bash -c "pip install pymongo psycopg2-binary --quiet" 2>$null | Out-Null

# Copy ETL script into container
Write-Host "📂 Copying ETL script to container..." -ForegroundColor Yellow
docker cp etl_mongodb_to_postgres.py postgres-warehouse:/tmp/

# Run ETL script
Write-Host ""
Write-Host "🔄 Executing ETL pipeline..." -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
docker exec postgres-warehouse python3 /tmp/etl_mongodb_to_postgres.py

# Check exit code
if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "✅ ETL pipeline completed successfully!" -ForegroundColor Green
    Write-Host ""
    Write-Host "📊 Next steps:" -ForegroundColor Cyan
    Write-Host "   1. Access Superset: http://localhost:8088" -ForegroundColor White
    Write-Host "   2. Login with admin/admin" -ForegroundColor White
    Write-Host "   3. Connect to database: postgresql://dataeng:dataeng123@postgres-warehouse:5432/healthcare_dw" -ForegroundColor White
    Write-Host "   4. Create visualizations from the data" -ForegroundColor White
    Write-Host ""
    Write-Host "🎨 Or run the automated Superset setup:" -ForegroundColor Cyan
    Write-Host "   python superset_setup.py" -ForegroundColor White
}
else {
    Write-Host ""
    Write-Host "❌ ETL pipeline failed" -ForegroundColor Red
    exit 1
}
