#!/bin/bash
# ETL Runner Script - Executes ETL pipeline inside Docker container

echo "========================================="
echo "🚀 Running Healthcare Data ETL Pipeline"
echo "========================================="
echo ""

# Check if containers are running
echo "🔍 Checking container status..."
MONGO_STATUS=$(docker inspect -f '{{.State.Running}}' mongodb 2>/dev/null)
PG_STATUS=$(docker inspect -f '{{.State.Running}}' postgres-warehouse 2>/dev/null)

if [ "$MONGO_STATUS" != "true" ] || [ "$PG_STATUS" != "true" ]; then
    echo "❌ Error: Required containers are not running"
    echo "   MongoDB: $MONGO_STATUS"
    echo "   PostgreSQL: $PG_STATUS"
    echo ""
    echo "💡 Start containers with: cd docker && docker-compose up -d"
    exit 1
fi

echo "✅ All required containers are running"
echo ""

# Install Python dependencies in warehouse container
echo "📦 Installing Python dependencies..."
docker exec postgres-warehouse bash -c "pip install pymongo psycopg2-binary --quiet" 2>/dev/null

# Copy ETL script into container
echo "📂 Copying ETL script to container..."
docker cp etl_mongodb_to_postgres.py postgres-warehouse:/tmp/

# Run ETL script
echo ""
echo "🔄 Executing ETL pipeline..."
echo "========================================="
docker exec postgres-warehouse python3 /tmp/etl_mongodb_to_postgres.py

# Check exit code
if [ $? -eq 0 ]; then
    echo ""
    echo "✅ ETL pipeline completed successfully!"
    echo ""
    echo "📊 Next steps:"
    echo "   1. Access Superset: http://localhost:8088"
    echo "   2. Login with admin/admin"
    echo "   3. Connect to database: postgresql://dataeng:dataeng123@postgres-warehouse:5432/healthcare_dw"
    echo "   4. Create visualizations from the data"
else
    echo ""
    echo "❌ ETL pipeline failed"
    exit 1
fi
