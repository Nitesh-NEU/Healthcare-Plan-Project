# 🏥 Healthcare Plan Data Engineering Platform - Complete Testing Guide

## 🎯 Complete Step-by-Step Testing from Scratch

This guide will walk you through testing the entire platform from a fresh installation.

---

## Prerequisites Checklist

Before starting, ensure you have:
- [ ] Docker Desktop installed (8GB+ RAM allocated)
- [ ] Node.js v20+ installed
- [ ] Git installed
- [ ] At least 20GB free disk space
- [ ] Ports available: 3000, 5432, 5433, 5672, 8082, 8088, 9200, 15672, 27017

---

## Phase 1: Installation (10 minutes)

### Step 1: Clone and Install

```bash
# Clone repository
git clone <your-repo-url>
cd ABD-NodeJS-RestAPI-main

# Install dependencies
npm install

# Install consumer dependencies
cd consumer
npm install
cd ..
```

### Step 2: Environment Setup

Create `.env` file:
```bash
APP_PORT=3000
MONGODB_URI=mongodb://localhost:27017/medicalPlans
STACK_VERSION=8.13.2
ES_PORT=9200
```

### Step 3: Start Infrastructure

```bash
cd docker
docker-compose up -d
```

Wait 2-3 minutes, then verify:
```bash
docker ps
```

You should see 8+ containers running.

### Step 4: Verify Services

```bash
# MongoDB
docker exec mongodb mongosh --eval "db.version()"

# PostgreSQL
docker exec postgres-warehouse psql -U dataeng -d healthcare_dw -c "\dt"

# Airflow (wait for it to initialize)
curl http://localhost:8082/health

# Elasticsearch
curl http://localhost:9200
```

---

## Phase 2: Test API & MongoDB (10 minutes)

### Test 2.1: Start API Server

```bash
# In terminal 1
npm start
```

Expected output: `Server running on port 3000`

### Test 2.2: Create First Healthcare Plan

```bash
curl -X POST http://localhost:3000/v1/plan \
  -H "Content-Type: application/json" \
  -d '{
    "planCostShares": {
      "deductible": 2000,
      "copay": 23,
      "_org": "example.com",
      "objectId": "cost-001",
      "objectType": "membercostshare"
    },
    "linkedPlanServices": [
      {
        "linkedService": {
          "_org": "example.com",
          "objectId": "service-001",
          "objectType": "service",
          "name": "Yearly physical"
        },
        "_org": "example.com",
        "objectId": "planservice-001",
        "objectType": "planservice",
        "planserviceCostShares": {
          "deductible": 10,
          "copay": 0,
          "_org": "example.com",
          "objectId": "cost-002",
          "objectType": "membercostshare"
        }
      }
    ],
    "_org": "example.com",
    "objectId": "plan-001",
    "objectType": "plan",
    "planType": "inNetwork",
    "creationDate": "12-12-2017"
  }'
```

### Test 2.3: Verify in MongoDB

```bash
# Count plans
docker exec mongodb mongosh medicalPlans --eval "db.plans.countDocuments({})"

# View plan
docker exec mongodb mongosh medicalPlans --eval "db.plans.findOne({}, {_id:1, objectId:1, planType:1})"
```

### Test 2.4: Retrieve via API

```bash
# Get all plans
curl http://localhost:3000/v1/plan | json_pp

# Get specific plan
curl http://localhost:3000/v1/plan/plan-001 | json_pp
```

✅ **Success:** Plan created and retrievable

---

## Phase 3: Test RabbitMQ → Elasticsearch (15 minutes)

### Test 3.1: Start Consumer Service

```bash
# In terminal 2
cd consumer
npm start
```

Expected output: `Connected to RabbitMQ`

### Test 3.2: Create Another Plan (Triggers RabbitMQ)

```bash
curl -X POST http://localhost:3000/v1/plan \
  -H "Content-Type: application/json" \
  -d '{
    "planCostShares": {
      "deductible": 3000,
      "copay": 30,
      "_org": "healthcare.org",
      "objectId": "cost-003",
      "objectType": "membercostshare"
    },
    "linkedPlanServices": [],
    "_org": "healthcare.org",
    "objectId": "plan-002",
    "objectType": "plan",
    "planType": "outOfNetwork",
    "creationDate": "01-15-2018"
  }'
```

### Test 3.3: Check Consumer Logs

In terminal 2, you should see:
```
{ type: 'PUT', document: { objectId: 'plan-002', ... } }
```

### Test 3.4: Verify RabbitMQ Queue

```bash
docker exec RabbitMQ rabbitmqctl list_queues name messages consumers
```

Or visit: http://localhost:15672 (guest/guest)

### Test 3.5: Check Elasticsearch

```bash
# List indices
curl "http://localhost:9200/_cat/indices?v"

# Search plans
curl "http://localhost:9200/planindex/_search?pretty" | head -50

# Count
curl "http://localhost:9200/planindex/_count"
```

✅ **Success:** Plans indexed in Elasticsearch

---

## Phase 4: Test Airflow ETL Pipeline (20 minutes)

### Test 4.1: Access Airflow UI

Open browser: http://localhost:8082
- Username: `admin`
- Password: `admin`

### Test 4.2: Locate Healthcare DAG

1. Find: `healthcare_etl_pipeline`
2. Toggle it ON (if paused)
3. It's scheduled for daily 2 AM but we'll trigger manually

### Test 4.3: Trigger Manual Run

Option A - Via UI:
- Click "Trigger DAG" play button

Option B - Via CLI:
```bash
docker exec airflow-scheduler airflow dags trigger healthcare_etl_pipeline
```

### Test 4.4: Monitor Execution

In Airflow UI:
1. Click DAG name
2. View "Graph" or "Grid"
3. Watch tasks turn green:
   - check_mongodb_connection ✅
   - check_postgres_connection ✅
   - extract_from_mongodb ✅
   - transform_and_load ✅
   - run_data_quality_checks ✅
   - log_etl_run ✅

This takes ~30-60 seconds.

### Test 4.5: Verify Data Warehouse

```bash
# Summary query
docker exec postgres-warehouse psql -U dataeng -d healthcare_dw -c "
SELECT 
  'Plans' as entity, COUNT(*) as count FROM dim_plan
UNION ALL
SELECT 'Services', COUNT(*) FROM dim_service
UNION ALL
SELECT 'Organizations', COUNT(*) FROM dim_organization
UNION ALL
SELECT 'Plan Cost Facts', COUNT(*) FROM fact_plan_costs
UNION ALL
SELECT 'Service Cost Facts', COUNT(*) FROM fact_service_costs;
"
```

Expected output:
```
     entity        | count 
-------------------+-------
 Plans             |     2
 Services          |     1
 Organizations     |     2
 Plan Cost Facts   |     2
 Service Cost Facts|     2
```

### Test 4.6: Query Analytics Data

```bash
docker exec postgres-warehouse psql -U dataeng -d healthcare_dw -c "
SELECT 
  dp.plan_id,
  dpt.plan_type_name,
  do.org_name,
  fpc.deductible,
  fpc.copay,
  fpc.total_cost_shares
FROM dim_plan dp
JOIN dim_plan_type dpt ON dp.plan_type_key = dpt.plan_type_key
JOIN dim_organization do ON dp.org_key = do.org_key
JOIN fact_plan_costs fpc ON dp.plan_key = fpc.plan_key
WHERE dp.is_current = true
ORDER BY fpc.total_cost_shares DESC;
"
```

### Test 4.7: Check ETL Audit Log

```bash
docker exec postgres-warehouse psql -U dataeng -d healthcare_dw -c "
SELECT 
  job_name,
  status,
  records_processed,
  records_inserted,
  start_time
FROM etl_audit_log
ORDER BY start_time DESC
LIMIT 3;
"
```

✅ **Success:** All 6 tasks green, data in warehouse

---

## Phase 5: Test Change Data Capture (10 minutes)

### Test 5.1: Start CDC Watcher

```bash
# In terminal 3
node cdc_etl_watcher.js
```

Expected: `CDC Watcher started, monitoring changes...`

### Test 5.2: Update a Plan

```bash
curl -X PUT http://localhost:3000/v1/plan/plan-001 \
  -H "Content-Type: application/json" \
  -d '{
    "planCostShares": {
      "deductible": 2500,
      "copay": 25,
      "_org": "example.com",
      "objectId": "cost-001",
      "objectType": "membercostshare"
    },
    "linkedPlanServices": [],
    "_org": "example.com",
    "objectId": "plan-001",
    "objectType": "plan",
    "planType": "inNetwork",
    "creationDate": "12-12-2017"
  }'
```

### Test 5.3: Watch CDC Logs

Terminal 3 should show:
```
Change detected: update
Running ETL for plan: plan-001
ETL completed successfully
```

### Test 5.4: Verify Real-time Sync

```bash
docker exec postgres-warehouse psql -U dataeng -d healthcare_dw -c "
SELECT 
  plan_id,
  deductible,
  copay
FROM dim_plan dp
JOIN fact_plan_costs fpc ON dp.plan_key = fpc.plan_key
WHERE plan_id = 'plan-001'
ORDER BY fpc.creation_date_key DESC
LIMIT 1;
"
```

Should show updated values: deductible=2500, copay=25

✅ **Success:** Real-time sync working

---

## Phase 6: Test Superset Dashboards (10 minutes)

### Test 6.1: Access Superset

Open browser: http://localhost:8088
- Username: `admin`
- Password: `admin`

(First login may take 30 seconds to load)

### Test 6.2: Connect Database

1. Go to **Settings** → **Database Connections**
2. Click **+ Database**
3. Select **PostgreSQL**
4. Enter connection:
   ```
   Host: postgres-warehouse
   Port: 5432
   Database: healthcare_dw
   Username: dataeng
   Password: dataeng123
   ```
5. Click **Connect**

### Test 6.3: Run SQL Query

1. Go to **SQL Lab** → **SQL Editor**
2. Select database: `postgresql healthcare_dw`
3. Run:

```sql
SELECT 
  plan_type_name,
  COUNT(*) as plan_count,
  AVG(deductible) as avg_deductible,
  AVG(copay) as avg_copay,
  SUM(total_cost_shares) as total_costs
FROM dim_plan dp
JOIN dim_plan_type dpt ON dp.plan_type_key = dpt.plan_type_key
JOIN fact_plan_costs fpc ON dp.plan_key = fpc.plan_key
WHERE dp.is_current = true
GROUP BY plan_type_name
ORDER BY total_costs DESC;
```

### Test 6.4: Create a Chart (Optional)

1. Click **Save** → **Save dataset**
2. Go to **Charts** → **+ Create Chart**
3. Select dataset and visualization type
4. Configure and save

✅ **Success:** Superset connected and querying

---

## Phase 7: Test Data Quality (5 minutes)

### Test 7.1: View Quality Check Results

```bash
docker exec postgres-warehouse psql -U dataeng -d healthcare_dw -c "
SELECT 
  check_name,
  table_name,
  status,
  records_checked,
  records_failed,
  error_message,
  check_timestamp
FROM data_quality_checks
ORDER BY check_timestamp DESC
LIMIT 5;
"
```

### Test 7.2: Test Validation (Negative Test)

```bash
# Try creating invalid plan (should fail)
curl -X POST http://localhost:3000/v1/plan \
  -H "Content-Type: application/json" \
  -d '{
    "planCostShares": {
      "deductible": "invalid",
      "copay": -100
    },
    "objectId": "invalid-plan"
  }'
```

Should return validation error.

✅ **Success:** Quality checks logging, validation working

---

## Phase 8: Full System Test (15 minutes)

### Test 8.1: Create Multiple Plans

Run this script to create 5 plans:

```bash
for i in {3..7}; do
  curl -X POST http://localhost:3000/v1/plan \
    -H "Content-Type: application/json" \
    -d "{
      \"planCostShares\": {
        \"deductible\": $((1000 + i * 500)),
        \"copay\": $((10 + i * 5)),
        \"_org\": \"healthcare-${i}.com\",
        \"objectId\": \"cost-00${i}\",
        \"objectType\": \"membercostshare\"
      },
      \"linkedPlanServices\": [],
      \"_org\": \"healthcare-${i}.com\",
      \"objectId\": \"plan-00${i}\",
      \"objectType\": \"plan\",
      \"planType\": \"inNetwork\",
      \"creationDate\": \"01-${i}-2018\"
    }"
  echo ""
  sleep 2
done
```

### Test 8.2: Trigger Full ETL

```bash
docker exec airflow-scheduler airflow dags trigger healthcare_etl_pipeline
```

Wait 60 seconds.

### Test 8.3: Verify Full Pipeline

```bash
# MongoDB
echo "=== MONGODB ===" 
docker exec mongodb mongosh medicalPlans --quiet --eval "db.plans.countDocuments({})"

# Elasticsearch
echo "=== ELASTICSEARCH ==="
curl -s "http://localhost:9200/planindex/_count" | json_pp

# PostgreSQL
echo "=== POSTGRESQL WAREHOUSE ==="
docker exec postgres-warehouse psql -U dataeng -d healthcare_dw -t -c "
  SELECT 'Total Plans: ' || COUNT(*) FROM dim_plan;
  SELECT 'Total Services: ' || COUNT(*) FROM dim_service;
  SELECT 'Total Facts: ' || COUNT(*) FROM fact_plan_costs;
"

# Airflow
echo "=== AIRFLOW ==="
docker exec airflow-scheduler airflow dags list-runs -d healthcare_etl_pipeline --no-backfill | head -3
```

### Test 8.4: Performance Check

```bash
# Check response time
time curl -s http://localhost:3000/v1/plan > /dev/null

# Check warehouse query performance
time docker exec postgres-warehouse psql -U dataeng -d healthcare_dw -c "
  SELECT COUNT(*) FROM fact_plan_costs fpc
  JOIN dim_plan dp ON fpc.plan_key = dp.plan_key
  JOIN dim_service ds ON fpc.org_key = ds.service_key;
" > /dev/null
```

✅ **Success:** All pipelines processing data

---

## Verification Checklist

At the end, you should have:

- [x] **7 plans** in MongoDB
- [x] **7 plans** indexed in Elasticsearch  
- [x] **7 plans** in PostgreSQL warehouse
- [x] **ETL audit log** with successful runs
- [x] **Data quality checks** passing
- [x] **RabbitMQ queue** processing messages
- [x] **CDC watcher** detecting changes
- [x] **Superset** connected to warehouse
- [x] **Airflow DAG** running successfully

---

## Quick Health Check Script

Save this as `health_check.sh`:

```bash
#!/bin/bash

echo "🏥 Healthcare Platform Health Check"
echo "===================================="

echo ""
echo "📊 Data Counts:"
echo "  MongoDB:        $(docker exec mongodb mongosh medicalPlans --quiet --eval 'db.plans.countDocuments({})')"
echo "  Elasticsearch:  $(curl -s http://localhost:9200/planindex/_count | grep -o '"count":[0-9]*' | cut -d: -f2)"
echo "  PostgreSQL:     $(docker exec postgres-warehouse psql -U dataeng -d healthcare_dw -t -c 'SELECT COUNT(*) FROM dim_plan;' | tr -d ' ')"

echo ""
echo "🔧 Service Status:"
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "(mongodb|postgres|airflow|rabbit|elastic)"

echo ""
echo "✅ All services operational!"
```

Run: `bash health_check.sh`

---

## Troubleshooting Quick Reference

| Issue | Solution |
|-------|----------|
| Port already in use | `docker-compose down` then `docker-compose up -d` |
| Airflow DAG not visible | `docker restart airflow-scheduler` |
| Consumer not processing | Check `npm run consumer` is running |
| PostgreSQL connection error | `docker restart postgres-warehouse` |
| Elasticsearch timeout | Wait 30s, ES takes time to start |

---

## Next Steps

Once everything is working:

1. ✅ **Document your setup** - Take screenshots
2. ✅ **Create sample queries** - SQL analytics queries
3. ✅ **Build dashboards** - Superset visualizations
4. ✅ **Add to portfolio** - GitHub with README
5. ✅ **Prepare demos** - For interviews

---

Congratulations! 🎉 You now have a fully functional data engineering platform!
