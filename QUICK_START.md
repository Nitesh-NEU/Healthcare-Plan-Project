# Quick Start Guide - Data Engineering Platform

## 🚀 Setup Steps (10 minutes)

### Step 1: Install Dependencies
```powershell
cd "ABD-NodeJS-RestAPI-main"
npm install
```

### Step 2: Start Infrastructure
```powershell
cd docker
docker-compose up -d
```

**Wait 3-4 minutes** for all services to initialize. You can monitor with:
```powershell
docker-compose logs -f
```

### Step 3: Verify Services
```powershell
docker ps
```

You should see 15+ containers running:
- ✅ elasticsearch
- ✅ kibana
- ✅ RabbitMQ
- ✅ mongodb
- ✅ postgres-warehouse
- ✅ kafka
- ✅ zookeeper
- ✅ spark-master
- ✅ spark-worker
- ✅ airflow-webserver
- ✅ airflow-scheduler
- ✅ superset
- ✅ redis
- ✅ kafka-ui

### Step 4: Start Application
```powershell
# Terminal 1: API Server
npm start

# Terminal 2: Consumer Service  
npm run consumer
```

### Step 5: Test the System

#### A. Create a Plan
```http
POST http://localhost:3000/v1/plan
Authorization: Bearer <your-token>
Content-Type: application/json

{
    "_org": "example.com",
    "objectId": "test-plan-001",
    "objectType": "plan",
    "planType": "inNetwork",
    "creationDate": "2024-01-15",
    "planCostShares": {
        "deductible": 2000,
        "_org": "example.com",
        "copay": 23,
        "objectId": "cost-001",
        "objectType": "membercostshare"
    },
    "linkedPlanServices": [
        {
            "_org": "example.com",
            "objectId": "service-001",
            "objectType": "planservice",
            "linkedService": {
                "_org": "example.com",
                "objectId": "srv-001",
                "objectType": "service",
                "name": "Annual Checkup"
            },
            "planserviceCostShares": {
                "deductible": 0,
                "_org": "example.com",
                "copay": 25,
                "objectId": "srv-cost-001",
                "objectType": "membercostshare"
            }
        }
    ]
}
```

#### B. Run ETL Pipeline
```powershell
# Access Airflow UI: http://localhost:8082
# Login: admin / admin
# Trigger: healthcare_plan_etl DAG
```

Or via API:
```powershell
curl -X POST http://localhost:8082/api/v1/dags/healthcare_plan_etl/dagRuns `
  -H "Content-Type: application/json" `
  -u admin:admin `
  -d '{}'
```

#### C. Run Spark Analytics
```powershell
# Copy Spark job to master
docker cp spark-jobs/healthcare_analytics.py spark-master:/opt/spark-apps/

# Execute Spark job
docker exec -it spark-master spark-submit `
  --master spark://spark-master:7077 `
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.0,org.postgresql:postgresql:42.6.0 `
  /opt/spark-apps/healthcare_analytics.py
```

#### D. Check Analytics
```http
# Get cost trends
GET http://localhost:3000/v1/analytics/cost-trends
Authorization: Bearer <token>

# Get dashboard metrics
GET http://localhost:3000/v1/analytics/dashboard
Authorization: Bearer <token>

# Get Spark results
GET http://localhost:3000/v1/analytics/spark/cost-trends
Authorization: Bearer <token>
```

---

## 🎯 Key URLs

| Service | URL | Purpose |
|---------|-----|---------|
| **API** | http://localhost:3000 | REST API endpoints |
| **Airflow** | http://localhost:8082 | Workflow orchestration |
| **Superset** | http://localhost:8088 | BI dashboards |
| **Kafka UI** | http://localhost:8080 | Kafka monitoring |
| **Spark UI** | http://localhost:8081 | Spark jobs |
| **Kibana** | http://localhost:5601 | Log visualization |
| **RabbitMQ** | http://localhost:15672 | Queue management |

---

## 🔍 Verification Commands

### Check MongoDB
```powershell
docker exec -it mongodb mongosh medicalPlans --eval "db.plans.countDocuments()"
```

### Check PostgreSQL Data Warehouse
```powershell
docker exec -it postgres-warehouse psql -U dataeng -d healthcare_dw -c "SELECT COUNT(*) FROM dim_plan;"
```

### Check Elasticsearch
```powershell
curl http://localhost:9200/planindex/_count
```

### Check Kafka Topics
```powershell
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

---

## 📊 Sample Queries

### PostgreSQL Analytics
```sql
-- Connect to data warehouse
docker exec -it postgres-warehouse psql -U dataeng -d healthcare_dw

-- Cost trends
SELECT * FROM v_monthly_cost_trends ORDER BY year DESC, month DESC LIMIT 10;

-- Service analysis
SELECT * FROM v_service_cost_analysis ORDER BY total_cost DESC LIMIT 10;

-- ETL audit
SELECT job_name, status, records_processed, end_time 
FROM etl_audit_log 
ORDER BY start_time DESC 
LIMIT 10;
```

### Elasticsearch Queries
```bash
# Search all plans
curl http://localhost:9200/planindex/_search?pretty

# Aggregate by plan type
curl -X POST http://localhost:9200/planindex/_search?pretty -H 'Content-Type: application/json' -d '
{
  "size": 0,
  "aggs": {
    "plan_types": {
      "terms": { "field": "plan_type.keyword" }
    }
  }
}'
```

---

## 🐛 Troubleshooting

### Issue: Kafka connection failed
```powershell
# Restart Kafka
docker restart kafka zookeeper
# Wait 30 seconds
```

### Issue: Airflow webserver not accessible
```powershell
# Check logs
docker logs airflow-webserver

# Restart Airflow
docker restart airflow-webserver airflow-scheduler
```

### Issue: Spark job fails
```powershell
# Check Spark logs
docker logs spark-master
docker logs spark-worker

# Ensure MongoDB and PostgreSQL are accessible
docker exec -it spark-master ping mongodb
docker exec -it spark-master ping postgres-warehouse
```

### Issue: Out of memory
```powershell
# Check Docker resources
docker stats

# Increase Docker Desktop memory to 8GB+ in settings
```

---

## 🎓 Learning Path

1. **Day 1**: Understand architecture, start services, test CRUD APIs
2. **Day 2**: Explore Airflow DAGs, run ETL pipeline
3. **Day 3**: Execute Spark jobs, analyze results
4. **Day 4**: Test Kafka streaming, data quality checks
5. **Day 5**: Build Superset dashboards, end-to-end testing

---

## 📚 Next Steps

1. ✅ Add more Spark jobs (ML models, advanced analytics)
2. ✅ Create Superset dashboards
3. ✅ Implement CDC (Change Data Capture) with Debezium
4. ✅ Add dbt for transformation layer
5. ✅ Implement data versioning with Delta Lake
6. ✅ Add Apache Flink for real-time processing
7. ✅ Deploy to cloud (AWS/Azure/GCP)

---

## 💡 Interview Talking Points

When discussing this project:

1. **Architecture**: "I built a Lambda architecture with both batch and stream processing"
2. **Scale**: "Used Spark for distributed processing of large healthcare datasets"
3. **Real-time**: "Implemented Kafka for event-driven, real-time data pipeline"
4. **Data Quality**: "Built comprehensive validation framework with 20+ quality checks"
5. **Orchestration**: "Used Airflow to schedule and monitor ETL workflows"
6. **Dimensional Modeling**: "Designed star schema with SCD Type 2 for historical tracking"
7. **Polyglot Persistence**: "Used right tool for the job - MongoDB for OLTP, PostgreSQL for OLAP, Elasticsearch for search"

---

## ✅ Success Criteria

Your setup is successful when:
- ✅ All 15 containers running
- ✅ Plans created via API appear in MongoDB
- ✅ Consumer indexes data to Elasticsearch
- ✅ Airflow DAG runs successfully
- ✅ Data appears in PostgreSQL data warehouse
- ✅ Spark job completes without errors
- ✅ Analytics endpoints return data
- ✅ Kafka events flowing through topics

---

## 🆘 Getting Help

If stuck:
1. Check container logs: `docker logs <container-name>`
2. Verify network: `docker network inspect docker_data-engineering-net`
3. Check disk space: `docker system df`
4. Review documentation: `DATA_ENGINEERING_README.md`

---

**Happy Data Engineering! 🚀**
