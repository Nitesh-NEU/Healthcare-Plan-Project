# 🏥 Healthcare Plan Data Engineering Platform

> A production-grade data engineering platform demonstrating end-to-end data pipelines, real-time streaming, batch processing, and advanced analytics.

[![Docker](https://img.shields.io/badge/Docker-Compose-blue)](https://www.docker.com/)
[![Node.js](https://img.shields.io/badge/Node.js-20.x-green)](https://nodejs.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache-Airflow-orange)](https://airflow.apache.org/)

---

## 📋 Table of Contents

- [Architecture Overview](#-architecture-overview)
- [Technology Stack](#-technology-stack)
- [Quick Start Guide](#-quick-start-guide)
- [Complete Testing Guide](#-complete-testing-guide)
- [Data Pipelines Explained](#-data-pipelines-explained)
- [API Documentation](#-api-documentation)
- [Monitoring & UI Access](#-monitoring--ui-access)
- [Troubleshooting](#-troubleshooting)
- [Skills Demonstrated](#-skills-demonstrated)

---

## 🏗️ Architecture Overview

This platform demonstrates **three core data engineering patterns**:

### Technology Stack

#### Core Services
- **API Server**: Node.js + Express (Port 3000)
- **Consumer Service**: Node.js (Port 9090)
- **MongoDB**: Operational database (Port 27017)

#### Data Engineering Stack
- **Apache Kafka**: Event streaming platform (Ports 9092, 9093)
- **Apache Spark**: Distributed data processing (Ports 8081, 7077)
- **PostgreSQL**: Data warehouse (Port 5432)
- **Elasticsearch**: Search and analytics (Port 9200)
- **RabbitMQ**: Message queue (Ports 5672, 15672)

#### Orchestration & Monitoring
- **Apache Airflow**: Workflow orchestration (Port 8082)
- **Apache Superset**: Business intelligence (Port 8088)
- **Kibana**: Log visualization (Port 5601)
- **Kafka UI**: Kafka monitoring (Port 8080)
- **Redis**: Caching layer (Port 6379)

---

## 📊 Data Flow Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        CLIENT LAYER                               │
│                 (Postman, Web Apps, Mobile)                       │
└────────────────────────┬─────────────────────────────────────────┘
                         │ REST API (OAuth2)
┌────────────────────────▼─────────────────────────────────────────┐
│                     API SERVER (Node.js)                          │
│  • Authentication      • Validation       • ETags                 │
│  • CRUD Operations     • Data Quality     • Analytics Endpoints   │
└─────┬──────────────────┬──────────────────┬────────────────────┬─┘
      │                  │                  │                    │
      │ Save             │ Publish          │ Stream             │ Query
      ▼                  ▼                  ▼                    ▼
┌──────────┐      ┌─────────────┐    ┌──────────┐      ┌──────────────┐
│ MongoDB  │      │  RabbitMQ   │    │  Kafka   │      │  PostgreSQL  │
│ (OLTP)   │      │  (Queue)    │    │ (Stream) │      │ (Data Warehouse)│
└──────────┘      └──────┬──────┘    └─────┬────┘      └──────────────┘
                         │                  │
                         ▼                  ▼
                  ┌────────────┐     ┌──────────────┐
                  │ Consumer   │     │ Kafka        │
                  │ Service    │     │ Consumer     │
                  └──────┬─────┘     └──────┬───────┘
                         │                  │
                         ▼                  ▼
                  ┌─────────────────────────────────┐
                  │      Elasticsearch               │
                  │   (Search & Analytics)           │
                  └──────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│                    BATCH PROCESSING LAYER                         │
├──────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐         ┌──────────────┐                       │
│  │  Airflow     │────────▶│  Spark Jobs  │                       │
│  │  (ETL DAGs)  │         │  (Analytics) │                       │
│  └──────────────┘         └──────────────┘                       │
│         │                         │                               │
│         └─────────────────────────▼                               │
│                        PostgreSQL DW                              │
│                    (Star Schema / OLAP)                           │
└──────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│                  VISUALIZATION & BI LAYER                         │
├──────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐       │
│  │   Superset   │    │    Kibana    │    │  Analytics   │       │
│  │ (Dashboards) │    │  (Logs/Viz)  │    │  API         │       │
│  └──────────────┘    └──────────────┘    └──────────────┘       │
└──────────────────────────────────────────────────────────────────┘
```

---

## 🗂️ Data Warehouse Schema (Star Schema)

### Dimension Tables
- **dim_organization**: Healthcare organizations
- **dim_plan_type**: Plan type classifications (HMO, PPO, EPO, etc.)
- **dim_service**: Healthcare services catalog
- **dim_date**: Time dimension (2017-2030)
- **dim_plan**: Plans with SCD Type 2 for history tracking

### Fact Tables
- **fact_plan_costs**: Plan cost shares (accumulating snapshot)
- **fact_service_costs**: Service-level costs (transaction fact)
- **fact_plan_metrics**: Daily aggregated metrics (periodic snapshot)

### Aggregate Tables
- **agg_daily_plan_costs**: Pre-aggregated daily metrics
- **agg_monthly_service_costs**: Monthly service cost summaries

---

## 🚀 Getting Started

### Prerequisites
```bash
# Required software
- Docker & Docker Compose
- Node.js v20+
- Python 3.9+ (for Spark jobs)
- 16GB RAM minimum
```

### Installation

1. **Clone and Install Dependencies**
```bash
cd "Healthcare-Plan-Project-main"
npm install
```

2. **Environment Configuration**
```bash
# Create .env file
OAUTH_CLIENT_ID="your-google-oauth-client-id"
APP_PORT=3000
MONGODB_URI="mongodb://localhost:27017/medicalPlans"
STACK_VERSION=8.13.2
ES_PORT=9200
KIBANA_PORT=5601
```

3. **Start Infrastructure Services**
```bash
cd docker
docker-compose up -d
```

Wait 2-3 minutes for all services to initialize. Verify with:
```bash
docker ps
```

4. **Initialize Database**
```bash
# PostgreSQL data warehouse schema is auto-created
# Verify by connecting to PostgreSQL:
docker exec -it postgres-warehouse psql -U dataeng -d healthcare_dw
\dt  # List tables
\q   # Exit
```

5. **Start Application Services**
```bash
# Terminal 1: API Server
npm start

# Terminal 2: Consumer Service
npm run consumer

# Terminal 3: Kafka Consumer (optional, for real-time analytics)
npm run kafka-consumer
```

---

## 📡 API Endpoints

### Plan Management
```http
GET    /v1/plan              # Get all plans
GET    /v1/plan/:id          # Get single plan
POST   /v1/plan              # Create plan
PUT    /v1/plan/:id          # Update plan
PATCH  /v1/plan/:id          # Add linked services
DELETE /v1/plan/:id          # Delete plan
```

### Analytics Endpoints (NEW ✨)
```http
# Cost Analytics
GET /v1/analytics/cost-trends          # Cost trends by plan type
GET /v1/analytics/monthly-trends       # Monthly aggregations
  ?year=2024&planType=inNetwork

# Service Analytics  
GET /v1/analytics/service-analysis     # Service cost breakdown
GET /v1/analytics/top-services         # Top services by cost
  ?limit=10

# Metrics & Dashboards
GET /v1/analytics/plan-metrics         # Overall plan metrics
GET /v1/analytics/dashboard            # Real-time dashboard data

# Spark Analytics Results
GET /v1/analytics/spark/cost-trends    # Spark cost analysis
GET /v1/analytics/spark/service-patterns  # Service patterns
GET /v1/analytics/spark/anomalies      # Cost anomaly detection
GET /v1/analytics/spark/monthly-metrics   # Monthly metrics

# Data Engineering Monitoring
GET /v1/analytics/etl-logs             # ETL pipeline logs
  ?status=SUCCESS&limit=50
GET /v1/analytics/data-quality         # Data quality checks
  ?status=PASSED&tableName=fact_plan_costs
```

### Authentication
All endpoints require OAuth Bearer token:
```http
Authorization: Bearer <your-google-oauth-token>
```

---

## 🔄 Data Processing Workflows

### 1. Real-Time Stream Processing (Kafka)

**Event Types:**
- `plan-created`: Published when new plan created
- `plan-updated`: Published on plan modifications
- `plan-deleted`: Published on plan deletion
- `plan-analytics`: Real-time metrics events

**Kafka Consumer** indexes events to Elasticsearch and triggers data quality checks.

### 2. Batch ETL Pipeline (Airflow)

**DAG: `healthcare_plan_etl`**
- Schedule: Daily at 2 AM
- Tasks:
  1. Extract plans from MongoDB
  2. Transform to dimensional model
  3. Load to PostgreSQL data warehouse
  4. Update aggregate tables

**Run manually:**
```bash
docker exec -it airflow-webserver airflow dags trigger healthcare_plan_etl
```

### 3. Spark Analytics Jobs

**Job: `healthcare_analytics.py`**
- Cost trend analysis
- Service pattern detection
- Anomaly detection (z-score > 2)
- Monthly metrics calculation

**Run Spark job:**
```bash
docker cp spark-jobs/healthcare_analytics.py spark-master:/opt/spark-apps/
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/healthcare_analytics.py
```

---

## 📊 Data Quality Framework

### Validation Categories

1. **Completeness**: Required fields present
2. **Validity**: Data types and formats correct
3. **Consistency**: Logical relationships maintained
4. **Uniqueness**: No duplicate IDs
5. **Timeliness**: Dates are reasonable

### Running Validation
```javascript
const validator = require('./data-quality/validator');

// Validate single plan
const result = validator.validatePlan(planData);

// Validate batch
const plans = await getPlanDataFromDB();
const batchResults = validator.validateBatch(plans);

// Get statistics
const stats = validator.getStatistics();
```

---

## 🎯 Use Cases for Data Engineers

### 1. Real-Time Analytics Pipeline
```
Kafka Producer → Kafka Topics → Kafka Consumer → Elasticsearch
```
Demonstrates event-driven architecture and stream processing.

### 2. Batch ETL Pipeline
```
MongoDB → Airflow → Data Warehouse (PostgreSQL)
```
Shows scheduled data extraction, transformation, and loading.

### 3. Big Data Processing
```
Data Warehouse → Spark → Analytics Results
```
Demonstrates distributed computing for large-scale analytics.

### 4. Data Quality Engineering
```
Validation Framework → Quality Checks → Monitoring
```
Implements data quality gates and monitoring.

### 5. Multi-Store Architecture
```
MongoDB (OLTP) + PostgreSQL (OLAP) + Elasticsearch (Search)
```
Shows polyglot persistence pattern.

---

## 🖥️ Access UI Interfaces

| Service | URL | Credentials |
|---------|-----|-------------|
| API Server | http://localhost:3000 | OAuth token required |
| Airflow | http://localhost:8082 | admin / admin |
| Superset | http://localhost:8088 | admin / admin |
| Kibana | http://localhost:5601 | - |
| Kafka UI | http://localhost:8080 | - |
| RabbitMQ Mgmt | http://localhost:15672 | guest / guest |
| Spark Master | http://localhost:8081 | - |
| Elasticsearch | http://localhost:9200 | - |

---

## 🧪 Testing the Platform

### 1. Test Basic CRUD
```bash
# Create plan
POST http://localhost:3000/v1/plan

# Verify in MongoDB
docker exec -it mongodb mongosh medicalPlans --eval "db.plans.find()"

# Verify in Elasticsearch
curl http://localhost:9200/planindex/_search?q=*
```

### 2. Test ETL Pipeline
```bash
# Trigger Airflow DAG
curl -X POST http://localhost:8082/api/v1/dags/healthcare_plan_etl/dagRuns \
  -H "Content-Type: application/json" \
  -u admin:admin \
  -d '{}'

# Check data warehouse
docker exec -it postgres-warehouse psql -U dataeng -d healthcare_dw \
  -c "SELECT * FROM v_current_plans_analysis LIMIT 5;"
```

### 3. Test Analytics
```bash
# Get cost trends
curl -H "Authorization: Bearer <token>" \
  http://localhost:3000/v1/analytics/cost-trends

# Get dashboard metrics
curl -H "Authorization: Bearer <token>" \
  http://localhost:3000/v1/analytics/dashboard
```

---

## 📈 Monitoring & Observability

### Logs
- **API Server**: Winston logs in console
- **Airflow**: `/airflow/logs`
- **Spark**: Spark Master UI
- **Kafka**: Kafka UI

### Metrics
- ETL job statistics in `etl_audit_log` table
- Data quality results in `data_quality_checks` table
- Kafka consumer lag in Kafka UI

---

## 🛠️ Troubleshooting

### Common Issues

**1. Services not starting**
```bash
# Check Docker resources
docker stats

# Restart specific service
docker restart <container-name>
```

**2. RabbitMQ connection errors**
```bash
# Restart RabbitMQ and clear queue
docker restart RabbitMQ
```

**3. Spark job failures**
```bash
# Check Spark logs
docker logs spark-master
docker logs spark-worker
```

**4. Airflow DAG not appearing**
```bash
# Restart scheduler
docker restart airflow-scheduler
```

---

## 📚 Key Data Engineering Concepts Demonstrated

✅ **Lambda Architecture**: Batch (Airflow) + Stream (Kafka) processing  
✅ **Data Warehousing**: Star schema with fact/dimension tables  
✅ **ETL/ELT Pipelines**: Airflow orchestrated workflows  
✅ **Stream Processing**: Real-time event processing with Kafka  
✅ **Distributed Computing**: Spark for big data analytics  
✅ **Data Quality**: Comprehensive validation framework  
✅ **Polyglot Persistence**: MongoDB + PostgreSQL + Elasticsearch  
✅ **Data Modeling**: Dimensional modeling, SCD Type 2  
✅ **Workflow Orchestration**: Airflow DAGs  
✅ **Data Lineage**: Audit logs and tracking  

---

## 🎓 Portfolio Highlights

This project demonstrates enterprise-level data engineering skills:

1. **End-to-End Pipeline**: From data ingestion to visualization
2. **Scalability**: Distributed processing with Spark and Kafka
3. **Real-Time + Batch**: Hybrid processing architecture
4. **Data Governance**: Quality checks, validation, audit trails
5. **Modern Stack**: Industry-standard tools (Kafka, Spark, Airflow)
6. **Production Practices**: Monitoring, logging, error handling

---

## 👤 Author

**Nitesh More**

## 📄 License

ISC

---

## 🎉 What's New in v2.0

- ✨ Apache Kafka for event streaming
- ✨ Apache Spark for distributed analytics
- ✨ Apache Airflow for ETL orchestration
- ✨ PostgreSQL data warehouse with star schema
- ✨ Apache Superset for BI dashboards
- ✨ Comprehensive analytics API endpoints
- ✨ Data quality validation framework
- ✨ Real-time and batch processing
- ✨ ETL audit and data lineage tracking
