"""
Healthcare ETL DAG - Automated Data Pipeline
Extracts healthcare plans from MongoDB → Loads to PostgreSQL warehouse
Scheduled to run daily at 2 AM
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import subprocess
import json

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'email': ['alerts@healthcare.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# Define the DAG
dag = DAG(
    'healthcare_etl_pipeline',
    default_args=default_args,
    description='Automated ETL: MongoDB → PostgreSQL Data Warehouse',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['healthcare', 'etl', 'mongodb', 'postgresql'],
)

def check_mongodb_connection():
    """Task 1: Verify MongoDB is accessible"""
    import pymongo
    try:
        client = pymongo.MongoClient('mongodb://mongodb:27017/', serverSelectionTimeoutMS=5000)
        client.server_info()
        db = client['medicalPlans']
        count = db.plans.count_documents({})
        print(f"✅ MongoDB connected - {count} plans found")
        return count
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        raise

def check_postgres_connection():
    """Task 2: Verify PostgreSQL warehouse is accessible"""
    import psycopg2
    try:
        conn = psycopg2.connect(
            host='postgres-warehouse',
            port=5432,
            database='healthcare_dw',
            user='dataeng',
            password='dataeng123'
        )
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM dim_plan")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        print(f"✅ PostgreSQL connected - {count} plans in warehouse")
        return count
    except Exception as e:
        print(f"❌ PostgreSQL connection failed: {e}")
        raise

def extract_from_mongodb(**context):
    """Task 3: Extract healthcare plans from MongoDB"""
    import pymongo
    from bson import ObjectId
    import json
    
    client = pymongo.MongoClient('mongodb://mongodb:27017/')
    db = client['medicalPlans']
    
    # Extract all plans
    plans = list(db.plans.find({}))
    print(f"📥 Extracted {len(plans)} plans from MongoDB")
    
    # Convert to JSON string and back to handle all ObjectId conversions
    def convert_objectid(obj):
        """Recursively convert ObjectId to string"""
        if isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, dict):
            return {k: convert_objectid(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_objectid(item) for item in obj]
        return obj
    
    # Apply conversion to all plans
    plans_serializable = convert_objectid(plans)
    
    # Push to XCom for next task
    context['ti'].xcom_push(key='extracted_plans', value=plans_serializable)
    client.close()
    
    return len(plans)

def transform_and_load(**context):
    """Task 4: Transform data and load to PostgreSQL warehouse"""
    import psycopg2
    from datetime import datetime
    
    # Pull extracted plans from XCom
    plans = context['ti'].xcom_pull(key='extracted_plans', task_ids='extract_from_mongodb')
    
    if not plans:
        print("⚠️  No plans to load")
        return 0
    
    print(f"🔧 Transforming and loading {len(plans)} plans...")
    
    conn = psycopg2.connect(
        host='postgres-warehouse',
        port=5432,
        database='healthcare_dw',
        user='dataeng',
        password='dataeng123'
    )
    cursor = conn.cursor()
    
    plans_loaded = 0
    services_loaded = 0
    
    for plan in plans:
        try:
            # Get or create date key
            creation_date = plan.get('creationDate', '2025-01-01')
            if isinstance(creation_date, dict) and '$date' in creation_date:
                creation_date = creation_date['$date'][:10]
            
            date_obj = datetime.strptime(str(creation_date)[:10], '%Y-%m-%d')
            date_key = int(date_obj.strftime('%Y%m%d'))
            
            # Insert into dim_date if not exists
            cursor.execute("""
                INSERT INTO dim_date (date_key, date_value, year, quarter, month, month_name, 
                                     day, day_of_week, day_name, week_of_year, is_weekend)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date_key) DO NOTHING
            """, (
                date_key, date_obj.date(), date_obj.year,
                (date_obj.month - 1) // 3 + 1, date_obj.month,
                date_obj.strftime('%B'), date_obj.day, date_obj.weekday(),
                date_obj.strftime('%A'), date_obj.isocalendar()[1],
                date_obj.weekday() >= 5
            ))
            
            # Get or create organization
            org_name = plan.get('_org', 'Unknown')
            cursor.execute("""
                INSERT INTO dim_organization (org_id, org_name)
                VALUES (%s, %s)
                ON CONFLICT (org_id) DO NOTHING
                RETURNING org_key
            """, (org_name, org_name))
            
            result = cursor.fetchone()
            if result:
                org_key = result[0]
            else:
                cursor.execute("SELECT org_key FROM dim_organization WHERE org_id = %s", (org_name,))
                org_key = cursor.fetchone()[0]
            
            # Get or create plan type
            plan_type = plan.get('planType', 'Unknown')
            cursor.execute("""
                INSERT INTO dim_plan_type (plan_type_code, plan_type_name)
                VALUES (%s, %s)
                ON CONFLICT (plan_type_code) DO NOTHING
                RETURNING plan_type_key
            """, (plan_type, plan_type))
            
            result = cursor.fetchone()
            if result:
                plan_type_key = result[0]
            else:
                cursor.execute("SELECT plan_type_key FROM dim_plan_type WHERE plan_type_code = %s", (plan_type,))
                plan_type_key = cursor.fetchone()[0]
            
            # Insert plan
            plan_id = plan.get('objectId', plan.get('_id', 'unknown'))
            cursor.execute("""
                INSERT INTO dim_plan (plan_id, plan_type_key, org_key, is_current, effective_date)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (plan_id) DO NOTHING
                RETURNING plan_key
            """, (plan_id, plan_type_key, org_key, True, date_obj.date()))
            
            result = cursor.fetchone()
            if result:
                plan_key = result[0]
                plans_loaded += 1
            else:
                cursor.execute("SELECT plan_key FROM dim_plan WHERE plan_id = %s", (plan_id,))
                plan_key = cursor.fetchone()[0]
            
            # Insert plan costs
            cost_shares = plan.get('planCostShares', {})
            deductible = cost_shares.get('deductible', 0) if isinstance(cost_shares, dict) else 0
            copay = cost_shares.get('copay', 0) if isinstance(cost_shares, dict) else 0
            
            cursor.execute("""
                INSERT INTO fact_plan_costs 
                (plan_key, plan_type_key, org_key, creation_date_key, deductible, copay, total_cost_shares)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (plan_key, plan_type_key, org_key, date_key, deductible, copay, deductible + copay))
            
            # Load services
            services = plan.get('linkedPlanServices', [])
            for service in services:
                # linkedService is a nested object
                linked_service = service.get('linkedService', {})
                if isinstance(linked_service, dict):
                    service_id = linked_service.get('objectId', 'unknown')
                    service_name = linked_service.get('name', 'Unknown Service')
                else:
                    service_id = str(linked_service)
                    service_name = 'Unknown Service'
                
                cursor.execute("""
                    INSERT INTO dim_service (service_id, service_name, service_category)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (service_id) DO NOTHING
                    RETURNING service_key
                """, (service_id, service_name, 'Healthcare Service'))
                
                result = cursor.fetchone()
                if result:
                    service_key = result[0]
                    services_loaded += 1
                else:
                    cursor.execute("SELECT service_key FROM dim_service WHERE service_id = %s", (service_id,))
                    service_key = cursor.fetchone()[0]
                
                cursor.execute("""
                    INSERT INTO fact_service_costs 
                    (service_key, plan_key, org_key, date_key, deductible, copay, total_service_cost)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (service_key, plan_key, org_key, date_key, 0, 0, 0))
            
        except Exception as e:
            print(f"⚠️  Error loading plan {plan.get('objectId', 'unknown')}: {e}")
            continue
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"✅ Loaded {plans_loaded} plans and {services_loaded} services")
    return {'plans': plans_loaded, 'services': services_loaded}

def run_data_quality_checks(**context):
    """Task 5: Validate data quality in warehouse"""
    import psycopg2
    
    conn = psycopg2.connect(
        host='postgres-warehouse',
        port=5432,
        database='healthcare_dw',
        user='dataeng',
        password='dataeng123'
    )
    cursor = conn.cursor()
    
    issues = []
    
    # Check 1: Ensure no NULL values in critical columns
    cursor.execute("""
        SELECT COUNT(*) FROM dim_plan WHERE plan_id IS NULL OR plan_type_key IS NULL
    """)
    null_count = cursor.fetchone()[0]
    if null_count > 0:
        issues.append(f"Found {null_count} plans with NULL critical values")
    
    # Check 2: Validate cost ranges
    cursor.execute("""
        SELECT COUNT(*) FROM fact_plan_costs 
        WHERE deductible < 0 OR copay < 0 OR deductible > 100000
    """)
    invalid_costs = cursor.fetchone()[0]
    if invalid_costs > 0:
        issues.append(f"Found {invalid_costs} plans with invalid cost values")
    
    # Check 3: Verify referential integrity
    cursor.execute("""
        SELECT COUNT(*) FROM fact_plan_costs fpc
        LEFT JOIN dim_plan dp ON fpc.plan_key = dp.plan_key
        WHERE dp.plan_key IS NULL
    """)
    orphaned = cursor.fetchone()[0]
    if orphaned > 0:
        issues.append(f"Found {orphaned} orphaned cost records")
    
    cursor.close()
    conn.close()
    
    if issues:
        print("⚠️  Data quality issues detected:")
        for issue in issues:
            print(f"   - {issue}")
    else:
        print("✅ All data quality checks passed")
    
    return len(issues) == 0

def log_etl_run(**context):
    """Task 6: Log ETL execution to audit table"""
    import psycopg2
    from datetime import datetime
    
    ti = context['ti']
    plans_loaded = ti.xcom_pull(key='return_value', task_ids='transform_and_load')
    
    # Use logical_date instead of execution_date to avoid Proxy object
    execution_dt = context.get('logical_date', datetime.now())
    if hasattr(execution_dt, 'datetime'):
        execution_dt = execution_dt.datetime
    
    conn = psycopg2.connect(
        host='postgres-warehouse',
        port=5432,
        database='healthcare_dw',
        user='dataeng',
        password='dataeng123'
    )
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO etl_audit_log 
        (job_name, job_type, start_time, end_time, status, records_processed, records_inserted, records_failed)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        'Airflow Healthcare ETL',
        'Scheduled',
        execution_dt,
        datetime.now(),
        'SUCCESS',
        plans_loaded.get('plans', 0) if isinstance(plans_loaded, dict) else 0,
        plans_loaded.get('plans', 0) if isinstance(plans_loaded, dict) else 0,
        0
    ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"✅ Logged ETL run to audit table")
    
    print("📝 ETL run logged to audit table")

# Define tasks
check_mongodb = PythonOperator(
    task_id='check_mongodb_connection',
    python_callable=check_mongodb_connection,
    dag=dag,
)

check_postgres = PythonOperator(
    task_id='check_postgres_connection',
    python_callable=check_postgres_connection,
    dag=dag,
)

extract = PythonOperator(
    task_id='extract_from_mongodb',
    python_callable=extract_from_mongodb,
    provide_context=True,
    dag=dag,
)

transform_load = PythonOperator(
    task_id='transform_and_load',
    python_callable=transform_and_load,
    provide_context=True,
    dag=dag,
)

quality_checks = PythonOperator(
    task_id='run_data_quality_checks',
    python_callable=run_data_quality_checks,
    provide_context=True,
    dag=dag,
)

log_run = PythonOperator(
    task_id='log_etl_run',
    python_callable=log_etl_run,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
check_mongodb >> check_postgres >> extract >> transform_load >> quality_checks >> log_run
