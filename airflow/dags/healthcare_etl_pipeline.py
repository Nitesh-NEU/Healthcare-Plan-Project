"""
Healthcare Plan ETL Pipeline - Airflow DAG
Extracts data from MongoDB, transforms, and loads into PostgreSQL Data Warehouse
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
import json
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'healthcare_plan_etl',
    default_args=default_args,
    description='ETL pipeline for healthcare plans from MongoDB to PostgreSQL',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    catchup=False,
    tags=['healthcare', 'etl', 'mongodb', 'postgres'],
)

def extract_plans_from_mongodb(**context):
    """
    Extract healthcare plans from MongoDB
    """
    logging.info("Starting extraction from MongoDB...")
    
    try:
        # Note: You'll need to configure MongoDB connection in Airflow UI
        # For now, using direct connection
        from pymongo import MongoClient
        
        client = MongoClient('mongodb://localhost:27017/')
        db = client['medicalPlans']
        collection = db['plans']
        
        plans = list(collection.find({}))
        logging.info(f"Extracted {len(plans)} plans from MongoDB")
        
        # Convert ObjectId to string for JSON serialization
        for plan in plans:
            plan['_id'] = str(plan['_id'])
        
        # Push to XCom for next task
        context['task_instance'].xcom_push(key='plans_data', value=plans)
        
        return len(plans)
    
    except Exception as e:
        logging.error(f"Error extracting from MongoDB: {str(e)}")
        raise

def transform_plans_data(**context):
    """
    Transform plans data for data warehouse
    """
    logging.info("Starting data transformation...")
    
    plans = context['task_instance'].xcom_pull(
        task_ids='extract_plans',
        key='plans_data'
    )
    
    transformed_data = {
        'organizations': [],
        'plan_types': [],
        'services': [],
        'plans': [],
        'plan_costs': [],
        'service_costs': []
    }
    
    for plan in plans:
        # Extract organization
        org_id = plan.get('_org', 'unknown')
        if org_id not in [org['org_id'] for org in transformed_data['organizations']]:
            transformed_data['organizations'].append({
                'org_id': org_id,
                'org_name': org_id
            })
        
        # Extract plan type
        plan_type = plan.get('planType', 'unknown')
        if plan_type not in [pt['plan_type_code'] for pt in transformed_data['plan_types']]:
            transformed_data['plan_types'].append({
                'plan_type_code': plan_type,
                'plan_type_name': plan_type.replace('_', ' ').title()
            })
        
        # Extract plan info
        creation_date = plan.get('creationDate', datetime.now().strftime('%Y-%m-%d'))
        if isinstance(creation_date, str):
            # Handle ISO format
            if 'T' in creation_date:
                creation_date = creation_date.split('T')[0]
        
        transformed_data['plans'].append({
            'plan_id': plan.get('objectId'),
            'plan_type_code': plan_type,
            'org_id': org_id,
            'creation_date': creation_date
        })
        
        # Extract plan cost shares
        if 'planCostShares' in plan:
            cost_shares = plan['planCostShares']
            transformed_data['plan_costs'].append({
                'plan_id': plan.get('objectId'),
                'org_id': org_id,
                'plan_type_code': plan_type,
                'deductible': cost_shares.get('deductible', 0),
                'copay': cost_shares.get('copay', 0),
                'creation_date': creation_date
            })
        
        # Extract linked services
        if 'linkedPlanServices' in plan:
            for service_link in plan['linkedPlanServices']:
                if 'linkedService' in service_link:
                    service = service_link['linkedService']
                    service_id = service.get('objectId')
                    service_name = service.get('name', 'Unknown')
                    
                    # Add to services dimension
                    if service_id not in [s['service_id'] for s in transformed_data['services']]:
                        transformed_data['services'].append({
                            'service_id': service_id,
                            'service_name': service_name,
                            'service_type': service.get('objectType', 'service')
                        })
                    
                    # Add service costs
                    if 'planserviceCostShares' in service_link:
                        service_costs = service_link['planserviceCostShares']
                        transformed_data['service_costs'].append({
                            'plan_id': plan.get('objectId'),
                            'service_id': service_id,
                            'org_id': org_id,
                            'deductible': service_costs.get('deductible', 0),
                            'copay': service_costs.get('copay', 0),
                            'creation_date': creation_date
                        })
    
    logging.info(f"Transformed data: {len(transformed_data['plans'])} plans, "
                f"{len(transformed_data['services'])} services, "
                f"{len(transformed_data['service_costs'])} service costs")
    
    context['task_instance'].xcom_push(key='transformed_data', value=transformed_data)
    return transformed_data

def load_to_warehouse(**context):
    """
    Load transformed data into PostgreSQL data warehouse
    """
    logging.info("Starting load to data warehouse...")
    
    transformed_data = context['task_instance'].xcom_pull(
        task_ids='transform_plans',
        key='transformed_data'
    )
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_warehouse')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # Load organizations
        for org in transformed_data['organizations']:
            cursor.execute("""
                INSERT INTO dim_organization (org_id, org_name)
                VALUES (%s, %s)
                ON CONFLICT (org_id) DO UPDATE SET org_name = EXCLUDED.org_name
            """, (org['org_id'], org['org_name']))
        
        # Load plan types
        for pt in transformed_data['plan_types']:
            cursor.execute("""
                INSERT INTO dim_plan_type (plan_type_code, plan_type_name)
                VALUES (%s, %s)
                ON CONFLICT (plan_type_code) DO NOTHING
            """, (pt['plan_type_code'], pt['plan_type_name']))
        
        # Load services
        for service in transformed_data['services']:
            cursor.execute("""
                INSERT INTO dim_service (service_id, service_name, service_type)
                VALUES (%s, %s, %s)
                ON CONFLICT (service_id) DO UPDATE 
                SET service_name = EXCLUDED.service_name
            """, (service['service_id'], service['service_name'], service['service_type']))
        
        conn.commit()
        
        # Load plans (with surrogate keys)
        for plan in transformed_data['plans']:
            # Get surrogate keys
            cursor.execute("SELECT org_key FROM dim_organization WHERE org_id = %s", (plan['org_id'],))
            org_key = cursor.fetchone()[0]
            
            cursor.execute("SELECT plan_type_key FROM dim_plan_type WHERE plan_type_code = %s", 
                         (plan['plan_type_code'],))
            plan_type_key = cursor.fetchone()[0]
            
            # Insert plan
            cursor.execute("""
                INSERT INTO dim_plan (plan_id, plan_type_key, org_key, effective_date, is_current)
                VALUES (%s, %s, %s, %s, TRUE)
                ON CONFLICT DO NOTHING
            """, (plan['plan_id'], plan_type_key, org_key, plan['creation_date']))
        
        conn.commit()
        
        # Load plan costs
        for cost in transformed_data['plan_costs']:
            # Get surrogate keys
            cursor.execute("SELECT plan_key FROM dim_plan WHERE plan_id = %s AND is_current = TRUE", 
                         (cost['plan_id'],))
            result = cursor.fetchone()
            if not result:
                continue
            plan_key = result[0]
            
            cursor.execute("SELECT org_key FROM dim_organization WHERE org_id = %s", (cost['org_id'],))
            org_key = cursor.fetchone()[0]
            
            cursor.execute("SELECT plan_type_key FROM dim_plan_type WHERE plan_type_code = %s", 
                         (cost['plan_type_code'],))
            plan_type_key = cursor.fetchone()[0]
            
            # Get date key
            creation_date = cost['creation_date']
            date_key = int(creation_date.replace('-', ''))
            
            total_cost = cost['deductible'] + cost['copay']
            
            cursor.execute("""
                INSERT INTO fact_plan_costs 
                (plan_key, org_key, plan_type_key, creation_date_key, deductible, copay, total_cost_shares)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (plan_key, org_key, plan_type_key, date_key, 
                  cost['deductible'], cost['copay'], total_cost))
        
        conn.commit()
        
        # Load service costs
        for service_cost in transformed_data['service_costs']:
            # Get surrogate keys
            cursor.execute("SELECT plan_key FROM dim_plan WHERE plan_id = %s AND is_current = TRUE", 
                         (service_cost['plan_id'],))
            result = cursor.fetchone()
            if not result:
                continue
            plan_key = result[0]
            
            cursor.execute("SELECT service_key FROM dim_service WHERE service_id = %s", 
                         (service_cost['service_id'],))
            result = cursor.fetchone()
            if not result:
                continue
            service_key = result[0]
            
            cursor.execute("SELECT org_key FROM dim_organization WHERE org_id = %s", 
                         (service_cost['org_id'],))
            org_key = cursor.fetchone()[0]
            
            creation_date = service_cost['creation_date']
            date_key = int(creation_date.replace('-', ''))
            
            total_cost = service_cost['deductible'] + service_cost['copay']
            
            cursor.execute("""
                INSERT INTO fact_service_costs 
                (plan_key, service_key, org_key, date_key, deductible, copay, total_service_cost)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (plan_key, service_key, org_key, date_key, 
                  service_cost['deductible'], service_cost['copay'], total_cost))
        
        conn.commit()
        logging.info("Successfully loaded data to warehouse")
        
        # Log ETL audit
        cursor.execute("""
            INSERT INTO etl_audit_log 
            (job_name, job_type, start_time, end_time, status, records_processed, records_inserted)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, ('healthcare_plan_etl', 'FULL_LOAD', context['execution_date'], 
              datetime.now(), 'SUCCESS', len(transformed_data['plans']), 
              len(transformed_data['plans'])))
        
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Error loading to warehouse: {str(e)}")
        
        # Log failure
        cursor.execute("""
            INSERT INTO etl_audit_log 
            (job_name, job_type, start_time, end_time, status, error_message)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, ('healthcare_plan_etl', 'FULL_LOAD', context['execution_date'], 
              datetime.now(), 'FAILED', str(e)))
        conn.commit()
        raise
    finally:
        cursor.close()
        conn.close()

def update_aggregates(**context):
    """
    Update pre-aggregated tables for performance
    """
    logging.info("Updating aggregate tables...")
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_warehouse')
    
    # Update daily aggregates
    postgres_hook.run("""
        INSERT INTO agg_daily_plan_costs (date_key, plan_type_key, total_plans, avg_deductible, avg_copay, total_cost_shares)
        SELECT 
            fpc.creation_date_key,
            fpc.plan_type_key,
            COUNT(DISTINCT fpc.plan_key) as total_plans,
            AVG(fpc.deductible) as avg_deductible,
            AVG(fpc.copay) as avg_copay,
            SUM(fpc.total_cost_shares) as total_cost_shares
        FROM fact_plan_costs fpc
        GROUP BY fpc.creation_date_key, fpc.plan_type_key
        ON CONFLICT (date_key) DO UPDATE SET
            total_plans = EXCLUDED.total_plans,
            avg_deductible = EXCLUDED.avg_deductible,
            avg_copay = EXCLUDED.avg_copay,
            total_cost_shares = EXCLUDED.total_cost_shares,
            updated_at = CURRENT_TIMESTAMP
    """)
    
    logging.info("Aggregates updated successfully")

# Define tasks
extract_task = PythonOperator(
    task_id='extract_plans',
    python_callable=extract_plans_from_mongodb,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_plans',
    python_callable=transform_plans_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_warehouse',
    python_callable=load_to_warehouse,
    dag=dag,
)

aggregate_task = PythonOperator(
    task_id='update_aggregates',
    python_callable=update_aggregates,
    dag=dag,
)

# Define task dependencies
extract_task >> transform_task >> load_task >> aggregate_task
