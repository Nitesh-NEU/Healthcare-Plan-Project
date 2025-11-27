import pymongo
import psycopg2
from bson import ObjectId
from datetime import datetime

# Extract from MongoDB
client = pymongo.MongoClient('mongodb://mongodb:27017/')
db = client['medicalPlans']
plans = list(db.plans.find({}))
print(f'✅ Extracted {len(plans)} plans from MongoDB')

# Convert ObjectId
def convert_objectid(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, dict):
        return {k: convert_objectid(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_objectid(item) for item in obj]
    return obj

plans_clean = convert_objectid(plans)
print(f'✅ Converted ObjectIds')
print(f'First plan sample: objectId={plans_clean[0].get("objectId")}, _org={plans_clean[0].get("_org")}')

# Connect to PostgreSQL
conn = psycopg2.connect(
    host='postgres-warehouse',
    port=5432,
    database='healthcare_dw',
    user='dataeng',
    password='dataeng123'
)
cursor = conn.cursor()
print(f'✅ Connected to PostgreSQL')

# Try loading first plan
plan = plans_clean[0]
print(f'\n🔧 Loading plan: {plan.get("objectId")}')

try:
    # Get creation date
    creation_date = plan.get('creationDate', '2025-01-01')
    if isinstance(creation_date, dict) and '$date' in creation_date:
        creation_date = creation_date['$date'][:10]
    
    date_obj = datetime.strptime(str(creation_date)[:10], '%Y-%m-%d')
    date_key = int(date_obj.strftime('%Y%m%d'))
    print(f'   Date: {date_key}')
    
    # Insert date
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
    print(f'   ✅ Date inserted')
    
    # Insert organization
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
    print(f'   ✅ Organization: {org_name} (key={org_key})')
    
    # Insert plan type
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
    print(f'   ✅ Plan type: {plan_type} (key={plan_type_key})')
    
    # Insert plan
    plan_id = plan.get('objectId', plan.get('_id', 'unknown'))
    print(f'   Inserting plan with ID: {plan_id}')
    cursor.execute("""
        INSERT INTO dim_plan (plan_id, plan_type_key, org_key, is_current, effective_date)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (plan_id) DO NOTHING
        RETURNING plan_key
    """, (plan_id, plan_type_key, org_key, True, date_obj.date()))
    
    result = cursor.fetchone()
    if result:
        plan_key = result[0]
        print(f'   ✅ Plan inserted (key={plan_key})')
    else:
        cursor.execute("SELECT plan_key FROM dim_plan WHERE plan_id = %s", (plan_id,))
        plan_key = cursor.fetchone()[0]
        print(f'   ⚠️  Plan already exists (key={plan_key})')
    
    conn.commit()
    print(f'\n✅✅✅ Successfully loaded plan!')
    
except Exception as e:
    print(f'\n❌ ERROR: {e}')
    import traceback
    traceback.print_exc()

finally:
    cursor.close()
    conn.close()
    client.close()
