#!/usr/bin/env python3
"""
ETL Pipeline: MongoDB to PostgreSQL
Extracts healthcare plan data from MongoDB and loads into PostgreSQL warehouse
"""

import json
from datetime import datetime
import psycopg2
from pymongo import MongoClient
import hashlib

# Database connections
MONGO_URI = "mongodb://mongodb:27017/medicalPlans"
PG_CONFIG = {
    "host": "postgres-warehouse",
    "port": 5432,
    "database": "healthcare_dw",
    "user": "dataeng",
    "password": "dataeng123"
}

class ETLPipeline:
    def __init__(self):
        self.mongo_client = None
        self.pg_conn = None
        self.pg_cursor = None
        self.stats = {
            "plans_extracted": 0,
            "plans_loaded": 0,
            "services_loaded": 0,
            "errors": 0
        }
    
    def connect(self):
        """Establish database connections"""
        print("📡 Connecting to databases...")
        
        try:
            # MongoDB
            self.mongo_client = MongoClient(MONGO_URI)
            self.mongo_db = self.mongo_client.medicalPlans
            print("✅ Connected to MongoDB")
            
            # PostgreSQL
            self.pg_conn = psycopg2.connect(**PG_CONFIG)
            self.pg_cursor = self.pg_conn.cursor()
            print("✅ Connected to PostgreSQL")
            
            return True
        except Exception as e:
            print(f"❌ Connection error: {e}")
            return False
    
    def generate_key(self, *values):
        """Generate surrogate key using MD5 hash"""
        combined = "|".join([str(v) for v in values])
        return hashlib.md5(combined.encode()).hexdigest()[:16]
    
    def get_or_create_date_key(self, date_value):
        """Get or create date dimension key"""
        if not date_value:
            date_value = datetime.now()
        
        date_str = date_value.strftime("%Y-%m-%d") if isinstance(date_value, datetime) else str(date_value)
        
        self.pg_cursor.execute("""
            INSERT INTO dim_date (date_value, year, month, month_name, quarter, day_of_week, week_of_year)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_value) DO NOTHING
            RETURNING date_key
        """, (
            date_str,
            date_value.year if isinstance(date_value, datetime) else 2024,
            date_value.month if isinstance(date_value, datetime) else 1,
            date_value.strftime("%B") if isinstance(date_value, datetime) else "January",
            (date_value.month - 1) // 3 + 1 if isinstance(date_value, datetime) else 1,
            date_value.strftime("%A") if isinstance(date_value, datetime) else "Monday",
            date_value.isocalendar()[1] if isinstance(date_value, datetime) else 1
        ))
        
        result = self.pg_cursor.fetchone()
        if result:
            return result[0]
        
        # If INSERT didn't return (due to conflict), fetch existing key
        self.pg_cursor.execute("SELECT date_key FROM dim_date WHERE date_value = %s", (date_str,))
        return self.pg_cursor.fetchone()[0]
    
    def load_dimensions(self, plan):
        """Load dimension tables and return keys"""
        keys = {}
        
        # Organization dimension
        org_id = plan.get("_org", "unknown")
        org_name = plan.get("_org", "Unknown Organization")
        
        self.pg_cursor.execute("""
            INSERT INTO dim_organization (org_id, org_name)
            VALUES (%s, %s)
            ON CONFLICT (org_id) DO UPDATE SET org_name = EXCLUDED.org_name
            RETURNING org_key
        """, (org_id, org_name))
        keys["org_key"] = self.pg_cursor.fetchone()[0]
        
        # Plan Type dimension
        plan_type = plan.get("planType", "unknown")
        
        self.pg_cursor.execute("""
            INSERT INTO dim_plan_type (plan_type_name)
            VALUES (%s)
            ON CONFLICT (plan_type_name) DO NOTHING
            RETURNING plan_type_key
        """, (plan_type,))
        
        result = self.pg_cursor.fetchone()
        if result:
            keys["plan_type_key"] = result[0]
        else:
            self.pg_cursor.execute("SELECT plan_type_key FROM dim_plan_type WHERE plan_type_name = %s", (plan_type,))
            keys["plan_type_key"] = self.pg_cursor.fetchone()[0]
        
        # Date dimension
        creation_date = plan.get("creationDate", datetime.now())
        if isinstance(creation_date, str):
            try:
                creation_date = datetime.strptime(creation_date[:10], "%Y-%m-%d")
            except:
                creation_date = datetime.now()
        
        keys["creation_date_key"] = self.get_or_create_date_key(creation_date)
        
        # Plan dimension
        plan_id = plan.get("objectId", plan.get("_id", "unknown"))
        
        self.pg_cursor.execute("""
            INSERT INTO dim_plan (plan_id, plan_name, plan_type_key, org_key, is_current, effective_date, end_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (plan_id) DO UPDATE 
            SET plan_name = EXCLUDED.plan_name, 
                plan_type_key = EXCLUDED.plan_type_key,
                org_key = EXCLUDED.org_key
            RETURNING plan_key
        """, (
            plan_id,
            f"{plan_type} Plan",
            keys["plan_type_key"],
            keys["org_key"],
            True,
            creation_date,
            None
        ))
        keys["plan_key"] = self.pg_cursor.fetchone()[0]
        
        return keys
    
    def load_plan_facts(self, plan, keys):
        """Load fact tables for plan costs and metrics"""
        
        # Extract cost information
        deductible = plan.get("planCostShares", {}).get("deductible", 0)
        copay = plan.get("planCostShares", {}).get("copay", 0)
        
        # Count linked services
        linked_services = plan.get("linkedPlanServices", [])
        services_count = len(linked_services) if isinstance(linked_services, list) else 0
        
        # Calculate total cost shares
        total_cost_shares = deductible + copay
        
        # Insert into fact_plan_costs
        self.pg_cursor.execute("""
            INSERT INTO fact_plan_costs (
                plan_key, plan_type_key, org_key, creation_date_key,
                deductible, copay, total_cost_shares, service_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            keys["plan_key"],
            keys["plan_type_key"],
            keys["org_key"],
            keys["creation_date_key"],
            deductible,
            copay,
            total_cost_shares,
            services_count
        ))
        
        # Insert into fact_plan_metrics
        self.pg_cursor.execute("""
            INSERT INTO fact_plan_metrics (
                plan_key, org_key, creation_date_key,
                total_services, total_deductible, total_copay, total_cost
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            keys["plan_key"],
            keys["org_key"],
            keys["creation_date_key"],
            services_count,
            deductible,
            copay,
            total_cost_shares
        ))
    
    def load_service_facts(self, plan, keys):
        """Load service-level facts"""
        linked_services = plan.get("linkedPlanServices", [])
        
        if not isinstance(linked_services, list):
            return
        
        for service_data in linked_services:
            # Get service info
            service_name = service_data.get("name", "Unknown Service")
            service_org = service_data.get("_org", plan.get("_org", "unknown"))
            
            # Insert service dimension
            self.pg_cursor.execute("""
                INSERT INTO dim_service (service_name, service_category)
                VALUES (%s, %s)
                ON CONFLICT (service_name) DO NOTHING
                RETURNING service_key
            """, (service_name, "Healthcare Service"))
            
            result = self.pg_cursor.fetchone()
            if result:
                service_key = result[0]
            else:
                self.pg_cursor.execute("SELECT service_key FROM dim_service WHERE service_name = %s", (service_name,))
                service_key = self.pg_cursor.fetchone()[0]
            
            # Get service cost shares
            cost_shares = service_data.get("linkedService", {}).get("planserviceCostShares", {})
            service_deductible = cost_shares.get("deductible", 0)
            service_copay = cost_shares.get("copay", 0)
            
            # Insert service fact
            self.pg_cursor.execute("""
                INSERT INTO fact_service_costs (
                    service_key, plan_key, plan_type_key, org_key, creation_date_key,
                    service_deductible, service_copay, total_service_cost
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                service_key,
                keys["plan_key"],
                keys["plan_type_key"],
                keys["org_key"],
                keys["creation_date_key"],
                service_deductible,
                service_copay,
                service_deductible + service_copay
            ))
            
            self.stats["services_loaded"] += 1
    
    def extract_and_load(self):
        """Main ETL process"""
        print("\n🔄 Starting ETL process...")
        
        # Extract from MongoDB
        print("📥 Extracting data from MongoDB...")
        plans = list(self.mongo_db.plans.find({}))
        self.stats["plans_extracted"] = len(plans)
        print(f"   Found {len(plans)} plans")
        
        # Transform and Load
        print("🔧 Transforming and loading to PostgreSQL...")
        
        for idx, plan in enumerate(plans, 1):
            try:
                # Load dimensions
                keys = self.load_dimensions(plan)
                
                # Load facts
                self.load_plan_facts(plan, keys)
                self.load_service_facts(plan, keys)
                
                self.stats["plans_loaded"] += 1
                
                if idx % 10 == 0:
                    print(f"   Processed {idx}/{len(plans)} plans...")
                    self.pg_conn.commit()
                
            except Exception as e:
                print(f"⚠️  Error processing plan {plan.get('objectId', 'unknown')}: {e}")
                self.stats["errors"] += 1
                self.pg_conn.rollback()
        
        # Final commit
        self.pg_conn.commit()
        
        # Update ETL audit log
        self.log_etl_run()
    
    def log_etl_run(self):
        """Log ETL execution to audit table"""
        print("\n📝 Logging ETL run...")
        
        self.pg_cursor.execute("""
            INSERT INTO etl_audit_log (
                etl_process_name, source_system, target_table,
                records_processed, records_inserted, records_updated, records_failed,
                execution_status, execution_message
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            "MongoDB to PostgreSQL ETL",
            "MongoDB medicalPlans",
            "Multiple Tables",
            self.stats["plans_extracted"],
            self.stats["plans_loaded"],
            0,
            self.stats["errors"],
            "SUCCESS" if self.stats["errors"] == 0 else "COMPLETED_WITH_ERRORS",
            f"Loaded {self.stats['plans_loaded']} plans and {self.stats['services_loaded']} services"
        ))
        
        self.pg_conn.commit()
    
    def print_stats(self):
        """Print ETL statistics"""
        print("\n" + "=" * 60)
        print("📊 ETL Statistics")
        print("=" * 60)
        print(f"Plans extracted:     {self.stats['plans_extracted']}")
        print(f"Plans loaded:        {self.stats['plans_loaded']}")
        print(f"Services loaded:     {self.stats['services_loaded']}")
        print(f"Errors:              {self.stats['errors']}")
        print("=" * 60)
    
    def cleanup(self):
        """Close database connections"""
        if self.pg_cursor:
            self.pg_cursor.close()
        if self.pg_conn:
            self.pg_conn.close()
        if self.mongo_client:
            self.mongo_client.close()
        print("🧹 Cleaned up connections")

def main():
    print("=" * 60)
    print("🏥 Healthcare Data ETL Pipeline")
    print("   MongoDB → PostgreSQL Data Warehouse")
    print("=" * 60)
    
    etl = ETLPipeline()
    
    try:
        # Connect to databases
        if not etl.connect():
            return
        
        # Run ETL
        etl.extract_and_load()
        
        # Show statistics
        etl.print_stats()
        
        print("\n✅ ETL completed successfully!")
        
    except Exception as e:
        print(f"\n❌ ETL failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        etl.cleanup()

if __name__ == "__main__":
    main()
