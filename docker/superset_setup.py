#!/usr/bin/env python3
"""
Superset Dashboard Setup Script
Configures MongoDB connection and creates healthcare analytics dashboards
"""

import json
import requests
from requests.auth import HTTPBasicAuth
import time

# Superset configuration
SUPERSET_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin"

class SupersetConfig:
    def __init__(self):
        self.session = requests.Session()
        self.csrf_token = None
        self.access_token = None
        
    def login(self):
        """Authenticate with Superset"""
        print("🔐 Logging into Superset...")
        
        # Get CSRF token
        response = self.session.get(f"{SUPERSET_URL}/login/")
        
        # Login to get access token
        login_data = {
            "username": USERNAME,
            "password": PASSWORD,
            "provider": "db",
            "refresh": True
        }
        
        response = self.session.post(
            f"{SUPERSET_URL}/api/v1/security/login",
            json=login_data
        )
        
        if response.status_code == 200:
            result = response.json()
            self.access_token = result.get("access_token")
            self.session.headers.update({
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json"
            })
            print("✅ Successfully logged in")
            return True
        else:
            print(f"❌ Login failed: {response.status_code} - {response.text}")
            return False
    
    def get_csrf_token(self):
        """Get CSRF token for API calls"""
        response = self.session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/")
        if response.status_code == 200:
            self.csrf_token = response.json().get("result")
            self.session.headers.update({"X-CSRFToken": self.csrf_token})
            return True
        return False
    
    def create_database_connection(self):
        """Create MongoDB database connection"""
        print("\n📊 Creating MongoDB database connection...")
        
        # Check if database already exists
        response = self.session.get(f"{SUPERSET_URL}/api/v1/database/")
        if response.status_code == 200:
            databases = response.json().get("result", [])
            for db in databases:
                if db.get("database_name") == "MongoDB Healthcare":
                    print("ℹ️  MongoDB connection already exists")
                    return db.get("id")
        
        # Create new database connection
        # Note: Superset doesn't natively support MongoDB, we'll connect to PostgreSQL warehouse instead
        db_config = {
            "database_name": "Healthcare Data Warehouse",
            "sqlalchemy_uri": "postgresql://dataeng:dataeng123@postgres-warehouse:5432/healthcare_dw",
            "expose_in_sqllab": True,
            "allow_ctas": False,
            "allow_cvas": False,
            "allow_dml": False,
            "configuration_method": "sqlalchemy_form",
            "extra": json.dumps({
                "metadata_params": {},
                "engine_params": {},
                "metadata_cache_timeout": {},
                "schemas_allowed_for_csv_upload": []
            })
        }
        
        response = self.session.post(
            f"{SUPERSET_URL}/api/v1/database/",
            json=db_config
        )
        
        if response.status_code in [200, 201]:
            db_id = response.json().get("id")
            print(f"✅ Database connection created (ID: {db_id})")
            return db_id
        else:
            print(f"❌ Failed to create database: {response.status_code} - {response.text}")
            return None
    
    def test_connection(self, db_id):
        """Test the database connection"""
        print("\n🧪 Testing database connection...")
        
        response = self.session.post(
            f"{SUPERSET_URL}/api/v1/database/test_connection/",
            json={"id": db_id}
        )
        
        if response.status_code == 200:
            print("✅ Database connection test successful")
            return True
        else:
            print(f"⚠️  Connection test returned: {response.status_code}")
            return False
    
    def create_dataset(self, db_id, table_name, sql_query=None):
        """Create a dataset (virtual table) in Superset"""
        print(f"\n📋 Creating dataset: {table_name}...")
        
        dataset_config = {
            "database": db_id,
            "schema": "public",
            "table_name": table_name
        }
        
        if sql_query:
            dataset_config["sql"] = sql_query
        
        response = self.session.post(
            f"{SUPERSET_URL}/api/v1/dataset/",
            json=dataset_config
        )
        
        if response.status_code in [200, 201]:
            dataset_id = response.json().get("id")
            print(f"✅ Dataset created (ID: {dataset_id})")
            return dataset_id
        else:
            print(f"⚠️  Dataset creation: {response.status_code} - {response.text}")
            return None
    
    def create_chart(self, dataset_id, chart_name, viz_type, query_context):
        """Create a chart in Superset"""
        print(f"\n📈 Creating chart: {chart_name}...")
        
        chart_config = {
            "slice_name": chart_name,
            "viz_type": viz_type,
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "params": json.dumps(query_context),
            "query_context": json.dumps(query_context)
        }
        
        response = self.session.post(
            f"{SUPERSET_URL}/api/v1/chart/",
            json=chart_config
        )
        
        if response.status_code in [200, 201]:
            chart_id = response.json().get("id")
            print(f"✅ Chart created (ID: {chart_id})")
            return chart_id
        else:
            print(f"⚠️  Chart creation: {response.status_code}")
            return None
    
    def create_dashboard(self, dashboard_name, chart_ids):
        """Create a dashboard with specified charts"""
        print(f"\n🎨 Creating dashboard: {dashboard_name}...")
        
        # Build dashboard layout
        positions = {}
        row = 0
        for idx, chart_id in enumerate(chart_ids):
            positions[f"CHART-{chart_id}"] = {
                "type": "CHART",
                "id": chart_id,
                "children": [],
                "meta": {
                    "width": 6,
                    "height": 50,
                    "chartId": chart_id
                },
                "parents": ["ROOT_ID", f"ROW-{row}"]
            }
            if idx % 2 == 1:
                row += 1
        
        dashboard_config = {
            "dashboard_title": dashboard_name,
            "slug": dashboard_name.lower().replace(" ", "-"),
            "position_json": json.dumps(positions),
            "published": True
        }
        
        response = self.session.post(
            f"{SUPERSET_URL}/api/v1/dashboard/",
            json=dashboard_config
        )
        
        if response.status_code in [200, 201]:
            dashboard_id = response.json().get("id")
            print(f"✅ Dashboard created (ID: {dashboard_id})")
            print(f"🌐 Access at: {SUPERSET_URL}/superset/dashboard/{dashboard_id}/")
            return dashboard_id
        else:
            print(f"⚠️  Dashboard creation: {response.status_code}")
            return None

def main():
    print("=" * 60)
    print("🏥 Healthcare Analytics Dashboard Setup")
    print("=" * 60)
    
    config = SupersetConfig()
    
    # Step 1: Login
    if not config.login():
        print("\n❌ Setup failed: Could not login to Superset")
        print("💡 Make sure Superset is running: docker ps | grep superset")
        return
    
    # Step 2: Get CSRF token
    if not config.get_csrf_token():
        print("\n⚠️  Could not get CSRF token, continuing anyway...")
    
    # Step 3: Create database connection
    db_id = config.create_database_connection()
    if not db_id:
        print("\n❌ Setup failed: Could not create database connection")
        return
    
    # Step 4: Test connection
    config.test_connection(db_id)
    
    # Step 5: Create datasets from existing views
    print("\n" + "=" * 60)
    print("📊 Creating Datasets")
    print("=" * 60)
    
    datasets = [
        ("v_current_plans_analysis", None),
        ("v_monthly_cost_trends", None),
        ("v_service_cost_analysis", None),
    ]
    
    dataset_ids = []
    for table_name, sql_query in datasets:
        ds_id = config.create_dataset(db_id, table_name, sql_query)
        if ds_id:
            dataset_ids.append(ds_id)
        time.sleep(1)  # Rate limiting
    
    print("\n" + "=" * 60)
    print("✅ Setup Complete!")
    print("=" * 60)
    print(f"\n🌐 Access Superset at: {SUPERSET_URL}")
    print(f"👤 Username: {USERNAME}")
    print(f"🔑 Password: {PASSWORD}")
    print("\n📊 Next Steps:")
    print("1. Go to SQL Lab to explore data")
    print("2. Create charts from the datasets")
    print("3. Build dashboards by combining charts")
    print("\n💡 Views available:")
    print("   - v_current_plans_analysis")
    print("   - v_monthly_cost_trends")
    print("   - v_service_cost_analysis")
    
if __name__ == "__main__":
    main()
