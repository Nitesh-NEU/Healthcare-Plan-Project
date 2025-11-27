"""
Automated Superset Dashboard Setup
Creates database connection, datasets, charts, and dashboard via API
"""

import requests
import json
import time

# Superset configuration
SUPERSET_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin"

# PostgreSQL connection details
DB_CONFIG = {
    "database_name": "Healthcare Data Warehouse",
    "sqlalchemy_uri": "postgresql://dataeng:dataeng123@postgres-warehouse:5432/healthcare_dw",
    "expose_in_sqllab": True,
    "allow_ctas": True,
    "allow_cvas": True,
    "allow_dml": True
}

# Views to create as datasets
VIEWS = [
    "v_current_plans_analysis",
    "v_monthly_cost_trends",
    "v_service_cost_analysis"
]

# Chart configurations
CHARTS = [
    {
        "slice_name": "Total Plans Count",
        "viz_type": "big_number_total",
        "dataset": "v_current_plans_analysis",
        "params": {
            "metric": "count",
            "adhoc_filters": []
        }
    },
    {
        "slice_name": "Average Costs by Plan Type",
        "viz_type": "dist_bar",
        "dataset": "v_current_plans_analysis",
        "params": {
            "metrics": ["AVG(deductible)", "AVG(copay)"],
            "groupby": ["plan_type_name"],
            "adhoc_filters": []
        }
    },
    {
        "slice_name": "Monthly Cost Trends",
        "viz_type": "line",
        "dataset": "v_monthly_cost_trends",
        "params": {
            "metrics": ["avg_deductible", "avg_copay"],
            "groupby": ["month_name"],
            "adhoc_filters": []
        }
    },
    {
        "slice_name": "Service Distribution",
        "viz_type": "pie",
        "dataset": "v_service_cost_analysis",
        "params": {
            "metric": "count",
            "groupby": ["service_name"],
            "adhoc_filters": []
        }
    }
]

class SupersetAutomation:
    def __init__(self):
        self.session = requests.Session()
        self.access_token = None
        self.csrf_token = None
        self.database_id = None
        self.dataset_ids = {}
        self.chart_ids = []
        
    def login(self):
        """Authenticate and get access token"""
        print("🔐 Logging in to Superset...")
        
        # Login directly to API (skip CSRF for API login)
        login_data = {
            "username": USERNAME,
            "password": PASSWORD,
            "provider": "db"
        }
        
        try:
            response = self.session.post(
                f"{SUPERSET_URL}/api/v1/security/login",
                json=login_data,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                self.access_token = data.get("access_token")
                self.session.headers.update({
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json"
                })
                print("✅ Logged in successfully")
                return True
            else:
                print(f"❌ Login failed: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"❌ Connection error: {e}")
            print("💡 Make sure Superset is running at http://localhost:8088")
            return False
    
    def get_csrf_token(self):
        """Get CSRF token for form submissions"""
        response = self.session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/")
        if response.status_code == 200:
            self.csrf_token = response.json().get("result")
            self.session.headers.update({"X-CSRFToken": self.csrf_token})
            return True
        return False
    
    def check_database_exists(self):
        """Check if database already exists"""
        print("🔍 Checking for existing database connection...")
        
        response = self.session.get(f"{SUPERSET_URL}/api/v1/database/")
        if response.status_code == 200:
            databases = response.json().get("result", [])
            for db in databases:
                if "healthcare" in db.get("database_name", "").lower():
                    self.database_id = db.get("id")
                    print(f"✅ Found existing database: {db.get('database_name')} (ID: {self.database_id})")
                    return True
        return False
    
    def create_database_connection(self):
        """Create PostgreSQL database connection"""
        if self.check_database_exists():
            return True
            
        print("📊 Creating database connection...")
        
        response = self.session.post(
            f"{SUPERSET_URL}/api/v1/database/",
            json=DB_CONFIG
        )
        
        if response.status_code in [200, 201]:
            self.database_id = response.json().get("id")
            print(f"✅ Database created with ID: {self.database_id}")
            return True
        else:
            print(f"❌ Failed to create database: {response.status_code} - {response.text}")
            return False
    
    def create_datasets(self):
        """Create datasets from PostgreSQL views"""
        print("📋 Creating datasets...")
        
        for view_name in VIEWS:
            # Check if dataset already exists
            response = self.session.get(
                f"{SUPERSET_URL}/api/v1/dataset/?q={json.dumps({'filters': [{'col': 'table_name', 'opr': 'eq', 'value': view_name}]})}"
            )
            
            if response.status_code == 200:
                existing = response.json().get("result", [])
                if existing:
                    self.dataset_ids[view_name] = existing[0]["id"]
                    print(f"✅ Dataset already exists: {view_name} (ID: {existing[0]['id']})")
                    continue
            
            # Create new dataset
            dataset_data = {
                "database": self.database_id,
                "schema": "public",
                "table_name": view_name
            }
            
            response = self.session.post(
                f"{SUPERSET_URL}/api/v1/dataset/",
                json=dataset_data
            )
            
            if response.status_code in [200, 201]:
                dataset_id = response.json().get("id")
                self.dataset_ids[view_name] = dataset_id
                print(f"✅ Created dataset: {view_name} (ID: {dataset_id})")
            else:
                print(f"❌ Failed to create dataset {view_name}: {response.status_code} - {response.text}")
        
        return len(self.dataset_ids) > 0
    
    def create_charts(self):
        """Create charts from datasets"""
        print("📈 Creating charts...")
        
        for chart_config in CHARTS:
            dataset_name = chart_config["dataset"]
            if dataset_name not in self.dataset_ids:
                print(f"⚠️  Skipping chart {chart_config['slice_name']} - dataset not found")
                continue
            
            # Build chart data
            chart_data = {
                "slice_name": chart_config["slice_name"],
                "viz_type": chart_config["viz_type"],
                "datasource_id": self.dataset_ids[dataset_name],
                "datasource_type": "table",
                "params": json.dumps(chart_config["params"]),
                "query_context": json.dumps({
                    "datasource": {
                        "id": self.dataset_ids[dataset_name],
                        "type": "table"
                    },
                    "queries": [{
                        "columns": [],
                        "metrics": chart_config["params"].get("metrics", [chart_config["params"].get("metric")]),
                        "filters": []
                    }]
                })
            }
            
            response = self.session.post(
                f"{SUPERSET_URL}/api/v1/chart/",
                json=chart_data
            )
            
            if response.status_code in [200, 201]:
                chart_id = response.json().get("id")
                self.chart_ids.append(chart_id)
                print(f"✅ Created chart: {chart_config['slice_name']} (ID: {chart_id})")
            else:
                print(f"❌ Failed to create chart {chart_config['slice_name']}: {response.status_code} - {response.text}")
        
        return len(self.chart_ids) > 0
    
    def create_dashboard(self):
        """Create dashboard and add charts"""
        print("🎨 Creating dashboard...")
        
        # Build dashboard layout
        position_json = {}
        row = 0
        for idx, chart_id in enumerate(self.chart_ids):
            col = (idx % 2) * 6  # 2 charts per row
            if idx % 2 == 0 and idx > 0:
                row += 4
            
            position_json[f"CHART-{chart_id}"] = {
                "children": [],
                "id": f"CHART-{chart_id}",
                "meta": {
                    "chartId": chart_id,
                    "height": 50,
                    "width": 6
                },
                "type": "CHART",
                "parents": ["ROOT_ID", f"ROW-{row}"]
            }
        
        dashboard_data = {
            "dashboard_title": "Healthcare Plan Analytics Dashboard",
            "slug": "healthcare-analytics",
            "published": True,
            "position_json": json.dumps(position_json),
            "json_metadata": json.dumps({
                "default_filters": "{}",
                "filter_scopes": {}
            })
        }
        
        response = self.session.post(
            f"{SUPERSET_URL}/api/v1/dashboard/",
            json=dashboard_data
        )
        
        if response.status_code in [200, 201]:
            dashboard_id = response.json().get("id")
            print(f"✅ Dashboard created successfully! (ID: {dashboard_id})")
            print(f"🌐 Access it at: {SUPERSET_URL}/superset/dashboard/{dashboard_id}/")
            return True
        else:
            print(f"❌ Failed to create dashboard: {response.status_code} - {response.text}")
            return False
    
    def run(self):
        """Execute full automation workflow"""
        print("\n" + "="*60)
        print("🚀 SUPERSET AUTOMATION STARTING")
        print("="*60 + "\n")
        
        if not self.login():
            return False
        
        self.get_csrf_token()
        
        if not self.create_database_connection():
            return False
        
        time.sleep(2)  # Wait for database to be ready
        
        if not self.create_datasets():
            return False
        
        time.sleep(2)  # Wait for datasets to be ready
        
        if not self.create_charts():
            print("⚠️  Some charts failed, but continuing...")
        
        time.sleep(2)
        
        if not self.create_dashboard():
            return False
        
        print("\n" + "="*60)
        print("✅ AUTOMATION COMPLETED SUCCESSFULLY!")
        print("="*60)
        print(f"\n📊 Summary:")
        print(f"   - Database ID: {self.database_id}")
        print(f"   - Datasets created: {len(self.dataset_ids)}")
        print(f"   - Charts created: {len(self.chart_ids)}")
        print(f"\n🌐 Open Superset: {SUPERSET_URL}")
        print("="*60 + "\n")
        
        return True

if __name__ == "__main__":
    automation = SupersetAutomation()
    success = automation.run()
    exit(0 if success else 1)
