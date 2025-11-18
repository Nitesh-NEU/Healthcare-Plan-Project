-- Healthcare Plan Data Warehouse - Dimensional Model
-- Star Schema Design for Analytics

-- ===============================
-- DIMENSION TABLES
-- ===============================

-- Dimension: Organization
CREATE TABLE dim_organization (
    org_key SERIAL PRIMARY KEY,
    org_id VARCHAR(255) UNIQUE NOT NULL,
    org_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Plan Type
CREATE TABLE dim_plan_type (
    plan_type_key SERIAL PRIMARY KEY,
    plan_type_code VARCHAR(50) UNIQUE NOT NULL,
    plan_type_name VARCHAR(255),
    plan_type_description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Service
CREATE TABLE dim_service (
    service_key SERIAL PRIMARY KEY,
    service_id VARCHAR(255) UNIQUE NOT NULL,
    service_name VARCHAR(255) NOT NULL,
    service_category VARCHAR(100),
    service_type VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Date (for time-based analytics)
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    date_value DATE UNIQUE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    day INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    week_of_year INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);

-- Dimension: Plan (SCD Type 2 - Slowly Changing Dimension)
CREATE TABLE dim_plan (
    plan_key SERIAL PRIMARY KEY,
    plan_id VARCHAR(255) NOT NULL,
    plan_type_key INTEGER REFERENCES dim_plan_type(plan_type_key),
    org_key INTEGER REFERENCES dim_organization(org_key),
    version INTEGER DEFAULT 1,
    is_current BOOLEAN DEFAULT TRUE,
    effective_date DATE NOT NULL,
    expiration_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_plan_id ON dim_plan(plan_id);
CREATE INDEX idx_dim_plan_current ON dim_plan(plan_id, is_current);

-- ===============================
-- FACT TABLES
-- ===============================

-- Fact: Plan Costs (accumulating snapshot fact)
CREATE TABLE fact_plan_costs (
    fact_plan_cost_key SERIAL PRIMARY KEY,
    plan_key INTEGER NOT NULL REFERENCES dim_plan(plan_key),
    org_key INTEGER NOT NULL REFERENCES dim_organization(org_key),
    plan_type_key INTEGER NOT NULL REFERENCES dim_plan_type(plan_type_key),
    creation_date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    
    -- Measures
    deductible NUMERIC(10, 2),
    copay NUMERIC(10, 2),
    total_cost_shares NUMERIC(10, 2),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_plan_costs_plan ON fact_plan_costs(plan_key);
CREATE INDEX idx_fact_plan_costs_date ON fact_plan_costs(creation_date_key);
CREATE INDEX idx_fact_plan_costs_org ON fact_plan_costs(org_key);

-- Fact: Service Costs (transaction fact)
CREATE TABLE fact_service_costs (
    fact_service_cost_key SERIAL PRIMARY KEY,
    plan_key INTEGER NOT NULL REFERENCES dim_plan(plan_key),
    service_key INTEGER NOT NULL REFERENCES dim_service(service_key),
    org_key INTEGER NOT NULL REFERENCES dim_organization(org_key),
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    
    -- Measures
    deductible NUMERIC(10, 2),
    copay NUMERIC(10, 2),
    total_service_cost NUMERIC(10, 2),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_service_costs_plan ON fact_service_costs(plan_key);
CREATE INDEX idx_fact_service_costs_service ON fact_service_costs(service_key);
CREATE INDEX idx_fact_service_costs_date ON fact_service_costs(date_key);

-- Fact: Plan Metrics (periodic snapshot fact)
CREATE TABLE fact_plan_metrics (
    fact_plan_metric_key SERIAL PRIMARY KEY,
    snapshot_date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    plan_type_key INTEGER NOT NULL REFERENCES dim_plan_type(plan_type_key),
    org_key INTEGER NOT NULL REFERENCES dim_organization(org_key),
    
    -- Measures
    total_plans_count INTEGER,
    total_services_count INTEGER,
    avg_deductible NUMERIC(10, 2),
    avg_copay NUMERIC(10, 2),
    min_deductible NUMERIC(10, 2),
    max_deductible NUMERIC(10, 2),
    total_plan_value NUMERIC(12, 2),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_plan_metrics_date ON fact_plan_metrics(snapshot_date_key);
CREATE INDEX idx_fact_plan_metrics_type ON fact_plan_metrics(plan_type_key);

-- ===============================
-- STAGING TABLES (for ETL)
-- ===============================

CREATE TABLE staging_plans (
    staging_id SERIAL PRIMARY KEY,
    plan_data JSONB NOT NULL,
    source_system VARCHAR(50),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMP,
    error_message TEXT
);

CREATE INDEX idx_staging_plans_processed ON staging_plans(processed);

-- ===============================
-- AUDIT & LINEAGE TABLES
-- ===============================

CREATE TABLE etl_audit_log (
    audit_id SERIAL PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL,
    job_type VARCHAR(50),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20), -- RUNNING, SUCCESS, FAILED
    records_processed INTEGER,
    records_inserted INTEGER,
    records_updated INTEGER,
    records_failed INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE data_quality_checks (
    check_id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    check_type VARCHAR(50) NOT NULL,
    check_description TEXT,
    expected_value VARCHAR(255),
    actual_value VARCHAR(255),
    status VARCHAR(20), -- PASSED, FAILED, WARNING
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ===============================
-- AGGREGATE TABLES (for performance)
-- ===============================

-- Daily aggregates
CREATE TABLE agg_daily_plan_costs (
    date_key INTEGER PRIMARY KEY REFERENCES dim_date(date_key),
    plan_type_key INTEGER REFERENCES dim_plan_type(plan_type_key),
    total_plans INTEGER,
    avg_deductible NUMERIC(10, 2),
    avg_copay NUMERIC(10, 2),
    total_cost_shares NUMERIC(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Monthly aggregates
CREATE TABLE agg_monthly_service_costs (
    year INTEGER,
    month INTEGER,
    service_key INTEGER REFERENCES dim_service(service_key),
    plan_type_key INTEGER REFERENCES dim_plan_type(plan_type_key),
    total_services INTEGER,
    avg_service_cost NUMERIC(10, 2),
    total_copay NUMERIC(12, 2),
    total_deductible NUMERIC(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (year, month, service_key, plan_type_key)
);

-- ===============================
-- HELPER FUNCTIONS
-- ===============================

-- Function to populate date dimension
CREATE OR REPLACE FUNCTION populate_date_dimension(start_date DATE, end_date DATE)
RETURNS void AS $$
DECLARE
    current_date DATE := start_date;
BEGIN
    WHILE current_date <= end_date LOOP
        INSERT INTO dim_date (
            date_key,
            date_value,
            year,
            quarter,
            month,
            month_name,
            day,
            day_of_week,
            day_name,
            week_of_year,
            is_weekend,
            is_holiday
        ) VALUES (
            TO_CHAR(current_date, 'YYYYMMDD')::INTEGER,
            current_date,
            EXTRACT(YEAR FROM current_date)::INTEGER,
            EXTRACT(QUARTER FROM current_date)::INTEGER,
            EXTRACT(MONTH FROM current_date)::INTEGER,
            TO_CHAR(current_date, 'Month'),
            EXTRACT(DAY FROM current_date)::INTEGER,
            EXTRACT(DOW FROM current_date)::INTEGER,
            TO_CHAR(current_date, 'Day'),
            EXTRACT(WEEK FROM current_date)::INTEGER,
            CASE WHEN EXTRACT(DOW FROM current_date) IN (0, 6) THEN TRUE ELSE FALSE END,
            FALSE
        )
        ON CONFLICT (date_key) DO NOTHING;
        
        current_date := current_date + INTERVAL '1 day';
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Populate date dimension for 2017-2030
SELECT populate_date_dimension('2017-01-01'::DATE, '2030-12-31'::DATE);

-- ===============================
-- INITIAL REFERENCE DATA
-- ===============================

-- Insert common plan types
INSERT INTO dim_plan_type (plan_type_code, plan_type_name, plan_type_description) VALUES
('inNetwork', 'In Network', 'Services provided by in-network providers'),
('outOfNetwork', 'Out of Network', 'Services provided by out-of-network providers'),
('hmo', 'HMO', 'Health Maintenance Organization'),
('ppo', 'PPO', 'Preferred Provider Organization'),
('epo', 'EPO', 'Exclusive Provider Organization')
ON CONFLICT (plan_type_code) DO NOTHING;

-- Insert example organization
INSERT INTO dim_organization (org_id, org_name) VALUES
('example.com', 'Example Healthcare Organization')
ON CONFLICT (org_id) DO NOTHING;

-- ===============================
-- VIEWS FOR ANALYTICS
-- ===============================

-- View: Current Plans with Costs
CREATE OR REPLACE VIEW v_current_plans_analysis AS
SELECT 
    dp.plan_id,
    dpt.plan_type_name,
    do.org_name,
    fpc.deductible,
    fpc.copay,
    fpc.total_cost_shares,
    dd.date_value as creation_date,
    dd.year,
    dd.month_name
FROM fact_plan_costs fpc
JOIN dim_plan dp ON fpc.plan_key = dp.plan_key
JOIN dim_plan_type dpt ON fpc.plan_type_key = dpt.plan_type_key
JOIN dim_organization do ON fpc.org_key = do.org_key
JOIN dim_date dd ON fpc.creation_date_key = dd.date_key
WHERE dp.is_current = TRUE;

-- View: Service Cost Analysis
CREATE OR REPLACE VIEW v_service_cost_analysis AS
SELECT 
    ds.service_name,
    ds.service_category,
    dpt.plan_type_name,
    COUNT(*) as service_count,
    AVG(fsc.copay) as avg_copay,
    AVG(fsc.deductible) as avg_deductible,
    SUM(fsc.total_service_cost) as total_cost
FROM fact_service_costs fsc
JOIN dim_service ds ON fsc.service_key = ds.service_key
JOIN dim_plan dp ON fsc.plan_key = dp.plan_key
JOIN dim_plan_type dpt ON dp.plan_type_key = dpt.plan_type_key
GROUP BY ds.service_name, ds.service_category, dpt.plan_type_name;

-- View: Monthly Trends
CREATE OR REPLACE VIEW v_monthly_cost_trends AS
SELECT 
    dd.year,
    dd.month,
    dd.month_name,
    dpt.plan_type_name,
    COUNT(DISTINCT dp.plan_id) as plans_count,
    AVG(fpc.deductible) as avg_deductible,
    AVG(fpc.copay) as avg_copay,
    SUM(fpc.total_cost_shares) as total_cost_shares
FROM fact_plan_costs fpc
JOIN dim_plan dp ON fpc.plan_key = dp.plan_key
JOIN dim_plan_type dpt ON dp.plan_type_key = dpt.plan_type_key
JOIN dim_date dd ON fpc.creation_date_key = dd.date_key
GROUP BY dd.year, dd.month, dd.month_name, dpt.plan_type_name
ORDER BY dd.year, dd.month;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO dataeng;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO dataeng;
