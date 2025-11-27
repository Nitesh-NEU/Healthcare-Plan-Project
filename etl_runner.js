/**
 * ETL Pipeline - MongoDB to PostgreSQL
 * Extracts data from MongoDB and loads into PostgreSQL warehouse
 */

const mongoose = require('mongoose');
const { Pool } = require('pg');
const Plan = require('./src/api/models/plan');
require('dotenv').config();

// PostgreSQL connection
const pgPool = new Pool({
    host: 'localhost',
    port: 5433,  // Docker port mapping
    database: 'healthcare_dw',
    user: 'dataeng',
    password: 'dataeng123',
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000,
});

const stats = {
    plansExtracted: 0,
    plansLoaded: 0,
    servicesLoaded: 0,
    errors: 0,
    startTime: null,
    endTime: null
};

/**
 * Get or create date key
 */
async function getOrCreateDateKey(client, dateValue) {
    const date = dateValue instanceof Date ? dateValue : new Date(dateValue);
    const dateStr = date.toISOString().split('T')[0];
    
    // Check if date already exists
    const existing = await client.query(
        'SELECT date_key FROM dim_date WHERE date_value = $1',
        [dateStr]
    );
    
    if (existing.rows.length > 0) {
        return existing.rows[0].date_key;
    }
    
    // Generate date_key as YYYYMMDD integer
    const dateKey = parseInt(date.toISOString().replace(/[-:]/g, '').substring(0, 8));
    
    const result = await client.query(`
        INSERT INTO dim_date (
            date_key, date_value, year, quarter, month, month_name, 
            day, day_of_week, day_name, week_of_year, is_weekend
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (date_value) DO NOTHING
        RETURNING date_key
    `, [
        dateKey,
        dateStr,
        date.getFullYear(),
        Math.floor(date.getMonth() / 3) + 1,
        date.getMonth() + 1,
        date.toLocaleString('en-US', { month: 'long' }),
        date.getDate(),
        date.getDay(),  // 0-6 where 0 is Sunday
        date.toLocaleString('en-US', { weekday: 'long' }),
        getWeekNumber(date),
        date.getDay() === 0 || date.getDay() === 6
    ]);
    
    if (result.rows.length > 0) {
        return result.rows[0].date_key;
    }
    
    const existingResult = await client.query(
        'SELECT date_key FROM dim_date WHERE date_value = $1',
        [dateStr]
    );
    return existingResult.rows[0].date_key;
}

/**
 * Get ISO week number
 */
function getWeekNumber(date) {
    const d = new Date(date);
    d.setHours(0, 0, 0, 0);
    d.setDate(d.getDate() + 4 - (d.getDay() || 7));
    const yearStart = new Date(d.getFullYear(), 0, 1);
    return Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
}

/**
 * Load dimension tables and return keys
 */
async function loadDimensions(client, plan) {
    const keys = {};
    
    // Organization dimension
    const orgId = plan._org || 'unknown';
    const orgName = plan._org || 'Unknown Organization';
    
    const orgResult = await client.query(`
        INSERT INTO dim_organization (org_id, org_name)
        VALUES ($1, $2)
        ON CONFLICT (org_id) DO UPDATE SET org_name = EXCLUDED.org_name
        RETURNING org_key
    `, [orgId, orgName]);
    keys.org_key = orgResult.rows[0].org_key;
    
    // Plan Type dimension
    const planType = plan.planType || 'unknown';
    const planTypeCode = planType.toLowerCase().replace(/\s+/g, '_');
    
    // Check if plan type exists
    const existingPlanType = await client.query(
        'SELECT plan_type_key FROM dim_plan_type WHERE plan_type_code = $1',
        [planTypeCode]
    );
    
    if (existingPlanType.rows.length > 0) {
        keys.plan_type_key = existingPlanType.rows[0].plan_type_key;
    } else {
        let planTypeResult = await client.query(`
            INSERT INTO dim_plan_type (plan_type_code, plan_type_name)
            VALUES ($1, $2)
            RETURNING plan_type_key
        `, [planTypeCode, planType]);
        keys.plan_type_key = planTypeResult.rows[0].plan_type_key;
    }
    
    // Date dimension
    const creationDate = plan.creationDate || new Date();
    keys.creation_date_key = await getOrCreateDateKey(client, creationDate);
    
    // Plan dimension
    const planId = plan.objectId || plan._id.toString();
    
    // Check if plan already exists
    const existingPlan = await client.query(
        'SELECT plan_key FROM dim_plan WHERE plan_id = $1',
        [planId]
    );
    
    if (existingPlan.rows.length > 0) {
        keys.plan_key = existingPlan.rows[0].plan_key;
    } else {
        const planResult = await client.query(`
            INSERT INTO dim_plan (plan_id, plan_type_key, org_key, is_current, effective_date, expiration_date)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING plan_key
        `, [
            planId,
            keys.plan_type_key,
            keys.org_key,
            true,
            creationDate,
            null
        ]);
        keys.plan_key = planResult.rows[0].plan_key;
    }
    
    return keys;
}

/**
 * Load fact tables for plan costs and metrics
 */
async function loadPlanFacts(client, plan, keys) {
    const deductible = plan.planCostShares?.deductible || 0;
    const copay = plan.planCostShares?.copay || 0;
    const linkedServices = plan.linkedPlanServices || [];
    const servicesCount = Array.isArray(linkedServices) ? linkedServices.length : 0;
    const totalCostShares = deductible + copay;
    
    // Insert into fact_plan_costs
    await client.query(`
        INSERT INTO fact_plan_costs (
            plan_key, plan_type_key, org_key, creation_date_key,
            deductible, copay, total_cost_shares
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
    `, [
        keys.plan_key,
        keys.plan_type_key,
        keys.org_key,
        keys.creation_date_key,
        deductible,
        copay,
        totalCostShares
    ]);
    
    // Skip fact_plan_metrics - it's for aggregated snapshots, not individual plans
}

/**
 * Load service-level facts
 */
async function loadServiceFacts(client, plan, keys) {
    const linkedServices = plan.linkedPlanServices || [];
    
    if (!Array.isArray(linkedServices)) {
        return;
    }
    
    for (const serviceData of linkedServices) {
        const serviceName = serviceData.name || 'Unknown Service';
        const serviceId = serviceData.objectId || serviceName.toLowerCase().replace(/\s+/g, '_');
        
        // Check if service exists
        const existingService = await client.query(
            'SELECT service_key FROM dim_service WHERE service_id = $1',
            [serviceId]
        );
        
        let serviceKey;
        if (existingService.rows.length > 0) {
            serviceKey = existingService.rows[0].service_key;
        } else {
            // Insert service dimension
            const serviceResult = await client.query(`
                INSERT INTO dim_service (service_id, service_name, service_category)
                VALUES ($1, $2, $3)
                RETURNING service_key
            `, [serviceId, serviceName, 'Healthcare Service']);
            serviceKey = serviceResult.rows[0].service_key;
        }
        
        // Get service cost shares
        const costShares = serviceData.linkedService?.planserviceCostShares || {};
        const serviceDeductible = costShares.deductible || 0;
        const serviceCopay = costShares.copay || 0;
        
        // Insert service fact
        await client.query(`
            INSERT INTO fact_service_costs (
                service_key, plan_key, org_key, date_key,
                deductible, copay, total_service_cost
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        `, [
            serviceKey,
            keys.plan_key,
            keys.org_key,
            keys.creation_date_key,
            serviceDeductible,
            serviceCopay,
            serviceDeductible + serviceCopay
        ]);
        
        stats.servicesLoaded++;
    }
}

/**
 * Log ETL execution to audit table
 */
async function logETLRun(client) {
    const executionTime = Math.round((stats.endTime - stats.startTime) / 1000);
    const status = stats.errors === 0 ? 'SUCCESS' : 'PARTIAL_SUCCESS';
    const errorMsg = stats.errors > 0 ? `${stats.errors} plans failed to load` : null;
    
    await client.query(`
        INSERT INTO etl_audit_log (
            job_name, job_type, start_time, end_time, status,
            records_processed, records_inserted, records_updated, records_failed, error_message
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    `, [
        'MongoDB to PostgreSQL ETL',
        'FULL_LOAD',
        new Date(stats.startTime),
        new Date(stats.endTime),
        status,
        stats.plansExtracted,
        stats.plansLoaded,
        0,
        stats.errors,
        errorMsg
    ]);
}

/**
 * Main ETL process
 */
async function runETL() {
    console.log('=' . repeat(60));
    console.log('🏥 Healthcare Data ETL Pipeline');
    console.log('   MongoDB → PostgreSQL Data Warehouse');
    console.log('='.repeat(60));
    console.log('');
    
    stats.startTime = Date.now();
    let client;
    
    try {
        // Connect to MongoDB
        console.log('📡 Connecting to MongoDB...');
        await mongoose.connect(process.env.MONGODB_URI, {
            serverSelectionTimeoutMS: 10000,
            socketTimeoutMS: 45000,
        });
        console.log('✅ Connected to MongoDB');
        
        // Connect to PostgreSQL
        console.log('📡 Connecting to PostgreSQL...');
        client = await pgPool.connect();
        console.log('✅ Connected to PostgreSQL');
        console.log('');
        
        // Extract from MongoDB
        console.log('📥 Extracting data from MongoDB...');
        const plans = await Plan.find({}).lean();
        stats.plansExtracted = plans.length;
        console.log(`   Found ${plans.length} plans`);
        console.log('');
        
        // Transform and Load
        console.log('🔧 Transforming and loading to PostgreSQL...');
        
        for (let i = 0; i < plans.length; i++) {
            const plan = plans[i];
            
            try {
                await client.query('BEGIN');
                
                // Load dimensions
                const keys = await loadDimensions(client, plan);
                
                // Load facts
                await loadPlanFacts(client, plan, keys);
                await loadServiceFacts(client, plan, keys);
                
                await client.query('COMMIT');
                stats.plansLoaded++;
                
                if ((i + 1) % 10 === 0) {
                    console.log(`   Processed ${i + 1}/${plans.length} plans...`);
                }
                
            } catch (error) {
                await client.query('ROLLBACK');
                console.log(`⚠️  Error processing plan ${plan.objectId || plan._id}: ${error.message}`);
                stats.errors++;
            }
        }
        
        console.log('');
        console.log('📝 Logging ETL run...');
        await logETLRun(client);
        
        stats.endTime = Date.now();
        
        // Print statistics
        console.log('');
        console.log('='.repeat(60));
        console.log('📊 ETL Statistics');
        console.log('='.repeat(60));
        console.log(`Plans extracted:     ${stats.plansExtracted}`);
        console.log(`Plans loaded:        ${stats.plansLoaded}`);
        console.log(`Services loaded:     ${stats.servicesLoaded}`);
        console.log(`Errors:              ${stats.errors}`);
        console.log(`Execution time:      ${Math.round((stats.endTime - stats.startTime) / 1000)}s`);
        console.log('='.repeat(60));
        console.log('');
        console.log('✅ ETL completed successfully!');
        console.log('');
        console.log('📊 Next steps:');
        console.log('   1. Access Superset: http://localhost:8088');
        console.log('   2. Login with admin/admin');
        console.log('   3. Connect to database: postgresql://dataeng:dataeng123@localhost:5433/healthcare_dw');
        console.log('   4. Create visualizations from the views');
        
    } catch (error) {
        console.log('');
        console.log(`❌ ETL failed: ${error.message}`);
        console.error(error);
        // Don't exit - let CDC watcher continue running
        return false;
    } finally {
        if (client) {
            client.release();
        }
        await pgPool.end();
        await mongoose.connection.close();
        console.log('🧹 Cleaned up connections');
    }
}

// Run the ETL
runETL();
