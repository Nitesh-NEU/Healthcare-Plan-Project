/**
 * Analytics Controller
 * Provides data analytics and insights endpoints
 */

const logger = require('../../logger/logger');
const { MongoClient } = require('mongodb');
const { Pool } = require('pg');

// PostgreSQL connection pool
const pgPool = new Pool({
    host: 'localhost',
    port: 5432,
    database: 'healthcare_dw',
    user: 'dataeng',
    password: 'dataeng123',
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
});

// MongoDB connection
const mongoUri = 'mongodb://localhost:27017';
const mongoClient = new MongoClient(mongoUri);

/**
 * Get cost trends by plan type
 */
const getCostTrends = async (req, res) => {
    logger.info(`Handling ${req.method} for ${req.originalUrl}`);
    
    try {
        const result = await pgPool.query(`
            SELECT * FROM v_current_plans_analysis
            ORDER BY creation_date DESC
            LIMIT 100
        `);
        
        res.status(200).json({
            success: true,
            count: result.rows.length,
            data: result.rows
        });
    } catch (error) {
        logger.error('Error fetching cost trends:', error);
        res.status(500).json({ 
            success: false,
            error: 'Failed to fetch cost trends' 
        });
    }
};

/**
 * Get service cost analysis
 */
const getServiceAnalysis = async (req, res) => {
    logger.info(`Handling ${req.method} for ${req.originalUrl}`);
    
    try {
        const result = await pgPool.query(`
            SELECT * FROM v_service_cost_analysis
            ORDER BY total_cost DESC
        `);
        
        res.status(200).json({
            success: true,
            count: result.rows.length,
            data: result.rows
        });
    } catch (error) {
        logger.error('Error fetching service analysis:', error);
        res.status(500).json({ 
            success: false,
            error: 'Failed to fetch service analysis' 
        });
    }
};

/**
 * Get monthly cost trends
 */
const getMonthlyTrends = async (req, res) => {
    logger.info(`Handling ${req.method} for ${req.originalUrl}`);
    
    const { year, planType } = req.query;
    
    try {
        let query = 'SELECT * FROM v_monthly_cost_trends WHERE 1=1';
        const params = [];
        
        if (year) {
            params.push(year);
            query += ` AND year = $${params.length}`;
        }
        
        if (planType) {
            params.push(planType);
            query += ` AND plan_type_name = $${params.length}`;
        }
        
        query += ' ORDER BY year DESC, month DESC';
        
        const result = await pgPool.query(query, params);
        
        res.status(200).json({
            success: true,
            count: result.rows.length,
            filters: { year, planType },
            data: result.rows
        });
    } catch (error) {
        logger.error('Error fetching monthly trends:', error);
        res.status(500).json({ 
            success: false,
            error: 'Failed to fetch monthly trends' 
        });
    }
};

/**
 * Get plan metrics summary
 */
const getPlanMetrics = async (req, res) => {
    logger.info(`Handling ${req.method} for ${req.originalUrl}`);
    
    try {
        const result = await pgPool.query(`
            SELECT 
                COUNT(DISTINCT dp.plan_id) as total_plans,
                COUNT(DISTINCT dp.plan_type_key) as plan_types_count,
                COUNT(DISTINCT do.org_id) as organizations_count,
                AVG(fpc.deductible) as avg_deductible,
                AVG(fpc.copay) as avg_copay,
                MIN(fpc.total_cost_shares) as min_cost,
                MAX(fpc.total_cost_shares) as max_cost,
                SUM(fpc.total_cost_shares) as total_revenue
            FROM dim_plan dp
            JOIN fact_plan_costs fpc ON dp.plan_key = fpc.plan_key
            JOIN dim_organization do ON fpc.org_key = do.org_key
            WHERE dp.is_current = TRUE
        `);
        
        res.status(200).json({
            success: true,
            data: result.rows[0]
        });
    } catch (error) {
        logger.error('Error fetching plan metrics:', error);
        res.status(500).json({ 
            success: false,
            error: 'Failed to fetch plan metrics' 
        });
    }
};

/**
 * Get top services by cost
 */
const getTopServices = async (req, res) => {
    logger.info(`Handling ${req.method} for ${req.originalUrl}`);
    
    const limit = parseInt(req.query.limit) || 10;
    
    try {
        const result = await pgPool.query(`
            SELECT 
                ds.service_name,
                COUNT(*) as usage_count,
                AVG(fsc.copay) as avg_copay,
                AVG(fsc.deductible) as avg_deductible,
                SUM(fsc.total_service_cost) as total_cost
            FROM fact_service_costs fsc
            JOIN dim_service ds ON fsc.service_key = ds.service_key
            GROUP BY ds.service_name
            ORDER BY total_cost DESC
            LIMIT $1
        `, [limit]);
        
        res.status(200).json({
            success: true,
            count: result.rows.length,
            data: result.rows
        });
    } catch (error) {
        logger.error('Error fetching top services:', error);
        res.status(500).json({ 
            success: false,
            error: 'Failed to fetch top services' 
        });
    }
};

/**
 * Get Spark analytics results from MongoDB
 */
const getSparkAnalytics = async (req, res) => {
    logger.info(`Handling ${req.method} for ${req.originalUrl}`);
    
    const { type } = req.params; // cost_trends, service_patterns, anomalies, monthly_metrics
    
    try {
        await mongoClient.connect();
        const db = mongoClient.db('medicalPlans');
        
        const collectionMap = {
            'cost-trends': 'analytics_cost_trends',
            'service-patterns': 'analytics_service_patterns',
            'anomalies': 'analytics_anomalies',
            'monthly-metrics': 'analytics_monthly_metrics'
        };
        
        const collectionName = collectionMap[type];
        
        if (!collectionName) {
            return res.status(400).json({
                success: false,
                error: 'Invalid analytics type',
                validTypes: Object.keys(collectionMap)
            });
        }
        
        const collection = db.collection(collectionName);
        const data = await collection.find({}).toArray();
        
        res.status(200).json({
            success: true,
            type,
            count: data.length,
            data
        });
    } catch (error) {
        logger.error('Error fetching Spark analytics:', error);
        res.status(500).json({ 
            success: false,
            error: 'Failed to fetch Spark analytics' 
        });
    }
};

/**
 * Get ETL audit logs
 */
const getETLAuditLogs = async (req, res) => {
    logger.info(`Handling ${req.method} for ${req.originalUrl}`);
    
    const { status, limit = 50 } = req.query;
    
    try {
        let query = 'SELECT * FROM etl_audit_log WHERE 1=1';
        const params = [];
        
        if (status) {
            params.push(status.toUpperCase());
            query += ` AND status = $${params.length}`;
        }
        
        params.push(limit);
        query += ` ORDER BY start_time DESC LIMIT $${params.length}`;
        
        const result = await pgPool.query(query, params);
        
        res.status(200).json({
            success: true,
            count: result.rows.length,
            data: result.rows
        });
    } catch (error) {
        logger.error('Error fetching ETL logs:', error);
        res.status(500).json({ 
            success: false,
            error: 'Failed to fetch ETL logs' 
        });
    }
};

/**
 * Get data quality check results
 */
const getDataQualityChecks = async (req, res) => {
    logger.info(`Handling ${req.method} for ${req.originalUrl}`);
    
    const { status, tableName } = req.query;
    
    try {
        let query = 'SELECT * FROM data_quality_checks WHERE 1=1';
        const params = [];
        
        if (status) {
            params.push(status.toUpperCase());
            query += ` AND status = $${params.length}`;
        }
        
        if (tableName) {
            params.push(tableName);
            query += ` AND table_name = $${params.length}`;
        }
        
        query += ' ORDER BY execution_time DESC LIMIT 100';
        
        const result = await pgPool.query(query, params);
        
        res.status(200).json({
            success: true,
            count: result.rows.length,
            data: result.rows
        });
    } catch (error) {
        logger.error('Error fetching data quality checks:', error);
        res.status(500).json({ 
            success: false,
            error: 'Failed to fetch data quality checks' 
        });
    }
};

/**
 * Get real-time dashboard metrics
 */
const getDashboardMetrics = async (req, res) => {
    logger.info(`Handling ${req.method} for ${req.originalUrl}`);
    
    try {
        // Get metrics from multiple sources
        const [plansMetrics, servicesMetrics, recentActivity, qualityMetrics] = await Promise.all([
            // Plans metrics from PostgreSQL
            pgPool.query(`
                SELECT 
                    COUNT(DISTINCT plan_id) as total_plans,
                    AVG(deductible) as avg_deductible,
                    AVG(copay) as avg_copay
                FROM dim_plan dp
                JOIN fact_plan_costs fpc ON dp.plan_key = fpc.plan_key
                WHERE dp.is_current = TRUE
            `),
            
            // Services metrics
            pgPool.query(`
                SELECT 
                    COUNT(DISTINCT service_id) as total_services,
                    COUNT(*) as total_service_linkages
                FROM dim_service
            `),
            
            // Recent ETL activity
            pgPool.query(`
                SELECT job_name, status, end_time, records_processed
                FROM etl_audit_log
                ORDER BY start_time DESC
                LIMIT 5
            `),
            
            // Data quality summary
            pgPool.query(`
                SELECT 
                    status,
                    COUNT(*) as count
                FROM data_quality_checks
                WHERE execution_time > NOW() - INTERVAL '24 hours'
                GROUP BY status
            `)
        ]);
        
        res.status(200).json({
            success: true,
            timestamp: new Date().toISOString(),
            metrics: {
                plans: plansMetrics.rows[0],
                services: servicesMetrics.rows[0],
                recentActivity: recentActivity.rows,
                dataQuality: qualityMetrics.rows
            }
        });
    } catch (error) {
        logger.error('Error fetching dashboard metrics:', error);
        res.status(500).json({ 
            success: false,
            error: 'Failed to fetch dashboard metrics' 
        });
    }
};

module.exports = {
    getCostTrends,
    getServiceAnalysis,
    getMonthlyTrends,
    getPlanMetrics,
    getTopServices,
    getSparkAnalytics,
    getETLAuditLogs,
    getDataQualityChecks,
    getDashboardMetrics
};
