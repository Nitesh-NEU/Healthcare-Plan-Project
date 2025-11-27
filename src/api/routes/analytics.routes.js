/**
 * Analytics Routes
 * Routes for data analytics and insights
 */

const express = require('express');
const router = express.Router();
const analyticsController = require('../controllers/analytics.controller');
const { tokenValidator } = require('../middleware/validators');

// Middleware to validate OAuth token (commented out for testing)
// Uncomment the line below to enable OAuth authentication
// router.use(tokenValidator);

// Cost analytics
router.get('/cost-trends', analyticsController.getCostTrends);
router.get('/monthly-trends', analyticsController.getMonthlyTrends);

// Service analytics
router.get('/service-analysis', analyticsController.getServiceAnalysis);
router.get('/top-services', analyticsController.getTopServices);

// Metrics and summary
router.get('/plan-metrics', analyticsController.getPlanMetrics);
router.get('/dashboard', analyticsController.getDashboardMetrics);

// Spark analytics results
router.get('/spark-analytics', analyticsController.getSparkAnalytics);

// Data engineering monitoring
router.get('/etl-audit', analyticsController.getETLAuditLogs);
router.get('/data-quality', analyticsController.getDataQualityChecks);

module.exports = router;
