/**
 * Kafka Consumer Service
 * Consumes healthcare plan events and performs real-time analytics
 */

const { Kafka } = require('kafkajs');
const logger = require('../src/logger/logger');
const elasticClient = require('../consumer/clients/elastic_search');

// Configure Kafka client
const kafka = new Kafka({
    clientId: 'healthcare-analytics-consumer',
    brokers: ['localhost:9093'],
    retry: {
        initialRetryTime: 300,
        retries: 10
    }
});

const consumer = kafka.consumer({ 
    groupId: 'healthcare-analytics-group',
    sessionTimeout: 30000,
    heartbeatInterval: 3000
});

// Statistics tracking
const stats = {
    plansCreated: 0,
    plansUpdated: 0,
    plansDeleted: 0,
    errorsEncountered: 0,
    lastProcessedTime: null
};

/**
 * Initialize and start Kafka consumer
 */
async function startConsumer() {
    try {
        await consumer.connect();
        logger.info('Kafka consumer connected successfully');
        
        // Subscribe to all plan topics
        await consumer.subscribe({
            topics: ['plan-created', 'plan-updated', 'plan-deleted'],
            fromBeginning: false
        });
        
        // Start consuming messages
        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                try {
                    const event = JSON.parse(message.value.toString());
                    const eventSource = message.headers['event-source']?.toString();
                    
                    logger.info(`Received event from ${topic}:`, {
                        partition,
                        offset: message.offset,
                        key: message.key?.toString(),
                        eventType: event.eventType,
                        source: eventSource
                    });
                    
                    // Process based on topic
                    switch (topic) {
                        case 'plan-created':
                            await handlePlanCreated(event);
                            stats.plansCreated++;
                            break;
                        case 'plan-updated':
                            await handlePlanUpdated(event);
                            stats.plansUpdated++;
                            break;
                        case 'plan-deleted':
                            await handlePlanDeleted(event);
                            stats.plansDeleted++;
                            break;
                        default:
                            logger.warn(`Unknown topic: ${topic}`);
                    }
                    
                    stats.lastProcessedTime = new Date().toISOString();
                    
                } catch (error) {
                    logger.error(`Error processing message from ${topic}:`, error);
                    stats.errorsEncountered++;
                }
            }
        });
        
        logger.info('Kafka consumer started and listening for events...');
        
    } catch (error) {
        logger.error('Failed to start Kafka consumer:', error);
        throw error;
    }
}

/**
 * Handle plan created event
 */
async function handlePlanCreated(event) {
    logger.info(`Processing PLAN_CREATED for plan ${event.plan.objectId}`);
    
    // Index to Elasticsearch
    await indexPlanToElasticsearch(event.plan);
    
    // Perform real-time analytics
    await updateRealTimeMetrics(event.plan, 'create');
    
    // Trigger any downstream processes
    await triggerDataQualityCheck(event.plan);
}

/**
 * Handle plan updated event
 */
async function handlePlanUpdated(event) {
    logger.info(`Processing PLAN_UPDATED for plan ${event.plan.objectId}`);
    
    // Update Elasticsearch index
    await indexPlanToElasticsearch(event.plan);
    
    // Update analytics
    await updateRealTimeMetrics(event.plan, 'update');
}

/**
 * Handle plan deleted event
 */
async function handlePlanDeleted(event) {
    logger.info(`Processing PLAN_DELETED for plan ${event.planId}`);
    
    // Remove from Elasticsearch
    await deleteFromElasticsearch(event.plan);
    
    // Update metrics
    await updateRealTimeMetrics(event.plan, 'delete');
}

/**
 * Index plan to Elasticsearch
 */
async function indexPlanToElasticsearch(plan) {
    try {
        // This will use the existing Elasticsearch indexing logic
        // from the consumer service
        logger.info(`Indexing plan ${plan.objectId} to Elasticsearch`);
        
        // Index parent document
        await elasticClient.index({
            index: 'planindex',
            id: plan.objectId,
            document: {
                plan_type: plan.planType,
                creation_date: plan.creationDate,
                organization: plan._org,
                deductible: plan.planCostShares?.deductible,
                copay: plan.planCostShares?.copay,
                services_count: plan.linkedPlanServices?.length || 0,
                indexed_at: new Date().toISOString()
            }
        });
        
        // Index child documents (services)
        if (plan.linkedPlanServices && plan.linkedPlanServices.length > 0) {
            for (const service of plan.linkedPlanServices) {
                if (service.linkedService) {
                    await elasticClient.index({
                        index: 'planindex',
                        document: {
                            join_field: {
                                name: 'planservice',
                                parent: plan.objectId
                            },
                            service_name: service.linkedService.name,
                            service_id: service.linkedService.objectId,
                            service_copay: service.planserviceCostShares?.copay,
                            service_deductible: service.planserviceCostShares?.deductible
                        },
                        routing: plan.objectId
                    });
                }
            }
        }
        
        logger.info(`Successfully indexed plan ${plan.objectId}`);
    } catch (error) {
        logger.error(`Error indexing plan to Elasticsearch:`, error);
        throw error;
    }
}

/**
 * Delete plan from Elasticsearch
 */
async function deleteFromElasticsearch(plan) {
    try {
        await elasticClient.delete({
            index: 'planindex',
            id: plan.objectId
        });
        logger.info(`Deleted plan ${plan.objectId} from Elasticsearch`);
    } catch (error) {
        if (error.meta?.statusCode !== 404) {
            logger.error(`Error deleting from Elasticsearch:`, error);
        }
    }
}

/**
 * Update real-time metrics
 */
async function updateRealTimeMetrics(plan, operation) {
    try {
        // This would typically update a real-time dashboard or metrics store
        // For now, just log the metrics
        const metrics = {
            operation,
            planType: plan.planType,
            organization: plan._org,
            totalCost: (plan.planCostShares?.deductible || 0) + (plan.planCostShares?.copay || 0),
            servicesCount: plan.linkedPlanServices?.length || 0,
            timestamp: new Date().toISOString()
        };
        
        logger.info('Real-time metrics updated:', metrics);
        
        // Could publish to another Kafka topic for monitoring dashboards
        // await publishToMonitoringTopic(metrics);
        
    } catch (error) {
        logger.error('Error updating real-time metrics:', error);
    }
}

/**
 * Trigger data quality check
 */
async function triggerDataQualityCheck(plan) {
    try {
        const qualityIssues = [];
        
        // Check for missing required fields
        if (!plan.objectId) qualityIssues.push('Missing objectId');
        if (!plan.planType) qualityIssues.push('Missing planType');
        if (!plan.creationDate) qualityIssues.push('Missing creationDate');
        
        // Check for reasonable cost values
        const totalCost = (plan.planCostShares?.deductible || 0) + (plan.planCostShares?.copay || 0);
        if (totalCost < 0) qualityIssues.push('Negative cost values');
        if (totalCost > 100000) qualityIssues.push('Unusually high cost values');
        
        // Check for services
        if (!plan.linkedPlanServices || plan.linkedPlanServices.length === 0) {
            qualityIssues.push('No linked services');
        }
        
        if (qualityIssues.length > 0) {
            logger.warn(`Data quality issues found for plan ${plan.objectId}:`, qualityIssues);
        } else {
            logger.info(`Data quality check passed for plan ${plan.objectId}`);
        }
        
    } catch (error) {
        logger.error('Error in data quality check:', error);
    }
}

/**
 * Get consumer statistics
 */
function getStats() {
    return {
        ...stats,
        uptime: process.uptime()
    };
}

/**
 * Graceful shutdown
 */
async function stopConsumer() {
    try {
        await consumer.disconnect();
        logger.info('Kafka consumer disconnected');
        logger.info('Final statistics:', stats);
    } catch (error) {
        logger.error('Error disconnecting Kafka consumer:', error);
    }
}

// Handle process termination
process.on('SIGTERM', stopConsumer);
process.on('SIGINT', stopConsumer);

module.exports = {
    startConsumer,
    stopConsumer,
    getStats
};
