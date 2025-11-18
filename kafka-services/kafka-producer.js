/**
 * Kafka Producer Service
 * Publishes healthcare plan events to Kafka topics for stream processing
 */

const { Kafka } = require('kafkajs');
const logger = require('../src/logger/logger');

// Configure Kafka client
const kafka = new Kafka({
    clientId: 'healthcare-plan-api',
    brokers: ['localhost:9093'],
    retry: {
        initialRetryTime: 300,
        retries: 10
    }
});

const producer = kafka.producer();

// Topics
const TOPICS = {
    PLAN_CREATED: 'plan-created',
    PLAN_UPDATED: 'plan-updated',
    PLAN_DELETED: 'plan-deleted',
    PLAN_ANALYTICS: 'plan-analytics'
};

/**
 * Initialize Kafka producer
 */
async function initializeProducer() {
    try {
        await producer.connect();
        logger.info('Kafka producer connected successfully');
        
        // Create topics if they don't exist
        const admin = kafka.admin();
        await admin.connect();
        
        const existingTopics = await admin.listTopics();
        const topicsToCreate = Object.values(TOPICS).filter(
            topic => !existingTopics.includes(topic)
        );
        
        if (topicsToCreate.length > 0) {
            await admin.createTopics({
                topics: topicsToCreate.map(topic => ({
                    topic,
                    numPartitions: 3,
                    replicationFactor: 1
                }))
            });
            logger.info(`Created Kafka topics: ${topicsToCreate.join(', ')}`);
        }
        
        await admin.disconnect();
    } catch (error) {
        logger.error('Failed to initialize Kafka producer:', error);
        throw error;
    }
}

/**
 * Publish plan created event
 */
async function publishPlanCreated(plan) {
    return publishEvent(TOPICS.PLAN_CREATED, plan.objectId, {
        eventType: 'PLAN_CREATED',
        timestamp: new Date().toISOString(),
        plan: plan
    });
}

/**
 * Publish plan updated event
 */
async function publishPlanUpdated(plan) {
    return publishEvent(TOPICS.PLAN_UPDATED, plan.objectId, {
        eventType: 'PLAN_UPDATED',
        timestamp: new Date().toISOString(),
        plan: plan
    });
}

/**
 * Publish plan deleted event
 */
async function publishPlanDeleted(planId, plan) {
    return publishEvent(TOPICS.PLAN_DELETED, planId, {
        eventType: 'PLAN_DELETED',
        timestamp: new Date().toISOString(),
        planId: planId,
        plan: plan
    });
}

/**
 * Generic event publisher
 */
async function publishEvent(topic, key, value) {
    try {
        const message = {
            key: key,
            value: JSON.stringify(value),
            headers: {
                'event-source': 'healthcare-plan-api',
                'event-time': new Date().toISOString()
            }
        };
        
        const result = await producer.send({
            topic: topic,
            messages: [message]
        });
        
        logger.info(`Published event to ${topic}:`, {
            partition: result[0].partition,
            offset: result[0].offset
        });
        
        return result;
    } catch (error) {
        logger.error(`Failed to publish event to ${topic}:`, error);
        throw error;
    }
}

/**
 * Graceful shutdown
 */
async function disconnectProducer() {
    try {
        await producer.disconnect();
        logger.info('Kafka producer disconnected');
    } catch (error) {
        logger.error('Error disconnecting Kafka producer:', error);
    }
}

// Handle process termination
process.on('SIGTERM', disconnectProducer);
process.on('SIGINT', disconnectProducer);

module.exports = {
    initializeProducer,
    publishPlanCreated,
    publishPlanUpdated,
    publishPlanDeleted,
    publishEvent,
    disconnectProducer,
    TOPICS
};
