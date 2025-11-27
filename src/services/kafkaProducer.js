/**
 * Kafka Producer Service
 * Publishes healthcare plan events to Kafka for real-time streaming
 */

const { Kafka, Partitioners } = require('kafkajs');

// Kafka configuration
const kafka = new Kafka({
    clientId: 'healthcare-api',
    brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
    retry: {
        retries: 5,
        initialRetryTime: 300
    }
});

const producer = kafka.producer({
    createPartitioner: Partitioners.LegacyPartitioner
});

// Event types
const EVENT_TYPES = {
    PLAN_CREATED: 'plan.created',
    PLAN_UPDATED: 'plan.updated',
    PLAN_DELETED: 'plan.deleted',
    SERVICE_LINKED: 'service.linked',
    SERVICE_UNLINKED: 'service.unlinked'
};

// Topics
const TOPICS = {
    PLANS: 'healthcare.plans',
    SERVICES: 'healthcare.services',
    ANALYTICS: 'healthcare.analytics'
};

let isConnected = false;

/**
 * Initialize Kafka producer connection
 */
async function connect() {
    if (isConnected) return;
    
    try {
        await producer.connect();
        isConnected = true;
        console.log('✅ Kafka producer connected');
    } catch (error) {
        console.error('❌ Kafka producer connection failed:', error);
        throw error;
    }
}

/**
 * Publish event to Kafka
 */
async function publishEvent(topic, event) {
    try {
        if (!isConnected) {
            await connect();
        }

        const message = {
            key: event.entityId || event.planId || event.objectId,
            value: JSON.stringify({
                ...event,
                timestamp: new Date().toISOString(),
                source: 'healthcare-api'
            }),
            headers: {
                'event-type': event.eventType,
                'correlation-id': event.correlationId || `evt-${Date.now()}`
            }
        };

        await producer.send({
            topic,
            messages: [message]
        });

        console.log(`📤 Published to Kafka: ${event.eventType} → ${topic}`);
        return true;
    } catch (error) {
        console.error(`❌ Failed to publish to Kafka:`, error);
        return false;
    }
}

/**
 * Publish plan created event
 */
async function publishPlanCreated(plan) {
    const event = {
        eventType: EVENT_TYPES.PLAN_CREATED,
        entityId: plan.objectId || plan._id,
        planId: plan.objectId,
        planType: plan.planType,
        organization: plan._org,
        creationDate: plan.creationDate,
        planCostShares: plan.planCostShares,
        servicesCount: plan.linkedPlanServices?.length || 0,
        metadata: {
            createdAt: new Date().toISOString()
        }
    };

    return await publishEvent(TOPICS.PLANS, event);
}

/**
 * Publish plan updated event
 */
async function publishPlanUpdated(planId, updates) {
    const event = {
        eventType: EVENT_TYPES.PLAN_UPDATED,
        entityId: planId,
        planId: planId,
        updates: updates,
        metadata: {
            updatedAt: new Date().toISOString()
        }
    };

    return await publishEvent(TOPICS.PLANS, event);
}

/**
 * Publish plan deleted event
 */
async function publishPlanDeleted(planId) {
    const event = {
        eventType: EVENT_TYPES.PLAN_DELETED,
        entityId: planId,
        planId: planId,
        metadata: {
            deletedAt: new Date().toISOString()
        }
    };

    return await publishEvent(TOPICS.PLANS, event);
}

/**
 * Publish analytics event (for metrics, aggregations)
 */
async function publishAnalyticsEvent(eventData) {
    const event = {
        eventType: 'analytics.event',
        ...eventData,
        metadata: {
            timestamp: new Date().toISOString()
        }
    };

    return await publishEvent(TOPICS.ANALYTICS, event);
}

/**
 * Graceful shutdown
 */
async function disconnect() {
    if (!isConnected) return;
    
    try {
        await producer.disconnect();
        isConnected = false;
        console.log('✅ Kafka producer disconnected');
    } catch (error) {
        console.error('❌ Kafka disconnect error:', error);
    }
}

// Handle process termination
process.on('SIGINT', disconnect);
process.on('SIGTERM', disconnect);

module.exports = {
    connect,
    disconnect,
    publishPlanCreated,
    publishPlanUpdated,
    publishPlanDeleted,
    publishAnalyticsEvent,
    publishEvent,
    EVENT_TYPES,
    TOPICS
};
