/**
 * Kafka Consumer Service
 * Consumes healthcare plan events and processes them in real-time
 * Streams data to Elasticsearch for search capability
 */

const { Kafka } = require('kafkajs');
const { Client } = require('@elastic/elasticsearch');

// Kafka configuration
const kafka = new Kafka({
    clientId: 'healthcare-consumer',
    brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
    retry: {
        retries: 5,
        initialRetryTime: 300
    }
});

// Elasticsearch configuration
const esClient = new Client({
    node: process.env.ELASTICSEARCH_URL || 'http://localhost:9200'
});

const consumer = kafka.consumer({ 
    groupId: 'healthcare-analytics-group',
    sessionTimeout: 30000,
    heartbeatInterval: 3000
});

const TOPICS = {
    PLANS: 'healthcare.plans',
    SERVICES: 'healthcare.services',
    ANALYTICS: 'healthcare.analytics'
};

/**
 * Initialize Elasticsearch index for plans
 */
async function setupElasticsearchIndex() {
    try {
        const indexExists = await esClient.indices.exists({ index: 'healthcare-plans' });
        
        if (!indexExists) {
            await esClient.indices.create({
                index: 'healthcare-plans',
                body: {
                    mappings: {
                        properties: {
                            planId: { type: 'keyword' },
                            planType: { type: 'keyword' },
                            organization: { type: 'keyword' },
                            eventType: { type: 'keyword' },
                            timestamp: { type: 'date' },
                            creationDate: { type: 'date' },
                            deductible: { type: 'float' },
                            copay: { type: 'float' },
                            servicesCount: { type: 'integer' },
                            planCostShares: { type: 'object' },
                            metadata: { type: 'object' }
                        }
                    }
                }
            });
            console.log('✅ Created Elasticsearch index: healthcare-plans');
        }
    } catch (error) {
        console.error('❌ Failed to setup Elasticsearch index:', error);
    }
}

/**
 * Process plan created event
 */
async function processPlanCreated(message) {
    try {
        const plan = message.value;
        
        // Index to Elasticsearch for search
        await esClient.index({
            index: 'healthcare-plans',
            id: plan.planId || plan.entityId,
            body: {
                ...plan,
                indexed_at: new Date().toISOString()
            }
        });
        
        console.log(`✅ Indexed plan to Elasticsearch: ${plan.planId}`);
        
        // Additional processing: analytics, notifications, etc.
        await updateAnalyticsDashboard(plan);
        
    } catch (error) {
        console.error('❌ Error processing plan created event:', error);
    }
}

/**
 * Process plan updated event
 */
async function processPlanUpdated(message) {
    try {
        const update = message.value;
        
        // Update in Elasticsearch
        await esClient.update({
            index: 'healthcare-plans',
            id: update.planId || update.entityId,
            body: {
                doc: {
                    ...update.updates,
                    updated_at: new Date().toISOString()
                }
            }
        });
        
        console.log(`✅ Updated plan in Elasticsearch: ${update.planId}`);
        
    } catch (error) {
        console.error('❌ Error processing plan updated event:', error);
    }
}

/**
 * Process plan deleted event
 */
async function processPlanDeleted(message) {
    try {
        const deletion = message.value;
        
        // Delete from Elasticsearch
        await esClient.delete({
            index: 'healthcare-plans',
            id: deletion.planId || deletion.entityId
        });
        
        console.log(`✅ Deleted plan from Elasticsearch: ${deletion.planId}`);
        
    } catch (error) {
        console.error('❌ Error processing plan deleted event:', error);
    }
}

/**
 * Update analytics dashboard metrics
 */
async function updateAnalyticsDashboard(plan) {
    try {
        // Calculate real-time metrics
        const metrics = {
            totalPlans: await esClient.count({ index: 'healthcare-plans' }),
            avgDeductible: plan.planCostShares?.deductible?.[0]?.amount || 0,
            avgCopay: plan.planCostShares?.copay?.[0]?.amount || 0
        };
        
        console.log(`📊 Analytics updated:`, metrics);
        
    } catch (error) {
        console.error('❌ Error updating analytics:', error);
    }
}

/**
 * Message handler - routes events to appropriate processors
 */
async function handleMessage(topic, partition, message) {
    try {
        const event = JSON.parse(message.value.toString());
        const eventType = message.headers['event-type']?.toString() || event.eventType;
        
        console.log(`\n📨 Received Kafka event: ${eventType}`);
        console.log(`   Topic: ${topic}`);
        console.log(`   Partition: ${partition}`);
        console.log(`   Offset: ${message.offset}`);
        
        // Route to appropriate processor
        switch (eventType) {
            case 'plan.created':
                await processPlanCreated({ value: event, headers: message.headers });
                break;
                
            case 'plan.updated':
                await processPlanUpdated({ value: event, headers: message.headers });
                break;
                
            case 'plan.deleted':
                await processPlanDeleted({ value: event, headers: message.headers });
                break;
                
            case 'analytics.event':
                console.log('📊 Analytics event received:', event);
                break;
                
            default:
                console.log(`⚠️  Unknown event type: ${eventType}`);
        }
        
    } catch (error) {
        console.error('❌ Error handling message:', error);
    }
}

/**
 * Start the Kafka consumer
 */
async function startConsumer() {
    try {
        console.log('\n' + '='.repeat(60));
        console.log('🚀 KAFKA CONSUMER STARTING');
        console.log('='.repeat(60) + '\n');
        
        // Setup Elasticsearch
        await setupElasticsearchIndex();
        
        // Connect to Kafka
        await consumer.connect();
        console.log('✅ Connected to Kafka');
        
        // Subscribe to topics
        await consumer.subscribe({ 
            topics: [TOPICS.PLANS, TOPICS.SERVICES, TOPICS.ANALYTICS],
            fromBeginning: false  // Only consume new messages
        });
        console.log('✅ Subscribed to topics:', Object.values(TOPICS).join(', '));
        
        console.log('\n' + '='.repeat(60));
        console.log('👂 CONSUMER ACTIVE - Listening for events...');
        console.log('='.repeat(60) + '\n');
        
        // Start consuming
        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                await handleMessage(topic, partition, message);
            }
        });
        
    } catch (error) {
        console.error('❌ Consumer startup failed:', error);
        process.exit(1);
    }
}

/**
 * Graceful shutdown
 */
async function shutdown() {
    console.log('\n📴 Shutting down consumer...');
    try {
        await consumer.disconnect();
        console.log('✅ Consumer disconnected');
        process.exit(0);
    } catch (error) {
        console.error('❌ Error during shutdown:', error);
        process.exit(1);
    }
}

// Handle process signals
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

// Start consumer if run directly
if (require.main === module) {
    startConsumer();
}

module.exports = {
    startConsumer,
    shutdown
};
