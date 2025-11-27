/**
 * Change Data Capture (CDC) ETL Watcher
 * Monitors MongoDB for new healthcare plans and automatically runs ETL
 */

const mongoose = require('mongoose');
const { spawn } = require('child_process');
const path = require('path');

// MongoDB connection - use a separate connection for CDC watcher
const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/medicalPlans';

// Create a separate mongoose connection specifically for the watcher
const watcherConnection = mongoose.createConnection(MONGO_URI);

// Plan Schema (must match your existing schema)
const planCostSharingSchema = new mongoose.Schema({
    deductible: [{ amount: Number, network: String }],
    copay: [{ amount: Number, service: String }]
}, { _id: false });

const linkedServiceSchema = new mongoose.Schema({
    _org: String,
    name: String,
    linkedService: { type: mongoose.Schema.Types.ObjectId, ref: 'Service' }
}, { _id: false });

const planSchema = new mongoose.Schema({
    planCostShares: planCostSharingSchema,
    linkedPlanServices: [linkedServiceSchema],
    _org: String,
    objectId: String,
    objectType: String,
    planType: String,
    creationDate: String
});

const Plan = watcherConnection.model('Plan', planSchema, 'plans');

// ETL execution tracker
let etlRunning = false;
let etlQueue = [];

/**
 * Execute ETL Pipeline
 */
function runETL() {
    if (etlRunning) {
        console.log('⏳ ETL already running, queuing...');
        etlQueue.push(Date.now());
        return;
    }

    etlRunning = true;
    console.log('\n' + '='.repeat(60));
    console.log('🚀 TRIGGERING ETL PIPELINE');
    console.log('='.repeat(60));
    console.log(`⏰ Timestamp: ${new Date().toISOString()}`);

    const etlScript = path.join(__dirname, 'etl_runner.js');
    const etlProcess = spawn('node', [etlScript], {
        stdio: 'inherit',
        cwd: __dirname
    });

    etlProcess.on('close', (code) => {
        etlRunning = false;
        
        if (code === 0) {
            console.log('✅ ETL completed successfully');
        } else {
            console.log(`❌ ETL failed with code: ${code}`);
        }

        // Process queued ETL runs
        if (etlQueue.length > 0) {
            console.log(`📋 Processing ${etlQueue.length} queued ETL runs...`);
            etlQueue = [];
            setTimeout(runETL, 2000); // Wait 2 seconds before next run
        }
        
        console.log('='.repeat(60) + '\n');
    });

    etlProcess.on('error', (error) => {
        console.error('❌ ETL process error:', error);
        etlRunning = false;
    });
}

/**
 * Setup MongoDB Change Stream
 */
async function setupChangeStream() {
    try {
        console.log('📡 Connecting to MongoDB...');
        // Connection already created above
        await new Promise((resolve, reject) => {
            watcherConnection.once('open', resolve);
            watcherConnection.once('error', reject);
        });
        console.log('✅ Connected to MongoDB');

        console.log('👀 Setting up Change Stream on plans collection...');
        
        // Create change stream on the plans collection
        const changeStream = Plan.watch([
            {
                $match: {
                    operationType: { $in: ['insert', 'update', 'replace'] }
                }
            }
        ], {
            fullDocument: 'updateLookup'
        });

        console.log('\n' + '='.repeat(60));
        console.log('🎯 CDC WATCHER ACTIVE - Monitoring for changes...');
        console.log('='.repeat(60));
        console.log('📊 Watching: plans collection');
        console.log('🔔 Triggers: insert, update, replace operations');
        console.log('🔄 Action: Automatic ETL pipeline execution');
        console.log('='.repeat(60) + '\n');

        // Listen for changes
        changeStream.on('change', (change) => {
            console.log('\n🔔 CHANGE DETECTED!');
            console.log('━'.repeat(60));
            console.log(`📝 Operation: ${change.operationType}`);
            console.log(`🆔 Document ID: ${change.documentKey._id}`);
            
            if (change.fullDocument) {
                console.log(`📋 Plan Type: ${change.fullDocument.planType || 'N/A'}`);
                console.log(`🏥 Organization: ${change.fullDocument._org || 'N/A'}`);
                console.log(`📅 Created: ${change.fullDocument.creationDate || 'N/A'}`);
            }
            
            console.log('━'.repeat(60));
            
            // Trigger ETL pipeline
            runETL();
        });

        changeStream.on('error', (error) => {
            console.error('❌ Change stream error:', error);
            // Attempt to reconnect
            setTimeout(() => {
                console.log('🔄 Attempting to reconnect...');
                setupChangeStream();
            }, 5000);
        });

        // Run initial ETL on startup
        console.log('🚀 Running initial ETL pipeline...');
        runETL();

    } catch (error) {
        console.error('❌ Failed to setup change stream:', error);
        process.exit(1);
    }
}

/**
 * Graceful shutdown
 */
let shuttingDown = false;

process.on('SIGINT', async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log('\n\n📴 Shutting down CDC watcher...');
    await watcherConnection.close();
    console.log('✅ Disconnected from MongoDB');
    process.exit(0);
});

process.on('SIGTERM', async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log('\n\n📴 Shutting down CDC watcher...');
    await watcherConnection.close();
    console.log('✅ Disconnected from MongoDB');
    process.exit(0);
});

// Prevent uncaught errors from killing the watcher
process.on('uncaughtException', (error) => {
    console.error('❌ Uncaught exception:', error);
    console.log('🔄 CDC watcher continuing...');
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('❌ Unhandled rejection:', reason);
    console.log('🔄 CDC watcher continuing...');
});

// Start the watcher
console.log('\n' + '█'.repeat(60));
console.log('🔍 CHANGE DATA CAPTURE (CDC) ETL WATCHER');
console.log('█'.repeat(60) + '\n');

setupChangeStream().catch(error => {
    console.error('❌ Fatal error:', error);
    process.exit(1);
});
