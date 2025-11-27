/**
 * Test CDC - Add New Healthcare Plan
 * This will trigger the CDC watcher and auto-run ETL
 */

const axios = require('axios');

const newPlan = {
    "planCostShares": {
        "deductible": [
            { "amount": 3000, "network": "inNetwork" }
        ],
        "copay": [
            { "amount": 50, "service": "Primary Care" },
            { "amount": 100, "service": "Specialist" }
        ]
    },
    "linkedPlanServices": [
        {
            "_org": "example.com",
            "name": "Cardiology",
            "linkedService": "507f1f77bcf86cd799439011"
        },
        {
            "_org": "example.com",
            "name": "Radiology",
            "linkedService": "507f1f77bcf86cd799439012"
        }
    ],
    "_org": "healthcorp.com",
    "objectId": "new-plan-001",
    "objectType": "plan",
    "planType": "outOfNetwork",
    "creationDate": "2025-01-15"
};

async function addPlan() {
    try {
        console.log('\n' + '='.repeat(60));
        console.log('📝 ADDING NEW HEALTHCARE PLAN TO MONGODB');
        console.log('='.repeat(60));
        console.log(`📋 Plan Type: ${newPlan.planType}`);
        console.log(`🏥 Organization: ${newPlan._org}`);
        console.log(`📅 Creation Date: ${newPlan.creationDate}`);
        console.log(`💰 Deductible: $${newPlan.planCostShares.deductible[0].amount}`);
        console.log('='.repeat(60));
        
        const response = await axios.post('http://localhost:3000/plan', newPlan, {
            headers: { 'Content-Type': 'application/json' }
        });
        
        console.log('\n✅ Plan added successfully!');
        console.log(`🆔 Plan ID: ${response.data.objectId || response.data._id}`);
        console.log('\n👀 Watch the CDC watcher terminal - ETL should trigger automatically!');
        console.log('='.repeat(60) + '\n');
        
    } catch (error) {
        if (error.response) {
            console.error('❌ API Error:', error.response.status, error.response.data);
        } else if (error.request) {
            console.error('❌ No response from server. Is the REST API running on port 3000?');
            console.error('   Start it with: node server.js');
        } else {
            console.error('❌ Error:', error.message);
        }
    }
}

addPlan();
