const { Pool } = require('pg');

const pool = new Pool({
    host: 'localhost',
    port: 5432,
    database: 'healthcare_dw',
    user: 'dataeng',
    password: 'dataeng123',
});

async function testConnection() {
    try {
        const result = await pool.query('SELECT NOW()');
        console.log('✅ PostgreSQL connection successful!');
        console.log('Current time:', result.rows[0].now);
        await pool.end();
    } catch (error) {
        console.error('❌ PostgreSQL connection failed:', error.message);
        process.exit(1);
    }
}

testConnection();
