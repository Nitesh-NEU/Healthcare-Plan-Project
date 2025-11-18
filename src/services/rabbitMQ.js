const amqp = require('amqplib');
const logger = require("../logger/logger");

async function publishToQueue(data) {
  let connection;
  let channel;
  try {
    const connectionOptions = {
      hostname: 'localhost',
      port: 5672,
      frameMax: 0,
      heartbeat: 30
    };
    
    connection = await amqp.connect(connectionOptions);
    channel = await connection.createChannel();
    const queue = 'plan_mq';
    await channel.assertQueue(queue, { durable: true });
    channel.sendToQueue(queue, Buffer.from(JSON.stringify(data)), {
        persistent: true,
    });
    
    await channel.close();
    await connection.close();
    logger.info("Successfully published message to RabbitMQ");
  } catch (error) {
    console.error(error);
    logger.error("Unable to publish to RMQ", error);
    
    // Cleanup on error
    try {
      if (channel) await channel.close();
      if (connection) await connection.close();
    } catch (cleanupError) {
      logger.error("Error during cleanup", cleanupError);
    }
    
    throw error; // Re-throw to let caller know it failed
  }
}

module.exports = { 
    publishToQueue:publishToQueue
};