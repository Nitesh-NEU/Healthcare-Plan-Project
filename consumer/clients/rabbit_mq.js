const amqp = require("amqplib")
const config = require("../utils/config")
const es_controller = require("./../controllers/es_controller");


var connection, channel;

async function connectQueue()  {
    try {
        connection = await amqp.connect({
            hostname: 'localhost',
            port: 5672,
            frameMax: 0
          })
        channel = await connection.createChannel();
        await channel.assertQueue(config.QUEUE_NAME)      
        channel.consume(config.QUEUE_NAME, processData)
    } catch (error) {
        throw error;
    }
}

const processData = async (data)=>{

    const message = JSON.parse(Buffer.from(data.content))
    console.log(message);
    
    switch(message.type){
        case "PUT":
            await es_controller.createPlanIndex(message.document);
            break;
        case "DELETE":
            await es_controller.deletePlanIndex(message.document);
            break;
        default:
            console.log("Message type unsupported");
    }    
    channel.ack(data);
    
}

const getChannel = () => {
    return channel; 
}

const getConnection = () => {
    return connection;
}

module.exports = { connectQueue: connectQueue, getChannel: getChannel, getConnection: getConnection }