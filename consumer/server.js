const http = require('http');
const config = require('./utils/config');
const rabbitmq = require("./clients/rabbit_mq")

const server = http.createServer();

rabbitmq.connectQueue()
.then(()=>{
    console.log("Connected to RabbitMQ");
})
.catch((error)=>{
    console.log("Error connecting to rabbitMQ: ", error)
})

server.listen(config.PORT, () => {
  console.log(`Server running on port ${config.PORT}`);
});