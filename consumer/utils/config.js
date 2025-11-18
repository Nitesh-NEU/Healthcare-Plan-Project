require("dotenv").config()

const PORT = process.env.PORT
const QUEUE_NAME = process.env.QUEUE_NAME

module.exports = { PORT, QUEUE_NAME}