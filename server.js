require('dotenv').config()
const app = require('./src/app');
const express = require('express')
const logger = require('./src/logger/logger');
app.use(express.json())
const PORT = process.env.APP_PORT || '8080'

app.listen(PORT, () => {
    logger.info(`Started the server on the PORT::${PORT}`)
});