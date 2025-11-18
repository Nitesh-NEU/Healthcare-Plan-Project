const express = require('express');
const healthzController = require('../controllers/healthz.controller');
const healthzRouter = express.Router();

healthzRouter.get('/', healthzController.ping);

module.exports = healthzRouter;
