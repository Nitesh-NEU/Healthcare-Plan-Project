const express = require('express');
const authController = require('../controllers/auth.controller');
const authRouter = express.Router();

authRouter.get('/validate', authController.validateToken);

module.exports = {
    authRouter
}