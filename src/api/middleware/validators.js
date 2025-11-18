const logger = require("../../logger/logger")
const planSchema = require("../../schemas/plan.schema.json")
const linkedPlanServicesSchema = require('../../schemas/linkedPlanServices.schema.json')
const validate = require('jsonschema').validate
const authSerivce = require('../../services/auth')


const validatePlan = async (req, res, next) => {
    logger.info("Validating the incoming JSON payload")
    var result = validate(req.body, planSchema)
    if(result.errors.length > 0){
        const formattedErrors = result.errors.map((error) => ({
            errorProperty: error.argument,
            errorMessage: error.message
        }));
        logger.error("Incoming JSONSchema validation failed", {validationErrors: formattedErrors})
        res.status(400).json(formattedErrors)
        return
    }
    next()
}

const validateLinkedPlanServices =  async (req, res, next) => {
    logger.info("Validating the incoming JSON payload")
    var result = validate(req.body, linkedPlanServicesSchema)
    if(result.errors.length > 0){
        const formattedErrors = result.errors.map((error) => ({
            errorProperty: error.argument,
            errorMessage: error.message
        }));
        logger.error("Incoming JSONSchema validation failed", {validationErrors: formattedErrors})
        res.status(400).json(formattedErrors)
        return
    }
    next()
}

const tokenValidator = async (req, res, next) => {
    logger.info("Validating the auth token")
    const authorizationHeader = req.headers.authorization;
    if (authorizationHeader && authorizationHeader.startsWith('Bearer ')) {
        const token = authorizationHeader.split(' ')[1];
        try {
            const isTokenValid = await authSerivce.validateToken(token)
            if(isTokenValid){
                next()
            }
        } catch (error) {
            res.status(401).json({ error: 'Authentication failed' });
            logger.error("Authentication failed, ", error)
        }
    } else {
        logger.error("Invalid bearer token: ", authorizationHeader)
        res.status(401).json({ error: 'Invalid bearer token' });
    }
}

module.exports ={
    validatePlan: validatePlan,
    tokenValidator: tokenValidator,
    validateLinkedPlanServices: validateLinkedPlanServices
}