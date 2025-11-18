const logger = require('../../logger/logger')
const authSerivce = require('../../services/auth')

const validateToken = async (req, res) => {
    const authorizationHeader = req.headers.authorization;
    if (authorizationHeader && authorizationHeader.startsWith('Bearer ')) {
        const token = authorizationHeader.split(' ')[1];
        try {
            const isTokenValid = await authSerivce.validateToken(token)
            if(isTokenValid){
                res.status(200).json({ message: 'Token is valid' });
            }
        } catch (error) {
            res.status(401).json({ error: 'Authentication failed' });
            logger.error("Authentication failed, ", error)
        }
    } else {
        logger.error("Invalid bearer token: ", authorizationHeader)
        res.status(401).json({ error: 'Invalid bearer token' });
    }
};

module.exports = {
    validateToken: validateToken
};