const logger = require('../logger/logger')

const { OAuth2Client } = require('google-auth-library');
const client = new OAuth2Client();

const validateToken = async (token) => {
    try {
        const ticket = await client.verifyIdToken({
            idToken: token,
            audience: process.env.OAUTH_CLIENT_ID,
        });
        return true
    } catch (error) {
        logger.error("Failed to validate token: ", error)
        throw new Error(error);
    }
};


module.exports = {
    validateToken: validateToken
};