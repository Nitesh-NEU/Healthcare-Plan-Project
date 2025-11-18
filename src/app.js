require('dotenv').config()
const express = require('express')
const app = express()
const mongoose= require("mongoose")
app.use(express.json())
const healthzRouter = require('./api/routes/healthz.routes')
const { plansRouter } = require('./api/routes/plan.routes')
const { authRouter } = require('./api/routes/auth.routes')
const analyticsRouter = require('./api/routes/analytics.routes')
const { tokenValidator } = require('./api/middleware/validators')

app.set("etag", "strong")
app.use("/auth/", authRouter)
app.use('/v1/healthz', healthzRouter)
app.use('/v1/plan', plansRouter)
app.use('/v1/analytics', analyticsRouter)  // Data analytics endpoints
MONGODB_URI = process.env.MONGODB_URI

mongoose.connect(MONGODB_URI)
    .then(() => {
        console.log("successfully connected to MongoDB");
    })
    .catch((error) => {
        console.log("Error connecting to database: ", error)
    })

module.exports = app