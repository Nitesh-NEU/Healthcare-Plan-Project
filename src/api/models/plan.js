const mongoose = require("mongoose")

const linkedServiceSchema = new mongoose.Schema({
    _org: {
        type: String,
        required: true
    },
    objectId: {
        type: String,
        required: true
    },
    objectType: {
        type: String,
        required: true
    },
    name: {
        type: String,
        required: true
    }
})

const planCostShareSchema = new mongoose.Schema({
    _org: {
        type: String,
        required: true
    },
    objectId: {
        type: String,
        required: true
    },
    objectType: {
        type: String,
        required: true
    },
    copay: {
        type: Number,
        required: true
    },
    deductible: {
        type: Number,
        required: true
    }
})


const linkedPlanServiceSchema = new mongoose.Schema({
    _org: {
        type: String,
        required: true
    },
    objectId: {
        type: String,
        required: true
    },
    objectType: {
        type: String,
        required: true
    },
    linkedService:{
        type: linkedServiceSchema,
        required: true
    },
    planserviceCostShares: {
        type:  planCostShareSchema,
        required: true
    }
})

const planSchema = new mongoose.Schema({
    _org: {
        type: String,
        required: true
    },
    objectId: {
        type: String,
        required: true
    },
    objectType: {
        type: String,
        required: true
    },
    planType: {
        type: String,
        required: true
    },
    creationDate:{
        type: Date,
        required: true
    },
    planCostShares: {
        type:  planCostShareSchema,
        required: true
    },
    linkedPlanServices: {
        type: [linkedPlanServiceSchema],
        required: true
    }
})

const transformJSON = (document, returnedObject) =>{
    delete returnedObject._id;
    delete returnedObject.__v;
}

linkedServiceSchema.set("toJSON", { transform: transformJSON });
planCostShareSchema.set("toJSON", { transform: transformJSON });
linkedPlanServiceSchema.set("toJSON", { transform: transformJSON });
planSchema.set("toJSON", { transform: transformJSON });


const Plan = mongoose.model("Plan", planSchema)

module.exports = Plan