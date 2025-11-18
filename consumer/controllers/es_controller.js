const es_service = require("./../services/es_service");

const createPlanIndex = async (plan) => {
    const result = await es_service.postDocument(plan);
}

const deletePlanIndex = async (plan) => {
    const result = await es_service.deleteDocument(plan);
}

module.exports = {
    createPlanIndex, deletePlanIndex
}