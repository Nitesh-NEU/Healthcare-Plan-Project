/**
 * Data Quality Validation Framework
 * Ensures data integrity and quality across the pipeline
 */

const logger = require('../src/logger/logger');

class DataQualityValidator {
    constructor() {
        this.validationResults = [];
        this.rules = this.defineValidationRules();
    }

    /**
     * Define validation rules
     */
    defineValidationRules() {
        return {
            // Completeness checks
            completeness: [
                {
                    name: 'required_fields_present',
                    description: 'All required fields must be present',
                    severity: 'critical',
                    check: (plan) => {
                        const required = ['objectId', 'objectType', 'planType', 'creationDate', '_org'];
                        const missing = required.filter(field => !plan[field]);
                        return {
                            passed: missing.length === 0,
                            message: missing.length > 0 ? `Missing fields: ${missing.join(', ')}` : 'All required fields present'
                        };
                    }
                },
                {
                    name: 'cost_shares_present',
                    description: 'Plan cost shares must be defined',
                    severity: 'high',
                    check: (plan) => {
                        const hasCostShares = plan.planCostShares && 
                                            typeof plan.planCostShares.deductible === 'number' &&
                                            typeof plan.planCostShares.copay === 'number';
                        return {
                            passed: hasCostShares,
                            message: hasCostShares ? 'Cost shares defined' : 'Missing or invalid cost shares'
                        };
                    }
                }
            ],

            // Validity checks
            validity: [
                {
                    name: 'valid_plan_type',
                    description: 'Plan type must be from allowed values',
                    severity: 'high',
                    check: (plan) => {
                        const allowedTypes = ['inNetwork', 'outOfNetwork', 'hmo', 'ppo', 'epo'];
                        const isValid = allowedTypes.includes(plan.planType);
                        return {
                            passed: isValid,
                            message: isValid ? 'Valid plan type' : `Invalid plan type: ${plan.planType}`
                        };
                    }
                },
                {
                    name: 'valid_date_format',
                    description: 'Creation date must be valid ISO date',
                    severity: 'critical',
                    check: (plan) => {
                        try {
                            const date = new Date(plan.creationDate);
                            const isValid = !isNaN(date.getTime());
                            return {
                                passed: isValid,
                                message: isValid ? 'Valid date format' : 'Invalid date format'
                            };
                        } catch (error) {
                            return {
                                passed: false,
                                message: 'Date parsing error'
                            };
                        }
                    }
                },
                {
                    name: 'object_id_format',
                    description: 'ObjectId should follow expected pattern',
                    severity: 'medium',
                    check: (plan) => {
                        // Check if objectId is not empty and has reasonable format
                        const isValid = plan.objectId && 
                                      typeof plan.objectId === 'string' && 
                                      plan.objectId.length > 5;
                        return {
                            passed: isValid,
                            message: isValid ? 'Valid objectId format' : 'Invalid objectId format'
                        };
                    }
                }
            ],

            // Consistency checks
            consistency: [
                {
                    name: 'positive_costs',
                    description: 'All cost values must be non-negative',
                    severity: 'high',
                    check: (plan) => {
                        const deductible = plan.planCostShares?.deductible || 0;
                        const copay = plan.planCostShares?.copay || 0;
                        const isValid = deductible >= 0 && copay >= 0;
                        return {
                            passed: isValid,
                            message: isValid ? 'All costs are positive' : 'Negative cost values detected'
                        };
                    }
                },
                {
                    name: 'reasonable_cost_ranges',
                    description: 'Costs should be within reasonable ranges',
                    severity: 'medium',
                    check: (plan) => {
                        const deductible = plan.planCostShares?.deductible || 0;
                        const copay = plan.planCostShares?.copay || 0;
                        const totalCost = deductible + copay;
                        
                        const isReasonable = totalCost >= 0 && totalCost <= 50000;
                        return {
                            passed: isReasonable,
                            message: isReasonable ? 'Costs within reasonable range' : 
                                    `Unusually high cost: $${totalCost}`
                        };
                    }
                },
                {
                    name: 'service_costs_consistency',
                    description: 'Service costs must be consistent',
                    severity: 'medium',
                    check: (plan) => {
                        if (!plan.linkedPlanServices || plan.linkedPlanServices.length === 0) {
                            return {
                                passed: true,
                                message: 'No services to validate'
                            };
                        }
                        
                        let allValid = true;
                        for (const service of plan.linkedPlanServices) {
                            const serviceCosts = service.planserviceCostShares;
                            if (serviceCosts) {
                                if (serviceCosts.deductible < 0 || serviceCosts.copay < 0) {
                                    allValid = false;
                                    break;
                                }
                            }
                        }
                        
                        return {
                            passed: allValid,
                            message: allValid ? 'All service costs consistent' : 'Inconsistent service costs'
                        };
                    }
                }
            ],

            // Uniqueness checks
            uniqueness: [
                {
                    name: 'unique_object_ids',
                    description: 'Object IDs within plan should be unique',
                    severity: 'high',
                    check: (plan) => {
                        const ids = [
                            plan.objectId,
                            plan.planCostShares?.objectId,
                            ...(plan.linkedPlanServices?.map(s => s.objectId) || []),
                            ...(plan.linkedPlanServices?.map(s => s.linkedService?.objectId) || [])
                        ].filter(id => id);
                        
                        const uniqueIds = new Set(ids);
                        const isUnique = ids.length === uniqueIds.size;
                        
                        return {
                            passed: isUnique,
                            message: isUnique ? 'All IDs unique' : 'Duplicate IDs detected'
                        };
                    }
                }
            ],

            // Timeliness checks
            timeliness: [
                {
                    name: 'recent_creation_date',
                    description: 'Creation date should not be in future',
                    severity: 'high',
                    check: (plan) => {
                        try {
                            const creationDate = new Date(plan.creationDate);
                            const now = new Date();
                            const isValid = creationDate <= now;
                            return {
                                passed: isValid,
                                message: isValid ? 'Creation date valid' : 'Creation date is in the future'
                            };
                        } catch (error) {
                            return {
                                passed: false,
                                message: 'Unable to validate date'
                            };
                        }
                    }
                }
            ]
        };
    }

    /**
     * Validate a single plan
     */
    validatePlan(plan, options = {}) {
        const results = {
            planId: plan.objectId,
            timestamp: new Date().toISOString(),
            overallStatus: 'passed',
            categories: {},
            failedRules: [],
            warnings: []
        };

        // Run all validation categories
        for (const [category, rules] of Object.entries(this.rules)) {
            results.categories[category] = {
                passed: 0,
                failed: 0,
                warnings: 0,
                rules: []
            };

            for (const rule of rules) {
                try {
                    const result = rule.check(plan);
                    
                    const ruleResult = {
                        name: rule.name,
                        description: rule.description,
                        severity: rule.severity,
                        passed: result.passed,
                        message: result.message
                    };

                    results.categories[category].rules.push(ruleResult);

                    if (result.passed) {
                        results.categories[category].passed++;
                    } else {
                        if (rule.severity === 'critical' || rule.severity === 'high') {
                            results.categories[category].failed++;
                            results.failedRules.push(ruleResult);
                            results.overallStatus = 'failed';
                        } else {
                            results.categories[category].warnings++;
                            results.warnings.push(ruleResult);
                            if (results.overallStatus === 'passed') {
                                results.overallStatus = 'warning';
                            }
                        }
                    }
                } catch (error) {
                    logger.error(`Error running rule ${rule.name}:`, error);
                    results.categories[category].failed++;
                    results.overallStatus = 'error';
                }
            }
        }

        // Store result
        this.validationResults.push(results);

        return results;
    }

    /**
     * Validate multiple plans
     */
    validateBatch(plans) {
        logger.info(`Validating batch of ${plans.length} plans...`);
        
        const results = plans.map(plan => this.validatePlan(plan));
        
        const summary = {
            totalPlans: plans.length,
            passed: results.filter(r => r.overallStatus === 'passed').length,
            warnings: results.filter(r => r.overallStatus === 'warning').length,
            failed: results.filter(r => r.overallStatus === 'failed').length,
            errors: results.filter(r => r.overallStatus === 'error').length
        };

        logger.info('Batch validation summary:', summary);

        return {
            summary,
            results
        };
    }

    /**
     * Get validation statistics
     */
    getStatistics() {
        const totalValidations = this.validationResults.length;
        if (totalValidations === 0) {
            return { message: 'No validations performed yet' };
        }

        return {
            totalValidations,
            passed: this.validationResults.filter(r => r.overallStatus === 'passed').length,
            warnings: this.validationResults.filter(r => r.overallStatus === 'warning').length,
            failed: this.validationResults.filter(r => r.overallStatus === 'failed').length,
            errors: this.validationResults.filter(r => r.overallStatus === 'error').length,
            passRate: (this.validationResults.filter(r => r.overallStatus === 'passed').length / totalValidations * 100).toFixed(2) + '%'
        };
    }

    /**
     * Clear validation history
     */
    clearResults() {
        this.validationResults = [];
    }
}

// Export singleton instance
module.exports = new DataQualityValidator();
