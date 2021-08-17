package io.strimzi.systemtest.annotations;

import io.strimzi.systemtest.parallel.ParallelSuiteController;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

public class IsolatedSuiteCondition implements ExecutionCondition {

    private static final ConditionEvaluationResult ENABLED = ConditionEvaluationResult.enabled("@IsolatedSuiteCondition");
    private static final Logger LOGGER = LogManager.getLogger(IsolatedSuiteCondition.class);

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext extensionContext) {
        while (ParallelSuiteController.waitUntilZeroParallelSuites()) {
            LOGGER.debug("Current number of suites running in parallel:{}", ParallelSuiteController.getCounter());
        }
        return ENABLED;
    }
}