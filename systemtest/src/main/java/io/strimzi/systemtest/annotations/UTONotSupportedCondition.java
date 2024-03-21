/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.annotations;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

public class UTONotSupportedCondition implements ExecutionCondition {
    private static final Logger LOGGER = LogManager.getLogger(UTONotSupportedCondition.class);

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext extensionContext) {
        LOGGER.warn("Skipping this test because the scenario will not work with UTO");
        return ConditionEvaluationResult.disabled("Test is disabled");
    }
}
