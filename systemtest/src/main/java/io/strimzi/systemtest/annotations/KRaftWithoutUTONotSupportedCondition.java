/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.annotations;

import io.strimzi.systemtest.Environment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

public class KRaftWithoutUTONotSupportedCondition implements ExecutionCondition {

    private static final Logger LOGGER = LogManager.getLogger(KRaftWithoutUTONotSupportedCondition.class);

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext extensionContext) {
        if (!Environment.isKRaftModeEnabled() || Environment.isUnidirectionalTopicOperatorEnabled()) {
            return ConditionEvaluationResult.enabled("Test is enabled");
        } else {
            LOGGER.warn("According to {} env variable with value: {}, the KRaft mode is used, skipping this test because is not KRaft mode without UTO compliant",
                Environment.STRIMZI_FEATURE_GATES_ENV,
                Environment.STRIMZI_FEATURE_GATES);
            return ConditionEvaluationResult.disabled("Test is disabled");
        }
    }
}
