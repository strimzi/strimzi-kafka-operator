/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.annotations;

import io.strimzi.test.k8s.KubeClusterResource;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Represents a condition that checks if a kind is supported for execution.
 * If the kind is supported, the test will be disabled, otherwise enabled.
 */
public class KindNotSupportedCondition implements ExecutionCondition {

    /**
     * Evaluates the condition to determine if the test should be enabled or disabled
     * based on the kind supported by the KubeClusterResource.
     *
     * @param extensionContext the context for the current test
     * @return {@link ConditionEvaluationResult} indicating if the test should be enabled or disabled
     */
    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext extensionContext) {
        return KubeClusterResource.getInstance().isKind() ? ConditionEvaluationResult.disabled("Test is disabled") : ConditionEvaluationResult.enabled("Test is enabled");
    }
}
