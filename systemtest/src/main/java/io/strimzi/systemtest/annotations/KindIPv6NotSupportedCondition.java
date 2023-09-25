/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.annotations;

import io.strimzi.systemtest.Environment;
import io.strimzi.test.k8s.KubeClusterResource;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Custom JUnit 5 execution condition to check if the current K8s cluster is of the 'kind' type
 * and if the environment uses IPv6.
 * <p>
 * Tests will be disabled when running on a 'kind' cluster with IPv6 due to the lack of support.
 * </p>
 */
public class KindIPv6NotSupportedCondition implements ExecutionCondition {

    /**
     * Evaluates the condition based on the K8s cluster type and environment settings.
     *
     * @param       extensionContext the current extension context, not used in this implementation.
     * @return      {@link ConditionEvaluationResult#disabled(String)} if the K8s cluster is of the 'kind' type and the environment uses IPv6.
     * Otherwise,   {@link ConditionEvaluationResult#enabled(String)}.
     */
    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext extensionContext) {
        return KubeClusterResource.getInstance().isKind() && Environment.isIpv6Family() ?
                ConditionEvaluationResult.disabled("Test is disabled") :
                ConditionEvaluationResult.enabled("Test is enabled");
    }
}
