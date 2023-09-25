/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.annotations;

import io.strimzi.systemtest.Environment;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Custom condition to check if the current network environment supports IPv6.
 *
 * <p>It implements the JUnit Jupiter's {@code ExecutionCondition} to determine
 * whether a test or test class should be executed or skipped based on network conditions.</p>
 *
 * <p>When the runtime environment uses IPv6, the test is disabled, otherwise, it's enabled.</p>
 *
 * @see IPv6NotSupported
 * @see ExecutionCondition
 */
public class IPv6NotSupportedCondition implements ExecutionCondition {

    /**
     * Evaluates the condition based on the network environment's support for IPv6.
     *
     * @param extensionContext the context for the current test or test class.
     * @return {@code ConditionEvaluationResult.disabled} if the environment supports IPv6, otherwise {@code ConditionEvaluationResult.enabled}.
     */
    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext extensionContext) {
        return Environment.isIpv6Family() ? ConditionEvaluationResult.disabled("Test is disabled") : ConditionEvaluationResult.enabled("Test is enabled");
    }
}
