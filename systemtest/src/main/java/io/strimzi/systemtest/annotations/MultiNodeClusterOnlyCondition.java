/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.annotations;

import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

public class MultiNodeClusterOnlyCondition implements ExecutionCondition {
    private static final Logger LOGGER = LogManager.getLogger(MultiNodeClusterOnlyCondition.class);

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext extensionContext) {
        KubeClusterResource clusterResource = KubeClusterResource.getInstance();

        if (clusterResource.client().getClusterNodes().size() > 1) {
            return ConditionEvaluationResult.enabled("Test is enabled");
        } else {
            LOGGER.info("{} is @MultiNodeClusterOnly, but the running cluster is not multi-node cluster: Ignoring {}",
                    extensionContext.getDisplayName(),
                    extensionContext.getDisplayName()
            );
            return ConditionEvaluationResult.disabled("Test is disabled");
        }
    }
}
