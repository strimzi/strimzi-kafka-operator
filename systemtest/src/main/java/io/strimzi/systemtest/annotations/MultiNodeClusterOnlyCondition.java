/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.annotations;

import io.fabric8.kubernetes.api.model.Node;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.systemtest.TestConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.List;
import java.util.Optional;

import static org.junit.platform.commons.support.AnnotationSupport.findAnnotation;

public class MultiNodeClusterOnlyCondition implements ExecutionCondition {
    private static final Logger LOGGER = LogManager.getLogger(MultiNodeClusterOnlyCondition.class);

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext extensionContext) {
        Optional<MultiNodeClusterOnly> annotation = findAnnotation(extensionContext.getElement(), MultiNodeClusterOnly.class);
        int expectedNodeCount = annotation.get().workerNodeCount();

        List<Node> nodes = KubeResourceManager.get().kubeClient().getClient()
            .nodes()
            .list()
            .getItems()
            .stream()
            .filter(node -> node.getMetadata().getLabels().containsKey(TestConstants.WORKER_NODE_LABEL))
            .toList();

        if (nodes.size() > expectedNodeCount) {
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
