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

import java.util.Optional;

import static org.junit.platform.commons.support.AnnotationSupport.findAnnotation;

public class RequiredMinKubeApiVersionCondition implements ExecutionCondition {
    private static final Logger LOGGER = LogManager.getLogger(RequiredMinKubeApiVersionCondition.class);

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext extensionContext) {
        Optional<RequiredMinKubeApiVersion> annotation = findAnnotation(extensionContext.getElement(), RequiredMinKubeApiVersion.class);
        KubeClusterResource clusterResource = KubeClusterResource.getInstance();
        double version = annotation.get().version();

        if (Double.parseDouble(clusterResource.client().clusterKubernetesVersion()) >= version) {
            return ConditionEvaluationResult.enabled("Test is enabled");
        } else {
            LOGGER.info("{} is @RequiredMinKubeApiVersion with version {}, but the running on cluster with {}: Ignoring {}",
                    extensionContext.getDisplayName(),
                    version,
                    clusterResource.client().clusterKubernetesVersion(),
                    extensionContext.getDisplayName()
            );
            return ConditionEvaluationResult.disabled("Test is disabled");
        }
    }
}
