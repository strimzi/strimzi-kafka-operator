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

public class RequiredMinKubeOrOpenshiftVersionCondition implements ExecutionCondition  {

    private static final Logger LOGGER = LogManager.getLogger(RequiredMinKubeOrOpenshiftVersionCondition.class);

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext extensionContext) {
        final Optional<RequiredMinKubeOrOcpBasedKubeVersion> annotation = findAnnotation(extensionContext.getElement(), RequiredMinKubeOrOcpBasedKubeVersion.class);
        final KubeClusterResource cluster = KubeClusterResource.getInstance();
        final double ocpBasedKubeVersion = annotation.get().ocpBasedKubeVersion();
        final double kubeVersion = annotation.get().kubeVersion();
        final boolean isOcp = cluster.isOpenShift();

        if ((isOcp && Double.parseDouble(cluster.client().clusterKubernetesVersion()) >= ocpBasedKubeVersion) ||
            !isOcp && Double.parseDouble(cluster.client().clusterKubernetesVersion()) >= kubeVersion) {
            return ConditionEvaluationResult.enabled("Test is enabled");
        } else {
            LOGGER.info("@RequiredMinKubeOrOpenshiftVersion with type of cluster: {} and version {}, but the running on cluster with {}: Ignoring {}",
                cluster.cluster().getClass().toString(),
                isOcp ? ocpBasedKubeVersion : kubeVersion,
                cluster.client().clusterKubernetesVersion(),
                extensionContext.getDisplayName()
            );
            return ConditionEvaluationResult.disabled("Test is disabled");
        }
    }
}
