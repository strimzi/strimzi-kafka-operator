/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.annotations;

import io.strimzi.test.k8s.KubeClusterResource;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

public class FIPSNotSupportedCondition implements ExecutionCondition {
    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext extensionContext) {
        boolean fipsEnabled = false;

        if (KubeClusterResource.getInstance().isOpenShift()) {
            fipsEnabled = kubeClient().getConfigMap("kube-system", "cluster-config-v1").getData().get("install-config").contains("fips: true");
        }

        return fipsEnabled ? ConditionEvaluationResult.disabled("Test is disabled") : ConditionEvaluationResult.enabled("Test is enabled");
    }
}
