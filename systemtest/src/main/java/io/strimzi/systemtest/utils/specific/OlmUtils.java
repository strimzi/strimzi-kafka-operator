/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.strimzi.systemtest.Constants;
import io.strimzi.test.TestUtils;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class OlmUtils {

    private OlmUtils() {}

    public static void waitUntilNonUsedInstallPlanIsPresent(String namespaceName) {
        TestUtils.waitFor("Wait for non used install plan to be present", Constants.OLM_UPGRADE_INSTALL_PLAN_POLL, Constants.OLM_UPGRADE_INSTALL_PLAN_TIMEOUT,
            () -> kubeClient().getNonApprovedInstallPlan(namespaceName) != null);
    }
}
