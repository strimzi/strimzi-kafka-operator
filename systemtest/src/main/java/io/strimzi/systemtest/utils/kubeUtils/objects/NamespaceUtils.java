/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.objects;

import io.strimzi.systemtest.Constants;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class NamespaceUtils {

    private static final Logger LOGGER = LogManager.getLogger(NamespaceUtils.class);

    private NamespaceUtils() { }

    public static void waitForNamespaceDeletion(String name) {
        LOGGER.info("Waiting when Namespace {} to be deleted", name);

        TestUtils.waitFor("namespace " + name, Constants.POLL_INTERVAL_FOR_RESOURCE_READINESS, Constants.TIMEOUT_FOR_RESOURCE_READINESS,
            () -> kubeClient().getNamespace(name) == null);
    }
}
