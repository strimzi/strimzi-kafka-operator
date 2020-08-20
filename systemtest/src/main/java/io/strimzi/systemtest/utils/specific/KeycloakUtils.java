/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KeycloakUtils {

    private static final Logger LOGGER = LogManager.getLogger(KeycloakUtils.class);

    private KeycloakUtils() {}

    public static void waitUntilKeycloakCustomResourceReady(String namespace, String customResourceName, String readyStatus) {
        TestUtils.waitFor("Keycloak CR will be ready", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT, () -> {

            LOGGER.info("Keycloak logs: {}", ResourceManager.cmdKubeClient().namespace(namespace).get("keycloak", customResourceName));

            if (ResourceManager.cmdKubeClient().namespace(namespace).get("keycloak", customResourceName).contains(readyStatus)) {
                LOGGER.info("Keycloak custom resource is ready");
                return true;
            }
            LOGGER.error("Keycloak custom resource is still not ready");
            return false;
        });
    }

    public static void waitUntilKeycloakCustomResourceDeletion(String namespace, String customResourceName) {
        TestUtils.waitFor("Wait for Keycloak CR will be deleted", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT, () -> {

            if (ResourceManager.cmdKubeClient().namespace(namespace).get("keycloak", customResourceName) == null) {
                LOGGER.info("Keycloak custom resource is successfully deleted");
                return true;
            }
            LOGGER.error("Keycloak custom resource is still up");
            return false;
        });
    }
}
