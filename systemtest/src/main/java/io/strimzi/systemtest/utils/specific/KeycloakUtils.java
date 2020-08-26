/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.test.executor.Exec;
import io.strimzi.test.executor.ExecResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KeycloakUtils {

    private static final Logger LOGGER = LogManager.getLogger(KeycloakUtils.class);

    public final static String PATH_TO_KEYCLOAK_PREPARE_SCRIPT = "../systemtest/src/test/resources/oauth2/prepare_keycloak_operator.sh";
    public final static String PATH_TO_KEYCLOAK_TEARDOWN_SCRIPT = "../systemtest/src/test/resources/oauth2/teardown_keycloak_operator.sh";


    private KeycloakUtils() {}

    public static void deployKeycloak(String namespace) {
        LOGGER.info("Prepare Keycloak in namespace: {}", namespace);
        ResourceManager.getPointerResources().push(() -> deleteKeycloak(namespace));
        ExecResult result = Exec.exec(true, "/bin/bash", PATH_TO_KEYCLOAK_PREPARE_SCRIPT, namespace);

        if (!result.out().contains("All realms were successfully imported")) {
            LOGGER.info("Errors occurred during Keycloak install: {}", result.err());
            throw new RuntimeException("Keycloak wasn't deployed correctly!");
        }
        LOGGER.info("Keycloak in namespace {} is ready", namespace);
    }

    public static void deleteKeycloak(String namespace) {
        LOGGER.info("Teardown Keycloak in namespace: {}", namespace);
        Exec.exec(true, "/bin/bash", PATH_TO_KEYCLOAK_TEARDOWN_SCRIPT, namespace);
    }
}
