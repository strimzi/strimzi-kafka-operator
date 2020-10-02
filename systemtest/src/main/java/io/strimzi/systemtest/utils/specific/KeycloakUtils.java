/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.test.executor.Exec;
import io.strimzi.test.executor.ExecResult;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static io.strimzi.systemtest.resources.ResourceManager.cmdKubeClient;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

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

    public static String getToken(String baseURI, String userName, String password) {
        String coPodName = kubeClient().getClusterOperatorPodName();
        return new JsonObject(
            cmdKubeClient().execInPod(
                coPodName,
                "curl",
                "-v",
                "--insecure",
                "-X",
                "POST",
                "-d", "client_id=admin-cli&client_secret=aGVsbG8td29ybGQtcHJvZHVjZXItc2VjcmV0&grant_type=password&username=" + userName + "&password=" + password,
                baseURI + "/auth/realms/master/protocol/openid-connect/token"
            ).out()).getString("access_token");
    }

    public static JsonObject getKeycloakRealm(String baseURI, String token, String desiredRealm) {
        String coPodName = kubeClient().getClusterOperatorPodName();
        return new JsonObject(cmdKubeClient().execInPod(
            coPodName,
            "curl",
            "-v",
            "--insecure",
            "-X",
            "GET",
            baseURI + "/auth/admin/realms/" + desiredRealm,
            "-H", "Authorization: Bearer " + token
        ).out());
    }

    public static JsonArray getKeycloakRealmClients(String baseURI, String token, String desiredRealm) {
        String coPodName = kubeClient().getClusterOperatorPodName();
        return new JsonArray(cmdKubeClient().execInPod(
            coPodName,
            "curl",
            "-v",
            "--insecure",
            "-X",
            "GET",
            baseURI + "/auth/admin/realms/" + desiredRealm + "/clients",
            "-H", "Authorization: Bearer " + token
        ).out());
    }

    public static JsonArray getResourcesFromRealmClient(String baseUri, String token, String desiredRealm, String clientId) {
        return getConfigFromResourceServerOfRealm(baseUri, token, desiredRealm, clientId, "resource");
    }

    public static JsonArray getPoliciesFromRealmClient(String baseUri, String token, String desiredRealm, String clientId) {
        return getConfigFromResourceServerOfRealm(baseUri, token, desiredRealm, clientId, "policy");
    }

    private static JsonArray getConfigFromResourceServerOfRealm(String baseUri, String token, String desiredRealm, String clientId, String endpoint) {
        String coPodName = kubeClient().getClusterOperatorPodName();
        return new JsonArray(cmdKubeClient().execInPod(
            coPodName,
            "curl",
            "-v",
            "--insecure",
            "-X",
            "GET",
            baseUri + "/auth/admin/realms/" + desiredRealm + "/clients/" + clientId + "/authz/resource-server/" + endpoint,
            "-H", "Authorization: Bearer " + token
        ).out());
    }

    public static String putConfigurationToRealm(String baseUri, String token, JsonObject config, String desiredRealm) {
        String coPodName = kubeClient().getClusterOperatorPodName();
        return cmdKubeClient().execInPod(
            coPodName,
            "curl",
            "-v",
            "--insecure",
            "-X",
            "PUT",
            baseUri + "/auth/admin/realms/" + desiredRealm,
            "-H", "Authorization: Bearer " + token,
            "-d", config.toString(),
            "-H", "Content-Type: application/json"
        ).out();
    }

    public static String updatePolicyOfRealmClient(String baseUri, String token, JsonObject policy, String desiredRealm, String clientId) {
        String coPodName = kubeClient().getClusterOperatorPodName();
        return cmdKubeClient().execInPod(
            coPodName,
            "curl",
            "-v",
            "--insecure",
            "-X",
            "PUT",
            baseUri + "/auth/admin/realms/" + desiredRealm + "/clients/" + clientId + "/authz/resource-server/policy/" + policy.getValue("id"),
            "-H", "Authorization: Bearer " + token,
            "-d", policy.toString(),
            "-H", "Content-Type: application/json"
        ).out();
    }
}
