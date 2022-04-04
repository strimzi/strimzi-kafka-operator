/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.strimzi.systemtest.Constants;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.Exec;
import io.strimzi.test.executor.ExecResult;
import io.strimzi.test.k8s.KubeClusterResource;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Level;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KeycloakUtils {

    private static final Logger LOGGER = LogManager.getLogger(KeycloakUtils.class);

    public final static String PATH_TO_KEYCLOAK_PREPARE_SCRIPT = "../systemtest/src/test/resources/oauth2/prepare_keycloak_operator.sh";
    public final static String PATH_TO_KEYCLOAK_TEARDOWN_SCRIPT = "../systemtest/src/test/resources/oauth2/teardown_keycloak_operator.sh";

    public final static String LATEST_KEYCLOAK_VERSION = "15.0.2";
    public final static String OLD_KEYCLOAK_VERSION = "11.0.1";


    private KeycloakUtils() {}

    public static void deployKeycloak(final String deploymentNamespace, final String watchNamespace) {
        LOGGER.info("Prepare Keycloak Operator in namespace: {} with watching namespace: {}", deploymentNamespace, watchNamespace);

        // This is needed because from time to time the first try fails on Azure
        TestUtils.waitFor("Keycloak instance readiness", Constants.KEYCLOAK_DEPLOYMENT_POLL, Constants.KEYCLOAK_DEPLOYMENT_TIMEOUT, () -> {
            ExecResult result = Exec.exec(Level.INFO, "/bin/bash", PATH_TO_KEYCLOAK_PREPARE_SCRIPT, deploymentNamespace, getValidKeycloakVersion(), watchNamespace);

            if (!result.out().contains("All realms were successfully imported")) {
                LOGGER.info("Errors occurred during Keycloak install: {}", result.err());
                return false;
            }
            return  true;
        });

        LOGGER.info("Keycloak in namespace {} is ready", deploymentNamespace);
    }

    public static void deleteKeycloak(final String deploymentNamespace, final String watchNamespace) {
        LOGGER.info("Teardown Keycloak Operator in namespace: {} with watching namespace: {}", deploymentNamespace, watchNamespace);
        Exec.exec(Level.INFO, "/bin/bash", PATH_TO_KEYCLOAK_TEARDOWN_SCRIPT, deploymentNamespace, getValidKeycloakVersion(), watchNamespace);
    }

    /**
     * Returns token from Keycloak API
     * @param namespaceName
     * @param baseURI base uri for accessing Keycloak API
     * @param userName name of user
     * @param password password of user
     * @return user token
     */
    public static String getToken(String namespaceName, String baseURI, String userName, String password) {
        String coPodName = kubeClient(namespaceName).getClusterOperatorPodName();
        return new JsonObject(
            cmdKubeClient(namespaceName).execInPod(
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

    /**
     * Returns specific realm from Keycloak API
     * @param namespaceName namespace name
     * @param baseURI base uri for accessing Keycloak API
     * @param token admin token
     * @param desiredRealm realm we want to get
     * @return JsonObject with whole desired realm from Keycloak
     */
    public static JsonObject getKeycloakRealm(String namespaceName, String baseURI, String token, String desiredRealm) {
        String coPodName = kubeClient(namespaceName).getClusterOperatorPodName();
        return new JsonObject(cmdKubeClient(namespaceName).execInPod(
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

    /**
     * Returns all clients for specific realm
     * @param namespaceName namespace name
     * @param baseURI base uri for accessing Keycloak API
     * @param token admin token
     * @param desiredRealm realm we want to get clients from
     * @return JsonArray with all clients set for the specific realm
     */
    public static JsonArray getKeycloakRealmClients(String namespaceName, String baseURI, String token, String desiredRealm) {
        String coPodName = kubeClient(namespaceName).getClusterOperatorPodName();
        return new JsonArray(cmdKubeClient(namespaceName).execInPod(
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

    /**
     * Returns all policies from client of specific realm
     * @param namespaceName namespace name
     * @param baseURI base uri for accessing Keycloak API
     * @param token admin token
     * @param desiredRealm realm we want to get clients from
     * @param clientId id of desired client
     * @return JsonArray with all policies for clients in specific realm
     */
    public static JsonArray getPoliciesFromRealmClient(String namespaceName, String baseURI, String token, String desiredRealm, String clientId) {
        return getConfigFromResourceServerOfRealm(namespaceName, baseURI, token, desiredRealm, clientId, "policy");
    }

    /**
     * Returns "resources" for desired endpoint -> policies, resources ...
     * @param namespaceName namespace name
     * @param baseURI base uri for accessing Keycloak API
     * @param token admin token
     * @param desiredRealm realm we want to get clients from
     * @param clientId id of desired client
     * @param endpoint endpoint for "resource" - resource, policy etc.
     * @return JsonArray with results from endpoint
     */
    private static JsonArray getConfigFromResourceServerOfRealm(String namespaceName, String baseURI, String token, String desiredRealm, String clientId, String endpoint) {
        String coPodName = kubeClient(namespaceName).getClusterOperatorPodName();
        return new JsonArray(cmdKubeClient(namespaceName).execInPod(
            coPodName,
            "curl",
            "-v",
            "--insecure",
            "-X",
            "GET",
            baseURI + "/auth/admin/realms/" + desiredRealm + "/clients/" + clientId + "/authz/resource-server/" + endpoint,
            "-H", "Authorization: Bearer " + token
        ).out());
    }

    /**
     * Puts new configuration to the specific realm
     * @param namespaceName namespace name
     * @param baseURI base uri for accessing Keycloak API
     * @param token admin token
     * @param desiredRealm realm where the config should be put
     * @param config configuration we want to put into the realm
     * @return response from server
     */
    public static String putConfigurationToRealm(String namespaceName, String baseURI, String token, JsonObject config, String desiredRealm) {
        String coPodName = kubeClient(namespaceName).getClusterOperatorPodName();
        return cmdKubeClient(namespaceName).execInPod(
            coPodName,
            "curl",
            "-v",
            "--insecure",
            "-X",
            "PUT",
            baseURI + "/auth/admin/realms/" + desiredRealm,
            "-H", "Authorization: Bearer " + token,
            "-d", config.toString(),
            "-H", "Content-Type: application/json"
        ).out();
    }

    /**
     * Updates policies of specific client in realm
     * @param namespaceName namespace name
     * @param baseURI base uri for accessing Keycloak API
     * @param token admin token
     * @param desiredRealm realm where the client policies should be updated
     * @param policy new updated policies
     * @param clientId id of client where we want to update policies
     * @return response from server
     */
    public static String updatePolicyOfRealmClient(String namespaceName, String baseURI, String token, JsonObject policy, String desiredRealm, String clientId) {
        String coPodName = kubeClient(namespaceName).getClusterOperatorPodName();
        return cmdKubeClient(namespaceName).execInPod(
            coPodName,
            "curl",
            "-v",
            "--insecure",
            "-X",
            "PUT",
            baseURI + "/auth/admin/realms/" + desiredRealm + "/clients/" + clientId + "/authz/resource-server/policy/" + policy.getValue("id"),
            "-H", "Authorization: Bearer " + token,
            "-d", policy.toString(),
            "-H", "Content-Type: application/json"
        ).out();
    }

    public static String getValidKeycloakVersion() {
        if (Double.parseDouble(KubeClusterResource.getInstance().client().clusterKubernetesVersion()) >= 1.22) {
            return LATEST_KEYCLOAK_VERSION;
        } else {
            return OLD_KEYCLOAK_VERSION;
        }
    }
}
