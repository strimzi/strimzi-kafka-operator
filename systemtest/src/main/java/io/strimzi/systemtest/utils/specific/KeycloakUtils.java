/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.test.TestUtils;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class KeycloakUtils {

    public final static String LATEST_KEYCLOAK_VERSION = "25.0.2";

    private final static LabelSelector SCRAPER_SELECTOR = new LabelSelector(null, Map.of(TestConstants.APP_POD_LABEL, TestConstants.SCRAPER_NAME));
    private static final Logger LOGGER = LogManager.getLogger(KeycloakUtils.class);

    private KeycloakUtils() {}

    /**
     * Returns token from Keycloak API
     * @param namespaceName             namespace where keycloak instance is located
     * @param baseURI                   base uri for accessing Keycloak API
     * @param userName                  name of user
     * @param password                  password of user
     * @return user token
     */
    public static String getToken(String namespaceName, String baseURI, String userName, String password) {
        final String testSuiteScraperPodName = KubeResourceManager.get().kubeClient().listPods(Environment.TEST_SUITE_NAMESPACE, SCRAPER_SELECTOR).get(0).getMetadata().getName();

        return new JsonObject(
            executeRequestAndReturnData(
                namespaceName,
                testSuiteScraperPodName,
                new String[]{
                    "curl",
                    "-v",
                    "--insecure",
                    "-X",
                    "POST",
                    "-d", "client_id=admin-cli&client_secret=aGVsbG8td29ybGQtcHJvZHVjZXItc2VjcmV0&grant_type=password&username=" + userName + "&password=" + password,
                    baseURI + "/realms/master/protocol/openid-connect/token"
                }
            )).getString("access_token");
    }

    /**
     * Returns specific realm from Keycloak API
     * @param namespaceName Namespace name
     * @param baseURI base uri for accessing Keycloak API
     * @param token admin token
     * @param desiredRealm realm we want to get
     * @return JsonObject with whole desired realm from Keycloak
     */
    public static JsonObject getKeycloakRealm(String namespaceName, String baseURI, String token, String desiredRealm) {
        final String testSuiteScraperPodName = KubeResourceManager.get().kubeClient().listPods(Environment.TEST_SUITE_NAMESPACE, SCRAPER_SELECTOR).get(0).getMetadata().getName();

        return new JsonObject(executeRequestAndReturnData(
            namespaceName,
            testSuiteScraperPodName,
            new String[]{
                "curl",
                "-v",
                "--insecure",
                "-X",
                "GET",
                baseURI + "/admin/realms/" + desiredRealm,
                "-H", "Authorization: Bearer " + token
            }
        ));
    }

    /**
     * Returns all clients for specific realm
     * @param namespaceName Namespace name
     * @param baseURI base uri for accessing Keycloak API
     * @param token admin token
     * @param desiredRealm realm we want to get clients from
     * @return JsonArray with all clients set for the specific realm
     */
    public static JsonArray getKeycloakRealmClients(String namespaceName, String baseURI, String token, String desiredRealm) {
        final String testSuiteScraperPodName = KubeResourceManager.get().kubeClient().listPods(Environment.TEST_SUITE_NAMESPACE, SCRAPER_SELECTOR).get(0).getMetadata().getName();

        return new JsonArray(executeRequestAndReturnData(
            namespaceName,
            testSuiteScraperPodName,
            new String[]{
                "curl",
                "-v",
                "--insecure",
                "-X",
                "GET",
                baseURI + "/admin/realms/" + desiredRealm + "/clients",
                "-H", "Authorization: Bearer " + token
            }
        ));
    }

    /**
     * Returns all policies from client of specific realm
     * @param namespaceName Namespace name
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
     * @param namespaceName Namespace name
     * @param baseURI base uri for accessing Keycloak API
     * @param token admin token
     * @param desiredRealm realm we want to get clients from
     * @param clientId id of desired client
     * @param endpoint endpoint for "resource" - resource, policy etc.
     * @return JsonArray with results from endpoint
     */
    private static JsonArray getConfigFromResourceServerOfRealm(String namespaceName, String baseURI, String token, String desiredRealm, String clientId, String endpoint) {
        final String testSuiteScraperPodName = KubeResourceManager.get().kubeClient().listPods(Environment.TEST_SUITE_NAMESPACE, SCRAPER_SELECTOR).get(0).getMetadata().getName();

        return new JsonArray(executeRequestAndReturnData(
            namespaceName,
            testSuiteScraperPodName,
            new String[]{
                "curl",
                "-v",
                "--insecure",
                "-X",
                "GET",
                baseURI + "/admin/realms/" + desiredRealm + "/clients/" + clientId + "/authz/resource-server/" + endpoint,
                "-H", "Authorization: Bearer " + token
            })
        );
    }

    /**
     * Puts new configuration to the specific realm
     * @param namespaceName Namespace name
     * @param baseURI base uri for accessing Keycloak API
     * @param token admin token
     * @param desiredRealm realm where the config should be put
     * @param config configuration we want to put into the realm
     */
    public static void putConfigurationToRealm(String namespaceName, String baseURI, String token, JsonObject config, String desiredRealm) {
        final String testSuiteScraperPodName = KubeResourceManager.get().kubeClient().listPods(Environment.TEST_SUITE_NAMESPACE, SCRAPER_SELECTOR).get(0).getMetadata().getName();

        executeRequestAndReturnData(
            namespaceName,
            testSuiteScraperPodName,
            new String[]{
                "curl",
                "-v",
                "--insecure",
                "-X",
                "PUT",
                baseURI + "/admin/realms/" + desiredRealm,
                "-H", "Authorization: Bearer " + token,
                "-d", config.toString(),
                "-H", "Content-Type: application/json"
            }
        );
    }

    /**
     * Updates policies of specific client in realm
     * @param namespaceName Namespace name
     * @param baseURI base uri for accessing Keycloak API
     * @param token admin token
     * @param desiredRealm realm where the client policies should be updated
     * @param policy new updated policies
     * @param clientId id of client where we want to update policies
     */
    public static void updatePolicyOfRealmClient(String namespaceName, String baseURI, String token, JsonObject policy, String desiredRealm, String clientId) {
        final String testSuiteScraperPodName = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(Environment.TEST_SUITE_NAMESPACE, TestConstants.SCRAPER_NAME).get(0).getMetadata().getName();

        executeRequestAndReturnData(
            namespaceName,
            testSuiteScraperPodName,
            new String[]{
                "curl",
                "-v",
                "--insecure",
                "-X",
                "PUT",
                baseURI + "/admin/realms/" + desiredRealm + "/clients/" + clientId + "/authz/resource-server/policy/" + policy.getValue("id"),
                "-H", "Authorization: Bearer " + token,
                "-d", policy.toString(),
                "-H", "Content-Type: application/json"
            }
        );
    }

    /**
     * Imports Keycloak realm
     * @param namespaceName Namespace name
     * @param baseURI base uri for accessing Keycloak API
     * @param token admin token
     * @param realmData realm data/configuration in JSON format
     * @return result of creation
     */
    public static String importRealm(String namespaceName, String baseURI, String token, String realmData) {
        final String testSuiteScraperPodName = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(Environment.TEST_SUITE_NAMESPACE, TestConstants.SCRAPER_NAME).get(0).getMetadata().getName();

        return executeRequestAndReturnData(
            namespaceName,
            testSuiteScraperPodName,
            new String[]{
                "curl",
                "--insecure",
                "-X",
                "POST",
                "-H", "Content-Type: application/json",
                "-d", realmData,
                baseURI + "/admin/realms",
                "-H", "Authorization: Bearer " + token
            }
        );
    }

    public static String executeRequestAndReturnData(String namespaceName, String scraperPodName, String[] request) {
        AtomicReference<String> response = new AtomicReference<>("");

        TestUtils.waitFor("request to Keycloak API will be successful", TestConstants.GLOBAL_POLL_INTERVAL_5_SECS, TestConstants.GLOBAL_STATUS_TIMEOUT, () -> {
            try {
                String commandOutput = KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(scraperPodName, request).out().trim();

                if (!commandOutput.contains("Connection refused")) {
                    response.set(commandOutput);
                    return true;
                }

                return false;
            } catch (Exception e) {
                LOGGER.warn("Exception occurred during doing request on Keycloak API: " + e.getMessage());
                return false;
            }
        });

        return response.get();
    }
}
