/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.keycloak;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.skodjob.testframe.resources.ResourceItem;
import io.strimzi.operator.common.Util;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.keycloak.KeycloakInstance;
import io.strimzi.systemtest.templates.kubernetes.NetworkPolicyTemplates;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.NetworkPolicyUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.systemtest.utils.specific.KeycloakUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.Exec;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public class SetupKeycloak {
    private static final List<String> KEYCLOAK_REALMS_FILE_NAMES = List.of("internal_realm.json", "authorization_realm.json", "scope_audience_realm.json");
    private static final String KEYCLOAK_INSTALL_FILES_BASE_PATH = TestUtils.USER_PATH + "/../systemtest/src/test/resources/oauth2";
    private static final String KEYCLOAK_INSTANCE_FILE_PATH = KEYCLOAK_INSTALL_FILES_BASE_PATH + "/keycloak-instance.yaml";
    private static final String POSTGRES_FILE_PATH = KEYCLOAK_INSTALL_FILES_BASE_PATH + "/postgres.yaml";
    private static final String KEYCLOAK_DEPLOYMENT_NAME = "example-keycloak";
    private static final String KEYCLOAK_OPERATOR_DEPLOYMENT_NAME = "keycloak-operator";
    private static final String KEYCLOAK_SECRET_NAME = "keycloak-initial-admin";
    private static final String POSTGRES_SECRET_NAME = "keycloak-db-secret";
    private static final String POSTGRES_USER_NAME = "arnost";
    private static final String POSTGRES_PASSWORD = "changeme";

    public final static String PATH_TO_KEYCLOAK_PREPARE_SCRIPT = "../systemtest/src/test/resources/oauth2/prepare_keycloak_operator.sh";
    public final static String PATH_TO_KEYCLOAK_TEARDOWN_SCRIPT = "../systemtest/src/test/resources/oauth2/teardown_keycloak_operator.sh";

    private static final String KEYCLOAK = "keycloak";
    private static final String POSTGRES = "postgres";

    private static final Logger LOGGER = LogManager.getLogger(SetupKeycloak.class);

    public static void deployKeycloakOperator(final String deploymentNamespace, final String watchNamespace) {
        LOGGER.info("Preparing Keycloak Operator in Namespace: {} while watching Namespace: {}", deploymentNamespace, watchNamespace);

        Exec.exec(Level.INFO, "/bin/bash", PATH_TO_KEYCLOAK_PREPARE_SCRIPT, deploymentNamespace, KeycloakUtils.LATEST_KEYCLOAK_VERSION, watchNamespace);
        DeploymentUtils.waitForDeploymentAndPodsReady(deploymentNamespace, KEYCLOAK_OPERATOR_DEPLOYMENT_NAME, 1);

        KubeResourceManager.get().pushToStack(new ResourceItem<>(() -> deleteKeycloakOperator(deploymentNamespace, watchNamespace)));

        LOGGER.info("Keycloak Operator in Namespace: {} is ready", deploymentNamespace);
    }

    public static void deleteKeycloakOperator(final String deploymentNamespace, final String watchNamespace) {
        LOGGER.info("Tearing down Keycloak Operator in Namespace: {} with watching Namespace: {}", deploymentNamespace, watchNamespace);
        Exec.exec(Level.INFO, "/bin/bash", PATH_TO_KEYCLOAK_TEARDOWN_SCRIPT, deploymentNamespace, KeycloakUtils.LATEST_KEYCLOAK_VERSION, watchNamespace);
        DeploymentUtils.waitForDeploymentDeletion(deploymentNamespace, KEYCLOAK_OPERATOR_DEPLOYMENT_NAME);
    }

    public static KeycloakInstance deployKeycloakAndImportRealms(String namespaceName) {
        deployPostgres(namespaceName);
        allowNetworkPolicyBetweenKeycloakAndPostgres(namespaceName);
        deployKeycloak(namespaceName);

        KeycloakInstance keycloakInstance = createKeycloakInstance(namespaceName);
        NetworkPolicyUtils.allowNetworkPolicyAllIngressForMatchingLabel(namespaceName, KEYCLOAK + "-allow", Map.of(TestConstants.APP_POD_LABEL, KEYCLOAK));
        importRealms(namespaceName, keycloakInstance);

        return keycloakInstance;
    }

    private static void deployKeycloak(String namespaceName) {
        LOGGER.info("Deploying Keycloak instance into Namespace: {}", namespaceName);
        KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).apply(KEYCLOAK_INSTANCE_FILE_PATH);

        StatefulSetUtils.waitForAllStatefulSetPodsReady(namespaceName, "keycloak", 1);

        KubeResourceManager.get().pushToStack(new ResourceItem<>(() -> deleteKeycloak(namespaceName)));

        LOGGER.info("Waiting for Keycloak Secret: {}/{} to be present", namespaceName, KEYCLOAK_SECRET_NAME);
        SecretUtils.waitForSecretReady(namespaceName, KEYCLOAK_SECRET_NAME, () -> { });
        LOGGER.info("Keycloak instance and Keycloak Secret are ready");
    }

    private static void deployPostgres(String namespaceName) {
        LOGGER.info("Deploying Postgres into Namespace: {}", namespaceName);

        try {
            final String postgresYaml =  Files.readString(Paths.get(POSTGRES_FILE_PATH)).replace(
                "${POSTGRES_IMAGE}", Environment.POSTGRES_IMAGE
            );
            KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).applyContent(postgresYaml);
        } catch (IOException e) {
            throw new RuntimeException("Failed to update the Postgres deployment YAML", e);
        }

        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, "postgres", 1);

        KubeResourceManager.get().pushToStack(new ResourceItem<>(() -> deletePostgres(namespaceName)));

        Secret postgresSecret = new SecretBuilder()
            .withNewMetadata()
                .withName(POSTGRES_SECRET_NAME)
                .withNamespace(namespaceName)
            .endMetadata()
            .withType("Opaque")
            .addToData("username", Base64.getEncoder().encodeToString(POSTGRES_USER_NAME.getBytes(StandardCharsets.UTF_8)))
            .addToData("password", Base64.getEncoder().encodeToString(POSTGRES_PASSWORD.getBytes(StandardCharsets.UTF_8)))
            .build();

        KubeResourceManager.get().createResourceWithWait(postgresSecret);
    }

    private static KeycloakInstance createKeycloakInstance(String namespaceName) {
        Secret keycloakSecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(namespaceName).withName(KEYCLOAK_SECRET_NAME).get();

        String usernameEncoded = keycloakSecret.getData().get("username");
        String username = Util.decodeFromBase64(usernameEncoded, StandardCharsets.UTF_8);

        String passwordEncoded = keycloakSecret.getData().get("password");
        String password = Util.decodeFromBase64(passwordEncoded, StandardCharsets.UTF_8);

        return new KeycloakInstance(namespaceName, password, username);
    }

    private static void importRealms(String keycloakNamespace, KeycloakInstance keycloakInstance) {
        String token = KeycloakUtils.getToken(keycloakNamespace, "https://" + keycloakInstance.getHttpsUri(), keycloakInstance.getUsername(), keycloakInstance.getPassword());

        LOGGER.info("Importing Keycloak realms to Keycloak");
        KEYCLOAK_REALMS_FILE_NAMES.forEach(realmFile -> {
            Path path = Path.of(KEYCLOAK_INSTALL_FILES_BASE_PATH + "/" + realmFile);
            try {
                LOGGER.info("Importing realm from file: {}", path);
                String jsonRealm = new JsonObject(Files.readString(path, StandardCharsets.UTF_8)).encode();
                String result = KeycloakUtils.importRealm(keycloakNamespace, "https://" + keycloakInstance.getHttpsUri(), token, jsonRealm);

                // if KeycloakRealm is successfully imported, the return contains just empty String
                if (!result.isEmpty()) {
                    throw new RuntimeException(String.format("Realm from file path: %s wasn't imported!", path));
                }

                LOGGER.info("Realm successfully imported");
            } catch (IOException e) {
                throw new RuntimeException(String.format("Unable to load file with path: %s due to exception: %n", path) + e);
            }
        });
    }

    public static void allowNetworkPolicyBetweenKeycloakAndPostgres(String namespaceName) {
        if (Environment.DEFAULT_TO_DENY_NETWORK_POLICIES) {
            LabelSelector labelSelector = new LabelSelectorBuilder()
                .addToMatchLabels(TestConstants.APP_POD_LABEL, KEYCLOAK)
                .build();

            LOGGER.info("Apply NetworkPolicy access to {} from Pods with LabelSelector {}", KEYCLOAK, labelSelector);

            NetworkPolicy networkPolicy = NetworkPolicyTemplates.networkPolicyBuilder(namespaceName, KEYCLOAK + "-" + POSTGRES, labelSelector)
                .editSpec()
                    .withNewPodSelector()
                       .addToMatchLabels(TestConstants.APP_POD_LABEL, POSTGRES)
                    .endPodSelector()
                .endSpec()
                .build();

            KubeResourceManager.get().createResourceWithWait(networkPolicy);
        }
    }

    private static void deleteKeycloak(String namespaceName) {
        LOGGER.info("Deleting Keycloak in Namespace: {}", namespaceName);
        KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).delete(KEYCLOAK_INSTANCE_FILE_PATH);
        SecretUtils.deleteSecretWithWait(namespaceName, KEYCLOAK_SECRET_NAME);
        DeploymentUtils.waitForDeploymentDeletion(namespaceName, KEYCLOAK_DEPLOYMENT_NAME);
    }

    private static void deletePostgres(String namespaceName) {
        LOGGER.info("Deleting Postgres in Namespace: {}", namespaceName);
        KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).delete(POSTGRES_FILE_PATH);
        SecretUtils.deleteSecretWithWait(namespaceName, POSTGRES_SECRET_NAME);
        DeploymentUtils.waitForDeploymentDeletion(namespaceName, "postgres");
    }
}
