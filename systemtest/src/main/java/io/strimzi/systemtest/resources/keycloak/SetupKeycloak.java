/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.keycloak;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.systemtest.keycloak.KeycloakInstance;
import io.strimzi.systemtest.resources.ResourceItem;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.systemtest.utils.specific.KeycloakUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.Exec;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class SetupKeycloak {
    private static final List<String> KEYCLOAK_REALMS_FILE_NAMES = List.of("internal_realm.json", "authorization_realm.json", "scope_audience_realm.json");
    private static final String KEYCLOAK_INSTALL_FILES_BASE_PATH = TestUtils.USER_PATH + "/../systemtest/src/test/resources/oauth2";
    private static final String KEYCLOAK_INSTANCE_FILE_PATH = KEYCLOAK_INSTALL_FILES_BASE_PATH + "/keycloak-instance.yaml";
    private static final String KEYCLOAK_DEPLOYMENT_NAME = "example-keycloak";
    private static final String KEYCLOAK_OPERATOR_DEPLOYMENT_NAME = "keycloak-operator";
    private static final String KEYCLOAK_SECRET_NAME = "credential-example-keycloak";

    public final static String PATH_TO_KEYCLOAK_PREPARE_SCRIPT = "../systemtest/src/test/resources/oauth2/prepare_keycloak_operator.sh";
    public final static String PATH_TO_KEYCLOAK_TEARDOWN_SCRIPT = "../systemtest/src/test/resources/oauth2/teardown_keycloak_operator.sh";
    private static final Logger LOGGER = LogManager.getLogger(SetupKeycloak.class);

    public static void deployKeycloakOperator(ExtensionContext extensionContext, final String deploymentNamespace, final String watchNamespace) {
        LOGGER.info("Prepare Keycloak Operator in namespace {} with watching namespace {}", deploymentNamespace, watchNamespace);

        Exec.exec(Level.INFO, "/bin/bash", PATH_TO_KEYCLOAK_PREPARE_SCRIPT, deploymentNamespace, KeycloakUtils.getValidKeycloakVersion(), watchNamespace);
        DeploymentUtils.waitForDeploymentAndPodsReady(deploymentNamespace, KEYCLOAK_OPERATOR_DEPLOYMENT_NAME, 1);

        ResourceManager.STORED_RESOURCES.get(extensionContext.getDisplayName()).push(new ResourceItem<>(() -> deleteKeycloakOperator(deploymentNamespace, watchNamespace)));

        LOGGER.info("Keycloak operator in namespace {} is ready", deploymentNamespace);
    }

    public static void deleteKeycloakOperator(final String deploymentNamespace, final String watchNamespace) {
        LOGGER.info("Teardown Keycloak Operator in namespace {} with watching namespace {}", deploymentNamespace, watchNamespace);
        Exec.exec(Level.INFO, "/bin/bash", PATH_TO_KEYCLOAK_TEARDOWN_SCRIPT, deploymentNamespace, KeycloakUtils.getValidKeycloakVersion(), watchNamespace);
        DeploymentUtils.waitForDeploymentDeletion(deploymentNamespace, KEYCLOAK_OPERATOR_DEPLOYMENT_NAME);
    }

    public static KeycloakInstance deployKeycloakAndImportRealms(ExtensionContext extensionContext, String namespaceName) {
        deployKeycloak(extensionContext, namespaceName);
        KeycloakInstance keycloakInstance = createKeycloakInstance(namespaceName);
        importRealms(namespaceName, keycloakInstance);

        return keycloakInstance;
    }

    private static void deployKeycloak(ExtensionContext extensionContext, String namespaceName) {
        LOGGER.info("Deploying Keycloak instance into namespace: {}", namespaceName);
        cmdKubeClient(namespaceName).apply(KEYCLOAK_INSTANCE_FILE_PATH);

        StatefulSetUtils.waitForAllStatefulSetPodsReady(namespaceName, "keycloak", 1);

        ResourceManager.STORED_RESOURCES.get(extensionContext.getDisplayName()).push(new ResourceItem<>(() -> deleteKeycloak(namespaceName)));

        LOGGER.info("Waiting for Keycloak secret: {} to be present", KEYCLOAK_SECRET_NAME);
        SecretUtils.waitForSecretReady(namespaceName, KEYCLOAK_SECRET_NAME, () -> { });
        LOGGER.info("Keycloak instance and Keycloak secret are ready");
    }

    private static KeycloakInstance createKeycloakInstance(String namespaceName) {
        Secret keycloakSecret = kubeClient().getSecret(namespaceName, KEYCLOAK_SECRET_NAME);

        String usernameEncoded = keycloakSecret.getData().get("ADMIN_USERNAME");
        String username = new String(Base64.getDecoder().decode(usernameEncoded.getBytes()));

        String passwordEncoded = keycloakSecret.getData().get("ADMIN_PASSWORD");
        String password = new String(Base64.getDecoder().decode(passwordEncoded.getBytes()));

        return new KeycloakInstance(username, password, namespaceName);
    }

    private static void importRealms(String namespaceName, KeycloakInstance keycloakInstance) {
        String token = KeycloakUtils.getToken(namespaceName, "https://" + keycloakInstance.getHttpsUri(), keycloakInstance.getUsername(), keycloakInstance.getPassword());

        LOGGER.info("Importing Keycloak realms to Keycloak");
        KEYCLOAK_REALMS_FILE_NAMES.forEach(realmFile -> {
            Path path = Path.of(KEYCLOAK_INSTALL_FILES_BASE_PATH + "/" + realmFile);
            try {
                LOGGER.info("Importing realm from file: {}", path);
                String jsonRealm = new JsonObject(Files.readString(path, StandardCharsets.UTF_8)).encode();
                String result = KeycloakUtils.importRealm(namespaceName, "https://" + keycloakInstance.getHttpsUri(), token, jsonRealm);

                // if KeycloakRealm is successfully imported, the return contains just empty String
                if (!result.isEmpty()) {
                    throw new RuntimeException(String.format("Realm from file path: %s wasn't imported!", path));
                }

                LOGGER.info("Realm successfully imported");
            } catch (IOException e) {
                throw new RuntimeException(String.format("Unable to load file with path: %s due to exception: \n", path) + e);
            }
        });
    }

    private static void deleteKeycloak(String namespaceName) {
        LOGGER.info("Deleting Keycloak in namespace {}", namespaceName);
        cmdKubeClient(namespaceName).delete(KEYCLOAK_INSTANCE_FILE_PATH);
        DeploymentUtils.waitForDeploymentDeletion(namespaceName, KEYCLOAK_DEPLOYMENT_NAME);
    }
}
