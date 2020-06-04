/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.specific.KeycloakUtils;
import io.strimzi.test.TestUtils;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class KeycloakResource {

    private static final Logger LOGGER = LogManager.getLogger(KeycloakResource.class);

    private static final String KEYCLOAK_OPERATOR_DEPLOYMENT_NAME = "keycloak-operator";
    private static final String KEYCLOAK_CUSTOM_RESOURCE_NAME = "keycloak";
    private static final String KEYCLOAK_POD_NAME = "keycloak-0";

    public static void keycloakOperator(String namespace) {

        LOGGER.info("Deploying CRDs for Keycloak Operator in {} namespace", ResourceManager.cmdKubeClient().namespace());

        ResourceManager.cmdKubeClient().namespace(namespace).apply(KeycloakResource.class.getResource("/keycloak/crds/keycloak.org_keycloakbackups_crd.yaml").getFile());
        ResourceManager.cmdKubeClient().namespace(namespace).apply(KeycloakResource.class.getResource("/keycloak/crds/keycloak.org_keycloakclients_crd.yaml").getFile());
        ResourceManager.cmdKubeClient().namespace(namespace).apply(KeycloakResource.class.getResource("/keycloak/crds/keycloak.org_keycloakrealms_crd.yaml").getFile());
        ResourceManager.cmdKubeClient().namespace(namespace).apply(KeycloakResource.class.getResource("/keycloak/crds/keycloak.org_keycloaks_crd.yaml").getFile());
        ResourceManager.cmdKubeClient().namespace(namespace).apply(KeycloakResource.class.getResource("/keycloak/crds/keycloak.org_keycloakusers_crd.yaml").getFile());

        ResourceManager.cmdKubeClient().namespace(namespace).apply(KeycloakResource.class.getResource("/keycloak/role.yaml").getFile());
        ResourceManager.cmdKubeClient().namespace(namespace).apply(KeycloakResource.class.getResource("/keycloak/role_binding.yaml").getFile());
        ResourceManager.cmdKubeClient().namespace(namespace).apply(KeycloakResource.class.getResource("/keycloak/service_account.yaml").getFile());

        LOGGER.info("Deploying Keycloak Operator in {} namespace", ResourceManager.cmdKubeClient().namespace());

        ResourceManager.cmdKubeClient().namespace(namespace).apply(KeycloakResource.class.getResource("/keycloak/operator.yaml").getFile());

        TestUtils.waitFor("Keycloak Operator deployment creation", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_RESOURCE_CREATION,
            () -> ResourceManager.kubeClient().getDeployment(KEYCLOAK_OPERATOR_DEPLOYMENT_NAME) != null);
        String deploymentName = ResourceManager.kubeClient().getDeployment(KEYCLOAK_OPERATOR_DEPLOYMENT_NAME).getMetadata().getName();

        // schedule deletion after teardown
        ResourceManager.getPointerResources().push(() -> deleteKeycloakOperator(deploymentName, namespace));
        // Wait for operator creation
        waitForKeycloakOperator(deploymentName, namespace, 1);
    }

    public static void deployKeycloak(String namespace) {
        ResourceManager.cmdKubeClient().namespace(namespace).apply(KeycloakResource.class.getResource("/keycloak/keycloak_instance.yaml").getFile());
        waitForKeycloak(namespace);

        // schedule deletion after teardown
        ResourceManager.getPointerResources().push(() -> deleteKeycloak(namespace));
    }

    private static void deleteKeycloakOperator(String deploymentName, String namespace) {
        // TODO: remove CRDs from the resource and apply it from the github with specific version 9.0.2

        ResourceManager.cmdKubeClient().namespace(namespace).delete(KeycloakResource.class.getResource("/keycloak/crds/keycloak.org_keycloakbackups_crd.yaml").getFile());
        ResourceManager.cmdKubeClient().namespace(namespace).delete(KeycloakResource.class.getResource("/keycloak/crds/keycloak.org_keycloakclients_crd.yaml").getFile());
        ResourceManager.cmdKubeClient().namespace(namespace).delete(KeycloakResource.class.getResource("/keycloak/crds/keycloak.org_keycloakrealms_crd.yaml").getFile());
        ResourceManager.cmdKubeClient().namespace(namespace).delete(KeycloakResource.class.getResource("/keycloak/crds/keycloak.org_keycloaks_crd.yaml").getFile());
        ResourceManager.cmdKubeClient().namespace(namespace).delete(KeycloakResource.class.getResource("/keycloak/crds/keycloak.org_keycloakusers_crd.yaml").getFile());

        ResourceManager.cmdKubeClient().namespace(namespace).delete(KeycloakResource.class.getResource("/keycloak/role.yaml").getFile());
        ResourceManager.cmdKubeClient().namespace(namespace).delete(KeycloakResource.class.getResource("/keycloak/role_binding.yaml").getFile());
        ResourceManager.cmdKubeClient().namespace(namespace).delete(KeycloakResource.class.getResource("/keycloak/service_account.yaml").getFile());

        ResourceManager.cmdKubeClient().namespace(namespace).delete(KeycloakResource.class.getResource("/keycloak/operator.yaml").getFile());

        DeploymentUtils.waitForDeploymentDeletion(deploymentName);
    }

    private static void deleteKeycloak(String namespace) {
        ResourceManager.cmdKubeClient().namespace(namespace).delete(KeycloakResource.class.getResource("/keycloak/keycloak_instance.yaml").getFile());

        KeycloakUtils.waitUntilKeycloakCustomResourceDeletion(namespace, KEYCLOAK_CUSTOM_RESOURCE_NAME);
    }

    private static void waitForKeycloakOperator(String deploymentName, String namespace, int replicas) {
        LOGGER.info("Waiting for deployment {} in namespace {}", deploymentName, namespace);
        DeploymentUtils.waitForDeploymentAndPodsReady(deploymentName, replicas);
        LOGGER.info("Deployment {} in namespace {} is ready", deploymentName, namespace);
    }

    private static void waitForKeycloak(String namespace) {
        LOGGER.info("Waiting for keycloak pod {} in namespace {}", KEYCLOAK_POD_NAME, namespace);
        PodUtils.waitUntilPodIsPresent(KEYCLOAK_POD_NAME);
        LOGGER.info("Waiting for keycloak pod {} in namespace {} is ready", KEYCLOAK_POD_NAME, namespace);
        PodUtils.verifyThatRunningPodsAreStable(KEYCLOAK_POD_NAME);
        LOGGER.info("Pod {} in namespace {} is ready", KEYCLOAK_POD_NAME, namespace);
    }
}
