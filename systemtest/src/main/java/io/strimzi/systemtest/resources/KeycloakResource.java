/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.specific.KeycloakUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

import static io.strimzi.systemtest.resources.ResourceManager.CR_CREATION_TIMEOUT;

public class KeycloakResource {

    private static final Logger LOGGER = LogManager.getLogger(KeycloakResource.class);

    private static final String KEYCLOAK_OPERATOR_DEPLOYMENT_NAME = "keycloak-operator";
    private static final String KEYCLOAK_CUSTOM_RESOURCE_NAME = "keycloak";
    private static final String KEYCLOAK_POD_NAME = "keycloak-0";

    public static void keycloakOperator(String namespace) {

        LOGGER.info("Deploying CRDs for Keycloak Operator in {} namespace", ResourceManager.cmdKubeClient().namespace());

        try {
            ResourceManager.cmdKubeClient().apply(FileUtils.downloadAndUnzip("https://raw.githubusercontent.com/keycloak/keycloak-operator/master/deploy/olm-catalog/keycloak-operator/9.0.2/keycloak-operator.v9.0.2.clusterserviceversion.yaml"));
            ResourceManager.cmdKubeClient().apply(FileUtils.downloadAndUnzip("https://raw.githubusercontent.com/keycloak/keycloak-operator/master/deploy/olm-catalog/keycloak-operator/9.0.2/keycloakbackups.keycloak.org.crd.yaml"));
            ResourceManager.cmdKubeClient().apply(FileUtils.downloadAndUnzip("https://raw.githubusercontent.com/keycloak/keycloak-operator/master/deploy/olm-catalog/keycloak-operator/9.0.2/keycloakclients.keycloak.org.crd.yaml"));
            ResourceManager.cmdKubeClient().apply(FileUtils.downloadAndUnzip("https://raw.githubusercontent.com/keycloak/keycloak-operator/master/deploy/olm-catalog/keycloak-operator/9.0.2/keycloakrealms.keycloak.org.crd.yaml"));
            ResourceManager.cmdKubeClient().apply(FileUtils.downloadAndUnzip("https://raw.githubusercontent.com/keycloak/keycloak-operator/master/deploy/olm-catalog/keycloak-operator/9.0.2/keycloaks.keycloak.org.crd.yaml"));
            ResourceManager.cmdKubeClient().apply(FileUtils.downloadAndUnzip("https://raw.githubusercontent.com/keycloak/keycloak-operator/master/deploy/olm-catalog/keycloak-operator/9.0.2/keycloakusers.keycloak.org.crd.yaml"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        ResourceManager.cmdKubeClient().namespace(namespace).apply(KeycloakResource.class.getResource("/keycloak/role.yaml").getFile());
        ResourceManager.cmdKubeClient().namespace(namespace).apply(KeycloakResource.class.getResource("/keycloak/role_binding.yaml").getFile());
        ResourceManager.cmdKubeClient().namespace(namespace).apply(KeycloakResource.class.getResource("/keycloak/service_account.yaml").getFile());

        LOGGER.info("Deploying Keycloak Operator in {} namespace", ResourceManager.cmdKubeClient().namespace());

        ResourceManager.cmdKubeClient().namespace(namespace).apply(KeycloakResource.class.getResource("/keycloak/operator.yaml").getFile());

        TestUtils.waitFor("Keycloak Operator deployment creation", Constants.GLOBAL_POLL_INTERVAL, CR_CREATION_TIMEOUT,
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

        try {
            ResourceManager.cmdKubeClient().delete(FileUtils.downloadAndUnzip("https://raw.githubusercontent.com/keycloak/keycloak-operator/master/deploy/olm-catalog/keycloak-operator/9.0.2/keycloak-operator.v9.0.2.clusterserviceversion.yaml"));
            ResourceManager.cmdKubeClient().delete(FileUtils.downloadAndUnzip("https://raw.githubusercontent.com/keycloak/keycloak-operator/master/deploy/olm-catalog/keycloak-operator/9.0.2/keycloakbackups.keycloak.org.crd.yaml"));
            ResourceManager.cmdKubeClient().delete(FileUtils.downloadAndUnzip("https://raw.githubusercontent.com/keycloak/keycloak-operator/master/deploy/olm-catalog/keycloak-operator/9.0.2/keycloakclients.keycloak.org.crd.yaml"));
            ResourceManager.cmdKubeClient().delete(FileUtils.downloadAndUnzip("https://raw.githubusercontent.com/keycloak/keycloak-operator/master/deploy/olm-catalog/keycloak-operator/9.0.2/keycloakrealms.keycloak.org.crd.yaml"));
            ResourceManager.cmdKubeClient().delete(FileUtils.downloadAndUnzip("https://raw.githubusercontent.com/keycloak/keycloak-operator/master/deploy/olm-catalog/keycloak-operator/9.0.2/keycloaks.keycloak.org.crd.yaml"));
            ResourceManager.cmdKubeClient().delete(FileUtils.downloadAndUnzip("https://raw.githubusercontent.com/keycloak/keycloak-operator/master/deploy/olm-catalog/keycloak-operator/9.0.2/keycloakusers.keycloak.org.crd.yaml"));
        } catch (IOException e) {
            e.printStackTrace();
        }

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
