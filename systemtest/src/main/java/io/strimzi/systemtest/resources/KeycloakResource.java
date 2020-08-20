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
    private static final String KEYCLOAK_CUSTOM_RESOURCE_NAME = "example-keycloak";
    private static final String KEYCLOAK_POD_NAME = "keycloak-0";

    public static void keycloakOperator(String namespace) {

        LOGGER.info("Deploying CRDs for Keycloak Operator in {} namespace", ResourceManager.cmdKubeClient().namespace());

        try {
            ResourceManager.cmdKubeClient().namespace(namespace).apply(FileUtils.downloadYaml("https://raw.githubusercontent.com/keycloak/keycloak-operator/11.0.0/deploy/cluster_roles/cluster_role.yaml"));
            ResourceManager.cmdKubeClient().namespace(namespace).apply(FileUtils.downloadYaml("https://raw.githubusercontent.com/keycloak/keycloak-operator/11.0.0/deploy/cluster_roles/cluster_role_binding.yaml"));
            ResourceManager.cmdKubeClient().namespace(namespace).apply(FileUtils.downloadYaml("https://raw.githubusercontent.com/keycloak/keycloak-operator/11.0.0/deploy/crds/keycloak.org_keycloakbackups_crd.yaml"));
            ResourceManager.cmdKubeClient().namespace(namespace).apply(FileUtils.downloadYaml("https://raw.githubusercontent.com/keycloak/keycloak-operator/11.0.0/deploy/crds/keycloak.org_keycloakclients_crd.yaml"));
            ResourceManager.cmdKubeClient().namespace(namespace).apply(FileUtils.downloadYaml("https://raw.githubusercontent.com/keycloak/keycloak-operator/11.0.0/deploy/crds/keycloak.org_keycloakrealms_crd.yaml"));
            ResourceManager.cmdKubeClient().namespace(namespace).apply(FileUtils.downloadYaml("https://raw.githubusercontent.com/keycloak/keycloak-operator/11.0.0/deploy/crds/keycloak.org_keycloaks_crd.yaml"));
            ResourceManager.cmdKubeClient().namespace(namespace).apply(FileUtils.downloadYaml("https://raw.githubusercontent.com/keycloak/keycloak-operator/11.0.0/deploy/crds/keycloak.org_keycloakusers_crd.yaml"));

            ResourceManager.cmdKubeClient().namespace(namespace).apply(FileUtils.downloadYaml("https://raw.githubusercontent.com/keycloak/keycloak-operator/11.0.0/deploy/role.yaml"));
            ResourceManager.cmdKubeClient().namespace(namespace).apply(FileUtils.downloadYaml("https://raw.githubusercontent.com/keycloak/keycloak-operator/11.0.0/deploy/role_binding.yaml"));
            ResourceManager.cmdKubeClient().namespace(namespace).apply(FileUtils.downloadYaml("https://raw.githubusercontent.com/keycloak/keycloak-operator/11.0.0/deploy/service_account.yaml"));

            LOGGER.info("Deploying Keycloak Operator in {} namespace", ResourceManager.cmdKubeClient().namespace());

            ResourceManager.cmdKubeClient().namespace(namespace).apply(FileUtils.downloadYaml("https://raw.githubusercontent.com/keycloak/keycloak-operator/11.0.0/deploy/operator.yaml"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        TestUtils.waitFor("Keycloak Operator deployment creation", Constants.GLOBAL_POLL_INTERVAL, CR_CREATION_TIMEOUT,
            () -> ResourceManager.kubeClient().getDeployment(KEYCLOAK_OPERATOR_DEPLOYMENT_NAME) != null);
        String deploymentName = ResourceManager.kubeClient().getDeployment(KEYCLOAK_OPERATOR_DEPLOYMENT_NAME).getMetadata().getName();

        // schedule deletion after teardown
        ResourceManager.getPointerResources().push(() -> deleteKeycloakOperator(deploymentName, namespace));
        // Wait for operator creation
        waitForKeycloakOperator(deploymentName, namespace, 1);
    }

    public static void deployKeycloak(String namespace) {

        try {
            ResourceManager.cmdKubeClient().namespace(namespace).apply(FileUtils.downloadYaml("https://raw.githubusercontent.com/keycloak/keycloak-operator/11.0.0/deploy/examples/keycloak/keycloak.yaml"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        waitForKeycloak(namespace);

        // schedule deletion after teardown
        ResourceManager.getPointerResources().push(() -> deleteKeycloak(namespace));
    }

    private static void deleteKeycloakOperator(String deploymentName, String namespace) {

        try {
            ResourceManager.cmdKubeClient().namespace(namespace).delete(FileUtils.downloadYaml("https://raw.githubusercontent.com/keycloak/keycloak-operator/11.0.0/deploy/cluster_roles/cluster_role.yaml"));
            ResourceManager.cmdKubeClient().namespace(namespace).delete(FileUtils.downloadYaml("https://raw.githubusercontent.com/keycloak/keycloak-operator/11.0.0/deploy/cluster_roles/cluster_role_binding.yaml"));
            ResourceManager.cmdKubeClient().namespace(namespace).delete(FileUtils.downloadYaml("https://raw.githubusercontent.com/keycloak/keycloak-operator/11.0.0/deploy/crds/keycloak.org_keycloakbackups_crd.yaml"));
            ResourceManager.cmdKubeClient().namespace(namespace).delete(FileUtils.downloadYaml("https://raw.githubusercontent.com/keycloak/keycloak-operator/11.0.0/deploy/crds/keycloak.org_keycloakclients_crd.yaml"));
            ResourceManager.cmdKubeClient().namespace(namespace).delete(FileUtils.downloadYaml("https://raw.githubusercontent.com/keycloak/keycloak-operator/11.0.0/deploy/crds/keycloak.org_keycloakrealms_crd.yaml"));
            ResourceManager.cmdKubeClient().namespace(namespace).delete(FileUtils.downloadYaml("https://raw.githubusercontent.com/keycloak/keycloak-operator/11.0.0/deploy/crds/keycloak.org_keycloaks_crd.yaml"));
            ResourceManager.cmdKubeClient().namespace(namespace).delete(FileUtils.downloadYaml("https://raw.githubusercontent.com/keycloak/keycloak-operator/11.0.0/deploy/crds/keycloak.org_keycloakusers_crd.yaml"));

            ResourceManager.cmdKubeClient().namespace(namespace).delete(FileUtils.downloadYaml("https://raw.githubusercontent.com/keycloak/keycloak-operator/11.0.0/deploy/role.yaml"));
            ResourceManager.cmdKubeClient().namespace(namespace).delete(FileUtils.downloadYaml("https://raw.githubusercontent.com/keycloak/keycloak-operator/11.0.0/deploy/role_binding.yaml"));
            ResourceManager.cmdKubeClient().namespace(namespace).delete(FileUtils.downloadYaml("https://raw.githubusercontent.com/keycloak/keycloak-operator/11.0.0/deploy/service_account.yaml"));

            LOGGER.info("Deploying Keycloak Operator in {} namespace", ResourceManager.cmdKubeClient().namespace());

            ResourceManager.cmdKubeClient().namespace(namespace).delete(FileUtils.downloadYaml("https://raw.githubusercontent.com/keycloak/keycloak-operator/11.0.0/deploy/operator.yaml"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        DeploymentUtils.waitForDeploymentDeletion(deploymentName);
    }

    private static void deleteKeycloak(String namespace) {

        try {
            ResourceManager.cmdKubeClient().namespace(namespace).delete(FileUtils.downloadYaml("https://raw.githubusercontent.com/keycloak/keycloak-operator/11.0.0/deploy/examples/keycloak/keycloak.yaml"));
        } catch (IOException e) {
            e.printStackTrace();
        }

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
        KeycloakUtils.waitUntilKeycloakCustomResourceReady(namespace, KEYCLOAK_CUSTOM_RESOURCE_NAME, "ready: true");
        LOGGER.info("Pod {} in namespace {} is ready", KEYCLOAK_POD_NAME, namespace);
    }
}
