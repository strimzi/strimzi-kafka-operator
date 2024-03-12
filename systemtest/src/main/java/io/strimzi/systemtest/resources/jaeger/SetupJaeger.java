/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.jaeger;

import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyBuilder;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.NamespaceManager;
import io.strimzi.systemtest.resources.ResourceItem;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.logs.CollectorElement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Stack;

import static io.strimzi.systemtest.TestConstants.JAEGER_DEPLOYMENT_POLL;
import static io.strimzi.systemtest.TestConstants.JAEGER_DEPLOYMENT_TIMEOUT;
import static io.strimzi.systemtest.tracing.TracingConstants.CERT_MANAGER_CA_INJECTOR_DEPLOYMENT;
import static io.strimzi.systemtest.tracing.TracingConstants.CERT_MANAGER_DEPLOYMENT;
import static io.strimzi.systemtest.tracing.TracingConstants.CERT_MANAGER_NAMESPACE;
import static io.strimzi.systemtest.tracing.TracingConstants.CERT_MANAGER_WEBHOOK_DEPLOYMENT;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_INSTANCE_NAME;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_OPERATOR_DEPLOYMENT_NAME;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;

/**
 * Class containing methods for deployment and deletion of Jaeger operator, Cert Manager, and Jaeger instance.
 * Jaeger instances are created for each parallel namespace specified by `namespaceName` parameter.
 */
public class SetupJaeger {

    private static final Logger LOGGER = LogManager.getLogger(SetupJaeger.class);

    private static final String CERT_MANAGER_PATH = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tracing/cert-manager.yaml";
    private static final String JAEGER_INSTANCE_PATH = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tracing/jaeger-instance.yaml";
    private static final String JAEGER_OPERATOR_PATH = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tracing/jaeger-operator.yaml";
    private static final String CERT_MANAGER = "cert-manager";
    private static final String JAEGER = "jaeger";

    /**
     * Delete Jaeger instance
     */
    private static void deleteJaeger(String yamlContent) {
        cmdKubeClient().namespace(Environment.TEST_SUITE_NAMESPACE).deleteContent(yamlContent);
    }

    /**
     * Encapsulates two methods for deploying Cert Manager and Jaeger operator
     */
    public static void deployJaegerOperatorAndCertManager() {
        deployAndWaitForCertManager();
        allowNetworkPolicySettingsForCertManagerWebhook();
        deployJaegerOperator();
        allowNetworkPolicySettingsForJaegerOperator();
    }

    public static void allowNetworkPolicySettingsForJaegerOperator() {
        NetworkPolicyResource.allowNetworkPolicySettingsForWebhook(Environment.TEST_SUITE_NAMESPACE, JAEGER_OPERATOR_DEPLOYMENT_NAME, Map.of("name", JAEGER_OPERATOR_DEPLOYMENT_NAME));
    }

    public static void allowNetworkPolicySettingsForCertManagerWebhook() {
        NetworkPolicyResource.allowNetworkPolicySettingsForWebhook(CERT_MANAGER_NAMESPACE, CERT_MANAGER, Map.of(TestConstants.APP_KUBERNETES_INSTANCE_LABEL, CERT_MANAGER));
    }

    /**
     * Deletes all Cert Manager resources and waits for their deletion
     */
    private static void deleteCertManager() {
        cmdKubeClient().delete(CERT_MANAGER_PATH);
        DeploymentUtils.waitForDeploymentDeletion(CERT_MANAGER_NAMESPACE, CERT_MANAGER_DEPLOYMENT);
        DeploymentUtils.waitForDeploymentDeletion(CERT_MANAGER_NAMESPACE, CERT_MANAGER_WEBHOOK_DEPLOYMENT);
        DeploymentUtils.waitForDeploymentDeletion(CERT_MANAGER_NAMESPACE, CERT_MANAGER_CA_INJECTOR_DEPLOYMENT);
    }

    /**
     * Deploys Cert Manager and adds it to the stack of resources to be deleted on clean up
     */
    private static void deployCertManager() {
        // create namespace `cert-manager` and add it to stack, to collect logs from it
        NamespaceManager.getInstance().createNamespaceAndPrepare(CERT_MANAGER_NAMESPACE, CollectorElement.createCollectorElement(ResourceManager.getTestContext().getRequiredTestClass().getName()));

        LOGGER.info("Deploying CertManager from {}", CERT_MANAGER_PATH);
        // because we don't want to apply CertManager's file to specific namespace, passing the empty String will do the trick
        cmdKubeClient("").apply(CERT_MANAGER_PATH);

        ResourceManager.STORED_RESOURCES.get(ResourceManager.getTestContext().getDisplayName()).push(new ResourceItem<>(SetupJaeger::deleteCertManager));
    }

    /**
     * Method that waits for all resources of Cert Manager to be up and running (ready) - Deployment, Webhook, and CA injector
     */
    private static void waitForCertManagerDeployment() {
        DeploymentUtils.waitForDeploymentAndPodsReady(CERT_MANAGER_NAMESPACE, CERT_MANAGER_DEPLOYMENT, 1);
        DeploymentUtils.waitForDeploymentAndPodsReady(CERT_MANAGER_NAMESPACE, CERT_MANAGER_WEBHOOK_DEPLOYMENT, 1);
        DeploymentUtils.waitForDeploymentAndPodsReady(CERT_MANAGER_NAMESPACE, CERT_MANAGER_CA_INJECTOR_DEPLOYMENT, 1);
    }

    /**
     * Encapsulates two other methods - deployment of CertManager and wait for deployment (and all resources of CM) to be ready
     */
    private static void deployAndWaitForCertManager() {
        deployCertManager();
        waitForCertManagerDeployment();
    }

    /**
     * Applies YAML file of Jaeger operator in a loop.
     * Loop is needed because of issue with Cert Manager, that can have problem injecting CA for Jaeger operator
     */
    private static void deployJaegerContent() {
        TestUtils.waitFor("Jaeger deploy", JAEGER_DEPLOYMENT_POLL, JAEGER_DEPLOYMENT_TIMEOUT, () -> {
            try {
                String jaegerOperator = Files.readString(Paths.get(JAEGER_OPERATOR_PATH)).replace("observability", Environment.TEST_SUITE_NAMESPACE);

                LOGGER.info("Creating Jaeger Operator (and needed resources) from {}", JAEGER_OPERATOR_PATH);
                cmdKubeClient(Environment.TEST_SUITE_NAMESPACE).applyContent(jaegerOperator);
                ResourceManager.STORED_RESOURCES.get(ResourceManager.getTestContext().getDisplayName()).push(new ResourceItem<>(() -> deleteJaeger(jaegerOperator)));

                return true;
            } catch (Exception e) {
                LOGGER.error("Following exception has been thrown during Jaeger Deployment: {}", e.getMessage());
                return false;
            }
        });
        DeploymentUtils.waitForDeploymentAndPodsReady(Environment.TEST_SUITE_NAMESPACE, JAEGER_OPERATOR_DEPLOYMENT_NAME, 1);
    }

    /**
     * Deploys Jaeger operator and NetworkPolicy needed for its proper function, waits for readiness of NetworkPolicy
     */
    private static void deployJaegerOperator() {
        LOGGER.info("=== Applying Jaeger Operator install files ===");

        deployJaegerContent();

        NetworkPolicy networkPolicy = new NetworkPolicyBuilder()
            .withApiVersion("networking.k8s.io/v1")
            .withKind(TestConstants.NETWORK_POLICY)
            .withNewMetadata()
                .withName("jaeger-allow")
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .addNewIngress()
                .endIngress()
                .withNewPodSelector()
                    .addToMatchLabels("app", "jaeger")
                .endPodSelector()
                .withPolicyTypes("Ingress")
            .endSpec()
            .build();

        LOGGER.debug("Creating NetworkPolicy: {}", networkPolicy.toString());
        ResourceManager.getInstance().createResourceWithWait(networkPolicy);
        LOGGER.info("Network policy for jaeger successfully created");
    }

    /**
     * Install of Jaeger instance
     */
    public static void deployJaegerInstance(String namespaceName) {
        LOGGER.info("=== Applying jaeger instance install file ===");

        String instanceYamlContent = TestUtils.getContent(new File(JAEGER_INSTANCE_PATH), TestUtils::toYamlString);
        cmdKubeClient(namespaceName).applyContent(instanceYamlContent);

        ResourceManager.STORED_RESOURCES.computeIfAbsent(ResourceManager.getTestContext().getDisplayName(), k -> new Stack<>());
        ResourceManager.STORED_RESOURCES.get(ResourceManager.getTestContext().getDisplayName()).push(new ResourceItem<>(() -> cmdKubeClient(namespaceName).deleteContent(instanceYamlContent)));

        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, JAEGER_INSTANCE_NAME, 1);

        NetworkPolicyResource.allowNetworkPolicyBetweenScraperPodAndMatchingLabel(namespaceName, JAEGER_INSTANCE_NAME + "-allow", Map.of(TestConstants.APP_POD_LABEL, JAEGER));
        NetworkPolicyResource.allowNetworkPolicyAllIngressForMatchingLabel(namespaceName, JAEGER_INSTANCE_NAME + "-traces-allow", Map.of(TestConstants.APP_POD_LABEL, JAEGER));
    }
}
