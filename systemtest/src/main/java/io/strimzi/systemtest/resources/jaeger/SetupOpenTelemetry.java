/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.jaeger;

import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyBuilder;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.logs.CollectorElement;
import io.strimzi.systemtest.resources.NamespaceManager;
import io.strimzi.systemtest.resources.ResourceItem;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.tracing.TracingConstants;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.test.ReadWriteUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_COLLECTOR_NAME;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_INSTANCE_NAME;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_NAMESPACE;
import static io.strimzi.systemtest.tracing.TracingConstants.OPEN_TELEMETRY_OPERATOR_DEPLOYMENT_NAME;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;

/**
 * Class containing methods for deployment and deletion of OpenTelemetry operator, Cert Manager, and Jaeger instance.
 * Jaeger instances are created for each parallel namespace specified by `namespaceName` parameter.
 */
public class SetupOpenTelemetry {

    private static final Logger LOGGER = LogManager.getLogger(SetupOpenTelemetry.class);

    private static final String CERT_MANAGER_PATH = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tracing/cert-manager.yaml";
    private static final String JAEGER_INSTANCE_PATH = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tracing/jaeger-instance.yaml";
    private static final String OPEN_TELEMETRY_OPERATOR_PATH = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tracing/open-telemetry-operator.yaml";
    private static final String CERT_MANAGER = "cert-manager";
    private static final String JAEGER = "jaeger";

    /**
     * Delete Jaeger instance
     */
    private static void deleteJaeger(String yamlContent) {
        cmdKubeClient().namespace(JAEGER_NAMESPACE).deleteContent(yamlContent);
    }

    /**
     * Encapsulates two methods for deploying Cert Manager and OpenTelemetry operator
     */
    public static void deployOpenTelemetryOperatorAndCertManager() {
        deployAndWaitForCertManager();
        allowNetworkPolicySettingsForCertManagerWebhook();
        deployOpenTelemetryOperator();
        allowNetworkPolicySettingsForOpenTelemetryOperator();
    }

    public static void allowNetworkPolicySettingsForOpenTelemetryOperator() {
        NetworkPolicyResource.allowNetworkPolicySettingsForWebhook(JAEGER_NAMESPACE, OPEN_TELEMETRY_OPERATOR_DEPLOYMENT_NAME, Map.of("app.kubernetes.io/name", TracingConstants.OPEN_TELEMETRY_OPERATOR_NAME));
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

        ResourceManager.STORED_RESOURCES.get(ResourceManager.getTestContext().getDisplayName()).push(new ResourceItem<>(SetupOpenTelemetry::deleteCertManager));
    }

    /**
     * Method that waits for all resources of Cert Manager to be up and running (ready) - Deployment, Webhook, and CA injector
     */
    private static void waitForCertManagerDeployment() {
        DeploymentUtils.waitForDeploymentAndPodsReady(CERT_MANAGER_NAMESPACE, CERT_MANAGER_DEPLOYMENT, 1);
        DeploymentUtils.waitForDeploymentAndPodsReady(CERT_MANAGER_NAMESPACE, CERT_MANAGER_WEBHOOK_DEPLOYMENT, 1);
        DeploymentUtils.waitForDeploymentAndPodsReady(CERT_MANAGER_NAMESPACE, CERT_MANAGER_CA_INJECTOR_DEPLOYMENT, 1);
        LOGGER.info("Cert-manager {}/{} is ready", CERT_MANAGER_NAMESPACE, CERT_MANAGER_DEPLOYMENT);
    }

    /**
     * Encapsulates two other methods - deployment of CertManager and wait for deployment (and all resources of CM) to be ready
     */
    private static void deployAndWaitForCertManager() {
        deployCertManager();
        waitForCertManagerDeployment();
    }

    /**
     * Applies YAML file of OpenTelemetry operator in a loop.
     * Loop is needed because of issue with Cert Manager, that can have problem injecting CA for OpenTelemetry operator
     */
    private static void deployOpenTelemetryOperatorContent() {
        TestUtils.waitFor("OpenTelemetry Operator deploy", JAEGER_DEPLOYMENT_POLL, JAEGER_DEPLOYMENT_TIMEOUT, () -> {
            try {
                String openTelemetryOperator = Files.readString(Paths.get(OPEN_TELEMETRY_OPERATOR_PATH)).replace("opentelemetry-operator-system", JAEGER_NAMESPACE);

                LOGGER.info("Creating OpenTelemetry Operator (and needed resources) from {}", OPEN_TELEMETRY_OPERATOR_PATH);
                cmdKubeClient(JAEGER_NAMESPACE).applyContent(openTelemetryOperator);
                ResourceManager.STORED_RESOURCES.get(ResourceManager.getTestContext().getDisplayName()).push(new ResourceItem<>(() -> deleteJaeger(openTelemetryOperator)));

                return true;
            } catch (Exception e) {
                LOGGER.error("Following exception has been thrown during OpenTelemetry Operator Deployment: {}", e.getMessage());
                return false;
            }
        });
        DeploymentUtils.waitForDeploymentAndPodsReady(JAEGER_NAMESPACE, OPEN_TELEMETRY_OPERATOR_DEPLOYMENT_NAME, 1);
    }

    /**
     * Deploys OpenTelemetry operator and NetworkPolicy needed for its proper function, waits for readiness of NetworkPolicy
     */
    private static void deployOpenTelemetryOperator() {
        LOGGER.info("=== Applying Open Telemetry Operator install files ===");

        // create namespace `jaeger` and add it to stack, to collect logs from it
        NamespaceManager.getInstance().createNamespaceAndPrepare(JAEGER_NAMESPACE, CollectorElement.createCollectorElement(ResourceManager.getTestContext().getRequiredTestClass().getName()));
        deployOpenTelemetryOperatorContent();

        NetworkPolicy networkPolicy = new NetworkPolicyBuilder()
            .withApiVersion("networking.k8s.io/v1")
            .withKind(TestConstants.NETWORK_POLICY)
            .withNewMetadata()
                .withName("jaeger-allow")
                .withNamespace(JAEGER_NAMESPACE)
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
        LOGGER.info("Network policy for Jaeger successfully created");
    }

    /**
     * Install of Jaeger instance
     */
    public static void deployJaegerInstance(String namespaceName) {
        LOGGER.info("=== Applying Jaeger instance install file ===");

        String instanceYamlContent = ReadWriteUtils.readFile(JAEGER_INSTANCE_PATH);

        TestUtils.waitFor("Jaeger Instance deploy", JAEGER_DEPLOYMENT_POLL, JAEGER_DEPLOYMENT_TIMEOUT, () -> {
            try {

                LOGGER.info("Creating Jaeger Instance from {}", JAEGER_INSTANCE_PATH);
                cmdKubeClient(namespaceName).applyContent(instanceYamlContent);

                return true;
            } catch (Exception e) {
                LOGGER.error("Following exception has been thrown during Jaeger Instance Deployment: {}", e.getMessage());
                return false;
            } finally {
                ResourceManager.STORED_RESOURCES.computeIfAbsent(ResourceManager.getTestContext().getDisplayName(), k -> new Stack<>());
                ResourceManager.STORED_RESOURCES.get(ResourceManager.getTestContext().getDisplayName()).push(new ResourceItem<>(() -> cmdKubeClient(namespaceName).deleteContent(instanceYamlContent)));
            }
        });
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, JAEGER_COLLECTOR_NAME, 1);

        NetworkPolicyResource.allowNetworkPolicyBetweenScraperPodAndMatchingLabel(namespaceName, JAEGER_INSTANCE_NAME + "-allow", Map.of(TestConstants.APP_KUBERNETES_NAME_LABEL, JAEGER_COLLECTOR_NAME));
        NetworkPolicyResource.allowNetworkPolicyAllIngressForMatchingLabel(namespaceName, JAEGER_INSTANCE_NAME + "-traces-allow", Map.of(TestConstants.APP_KUBERNETES_NAME_LABEL, JAEGER_COLLECTOR_NAME));
    }
}
