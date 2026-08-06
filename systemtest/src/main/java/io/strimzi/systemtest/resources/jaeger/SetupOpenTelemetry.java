/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.jaeger;

import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyBuilder;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.skodjob.kubetest4j.resources.ResourceItem;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.certManager.SetupCertManager;
import io.strimzi.systemtest.tracing.TracingConstants;
import io.strimzi.systemtest.utils.kubeUtils.NamespaceUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.NetworkPolicyUtils;
import io.strimzi.test.ReadWriteUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import static io.strimzi.systemtest.TestConstants.JAEGER_DEPLOYMENT_POLL;
import static io.strimzi.systemtest.TestConstants.JAEGER_DEPLOYMENT_TIMEOUT;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_COLLECTOR_NAME;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_INSTANCE_NAME;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_NAMESPACE;
import static io.strimzi.systemtest.tracing.TracingConstants.OPEN_TELEMETRY_OPERATOR_DEPLOYMENT_NAME;

/**
 * Class containing methods for deployment and deletion of OpenTelemetry operator, Cert Manager, and Jaeger instance.
 * Jaeger instances are created for each parallel namespace specified by `namespaceName` parameter.
 */
public class SetupOpenTelemetry {

    private static final Logger LOGGER = LogManager.getLogger(SetupOpenTelemetry.class);

    private static final String JAEGER_INSTANCE_PATH = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tracing/jaeger-instance.yaml";
    private static final String OPEN_TELEMETRY_OPERATOR_PATH = TestUtils.USER_PATH + "/../systemtest/src/test/resources/tracing/open-telemetry-operator.yaml";

    /**
     * Delete Jaeger instance
     */
    private static void deleteJaeger(String yamlContent) {
        KubeResourceManager.get().kubeCmdClient().inNamespace(JAEGER_NAMESPACE).deleteContent(yamlContent);
    }

    /**
     * Encapsulates the methods for deploying Cert Manager and OpenTelemetry operator
     */
    public static void deployOpenTelemetryOperatorAndCertManager() {
        SetupCertManager.deployCertManager();
        deployOpenTelemetryOperator();
        allowNetworkPolicySettingsForOpenTelemetryOperator();
    }

    public static void allowNetworkPolicySettingsForOpenTelemetryOperator() {
        NetworkPolicyUtils.allowNetworkPolicySettingsForWebhook(JAEGER_NAMESPACE, OPEN_TELEMETRY_OPERATOR_DEPLOYMENT_NAME, Map.of("app.kubernetes.io/name", TracingConstants.OPEN_TELEMETRY_OPERATOR_NAME));
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
                KubeResourceManager.get().kubeCmdClient().inNamespace(JAEGER_NAMESPACE).applyContent(openTelemetryOperator);
                KubeResourceManager.get().pushToStack(new ResourceItem<>(() -> deleteJaeger(openTelemetryOperator)));
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

        // create namespace `jaeger`
        NamespaceUtils.createNamespaceAndPrepare(JAEGER_NAMESPACE);
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
        KubeResourceManager.get().createResourceWithWait(networkPolicy);
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
                KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).applyContent(instanceYamlContent);

                return true;
            } catch (Exception e) {
                LOGGER.error("Following exception has been thrown during Jaeger Instance Deployment: {}", e.getMessage());
                return false;
            } finally {
                KubeResourceManager.get().pushToStack(new ResourceItem<>(() -> KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).deleteContent(instanceYamlContent)));
            }
        });
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, JAEGER_COLLECTOR_NAME, 1);

        NetworkPolicyUtils.allowNetworkPolicyBetweenScraperPodAndMatchingLabel(namespaceName, JAEGER_INSTANCE_NAME + "-allow", Map.of(TestConstants.APP_KUBERNETES_NAME_LABEL, JAEGER_COLLECTOR_NAME));
        NetworkPolicyUtils.allowNetworkPolicyAllIngressForMatchingLabel(namespaceName, JAEGER_INSTANCE_NAME + "-traces-allow", Map.of(TestConstants.APP_KUBERNETES_NAME_LABEL, JAEGER_COLLECTOR_NAME));
    }
}
