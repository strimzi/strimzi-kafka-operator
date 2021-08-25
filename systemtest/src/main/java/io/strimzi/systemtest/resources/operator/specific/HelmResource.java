/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.operator.specific;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.ResourceItem;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.specific.BridgeUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;

public class HelmResource implements SpecificResourceType {

    public static final String HELM_CHART = TestUtils.USER_PATH + "/../packaging/helm-charts/helm3/strimzi-kafka-operator/";
    public static final String HELM_RELEASE_NAME = "strimzi-systemtests";

    public static final String REQUESTS_MEMORY = "512Mi";
    public static final String REQUESTS_CPU = "200m";
    public static final String LIMITS_MEMORY = "512Mi";
    public static final String LIMITS_CPU = "1000m";

    private String namespaceToWatch;
    private String namespaceInstallTo;

    public HelmResource(String namespace) {
        this.namespaceInstallTo = namespace;
        this.namespaceToWatch = namespace;
    }
    public HelmResource(String namespaceInstallTo, String namespaceToWatch) {
        this.namespaceInstallTo = namespaceInstallTo;
        this.namespaceToWatch = namespaceToWatch;
    }

    public void create(ExtensionContext extensionContext) {
        this.create(extensionContext, Constants.CO_OPERATION_TIMEOUT_DEFAULT, Constants.RECONCILIATION_INTERVAL);
    }

    public void create(ExtensionContext extensionContext, long operationTimeout, long reconciliationInterval) {
        ResourceManager.STORED_RESOURCES.computeIfAbsent(extensionContext.getDisplayName(), k -> new Stack<>());
        ResourceManager.STORED_RESOURCES.get(extensionContext.getDisplayName()).push(new ResourceItem(this::delete));
        this.clusterOperator(operationTimeout, reconciliationInterval);
    }

    @Override
    public void delete() {
        this.deleteClusterOperator();
    }

    private void clusterOperator(long operationTimeout) {
        clusterOperator(operationTimeout, Constants.RECONCILIATION_INTERVAL);
    }

    private void clusterOperator(long operationTimeout, long reconciliationInterval) {

        Map<String, Object> values = new HashMap<>();
        // image registry config
        values.put("image.registry", Environment.STRIMZI_REGISTRY);
        values.put("topicOperator.image.registry", Environment.STRIMZI_REGISTRY);
        values.put("userOperator.image.registry", Environment.STRIMZI_REGISTRY);
        values.put("kafkaInit.image.registry", Environment.STRIMZI_REGISTRY);
        values.put("jmxTrans.image.registry", Environment.STRIMZI_REGISTRY);
        values.put("kanikoExecutor.image.registry", Environment.STRIMZI_REGISTRY);
        values.put("tlsSidecarEntityOperator.image.registry", Environment.STRIMZI_REGISTRY);
        values.put("kafkaExporter.image.registry", Environment.STRIMZI_REGISTRY);
        values.put("cruiseControl.image.registry", Environment.STRIMZI_REGISTRY);
        values.put("tlsSidecarCruiseControl.image.registry", Environment.STRIMZI_REGISTRY);
        values.put("kafka.image.registry", Environment.STRIMZI_REGISTRY);
        values.put("kafkaConnect.image.registry", Environment.STRIMZI_REGISTRY);
        values.put("kafkaMirrorMaker.image.registry", Environment.STRIMZI_REGISTRY);
        values.put("kafkaMirrorMaker2.image.registry", Environment.STRIMZI_REGISTRY);
        values.put("kafkaBridge.image.registry", Environment.STRIMZI_REGISTRY_DEFAULT);

        // image repository config
        values.put("image.repository", Environment.STRIMZI_ORG);
        values.put("topicOperator.image.repository", Environment.STRIMZI_ORG);
        values.put("userOperator.image.repository", Environment.STRIMZI_ORG);
        values.put("kafkaInit.image.repository", Environment.STRIMZI_ORG);
        values.put("jmxTrans.image.repository", Environment.STRIMZI_ORG);
        values.put("kanikoExecutor.image.repository", Environment.STRIMZI_ORG);
        values.put("tlsSidecarEntityOperator.image.repository", Environment.STRIMZI_ORG);
        values.put("kafkaExporter.image.repository", Environment.STRIMZI_ORG);
        values.put("cruiseControl.image.repository", Environment.STRIMZI_ORG);
        values.put("tlsSidecarCruiseControl.image.repository", Environment.STRIMZI_ORG);
        values.put("kafka.image.repository", Environment.STRIMZI_ORG);
        values.put("kafkaConnect.image.repository", Environment.STRIMZI_ORG);
        values.put("kafkaMirrorMaker.image.repository", Environment.STRIMZI_ORG);
        values.put("kafkaMirrorMaker2.image.repository", Environment.STRIMZI_ORG);
        values.put("kafkaBridge.image.repository", Environment.STRIMZI_ORG_DEFAULT);

        // image tags config
        values.put("image.tag", Environment.STRIMZI_TAG);
        values.put("topicOperator.image.tag", Environment.STRIMZI_TAG);
        values.put("userOperator.image.tag", Environment.STRIMZI_TAG);
        values.put("kafkaInit.image.tag", Environment.STRIMZI_TAG);
        values.put("jmxTrans.image.tag", Environment.STRIMZI_TAG);
        values.put("kafkaBridge.image.tag", Environment.useLatestReleasedBridge() ? "latest" : BridgeUtils.getBridgeVersion());

        // Additional config
        values.put("image.imagePullPolicy", Environment.OPERATOR_IMAGE_PULL_POLICY);
        values.put("resources.requests.memory", REQUESTS_MEMORY);
        values.put("resources.requests.cpu", REQUESTS_CPU);
        values.put("resources.limits.memory", LIMITS_MEMORY);
        values.put("resources.limits.cpu", LIMITS_CPU);
        values.put("logLevelOverride", Environment.STRIMZI_LOG_LEVEL);
        values.put("fullReconciliationIntervalMs", Long.toString(reconciliationInterval));
        values.put("operationTimeoutMs", Long.toString(operationTimeout));
        // As FG is CSV, we need to escape commas for interpretation of helm installation string
        values.put("featureGates", Environment.STRIMZI_FEATURE_GATES.replaceAll(",", "\\\\,"));
        values.put("watchAnyNamespace", this.namespaceToWatch.equals(Constants.WATCH_ALL_NAMESPACES));
        // We need to remove CO namespace to avoid creation of roles and rolebindings multiple times in one namespace
        // Roles will be created in installTo namespace even if it's not specified in watchNamespaces
        if (!this.namespaceToWatch.equals("*") && !this.namespaceToWatch.equals(this.namespaceInstallTo)) {
            values.put("watchNamespaces", "{" + this.namespaceToWatch.replaceAll(",*" + namespaceInstallTo + ",*", "") + "}");
        }

        Path pathToChart = new File(HELM_CHART).toPath();
        String oldNamespace = KubeClusterResource.getInstance().setNamespace("kube-system");
        InputStream helmAccountAsStream = HelmResource.class.getClassLoader().getResourceAsStream("helm/helm-service-account.yaml");
        String helmServiceAccount = TestUtils.readResource(helmAccountAsStream);
        cmdKubeClient().applyContent(helmServiceAccount);
        KubeClusterResource.getInstance().setNamespace(oldNamespace);
        ResourceManager.helmClient().install(pathToChart, HELM_RELEASE_NAME, values);
        DeploymentUtils.waitForDeploymentReady(ResourceManager.getCoDeploymentName());
    }

    /**
     * Delete CO deployed via helm chart.
     */
    private void deleteClusterOperator() {
        ResourceManager.helmClient().delete(namespaceInstallTo, HELM_RELEASE_NAME);
        DeploymentUtils.waitForDeploymentDeletion(ResourceManager.getCoDeploymentName());
    }
}
