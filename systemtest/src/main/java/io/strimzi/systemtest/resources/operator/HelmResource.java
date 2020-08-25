/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.operator;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.specific.BridgeUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static io.strimzi.test.TestUtils.entry;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;

public class HelmResource {
    private static final Logger LOGGER = LogManager.getLogger(HelmResource.class);

    public static final String HELM_CHART = TestUtils.USER_PATH + "/../helm-charts/helm3/strimzi-kafka-operator/";
    public static final String HELM_RELEASE_NAME = "strimzi-systemtests";

    public static final String REQUESTS_MEMORY = "512Mi";
    public static final String REQUESTS_CPU = "200m";
    public static final String LIMITS_MEMORY = "512Mi";
    public static final String LIMITS_CPU = "1000m";

    public static void clusterOperator() {
        clusterOperator(Constants.CO_OPERATION_TIMEOUT_DEFAULT);
    }

    public static void clusterOperator(long operationTimeout) {
        clusterOperator(operationTimeout, Constants.RECONCILIATION_INTERVAL);
    }

    public static void clusterOperator(long operationTimeout, long reconciliationInterval) {
        Map<String, String> values = Collections.unmodifiableMap(Stream.of(
                entry("imageRepositoryOverride", Environment.STRIMZI_REGISTRY + "/" + Environment.STRIMZI_ORG),
                entry("image.tag", Environment.STRIMZI_TAG),
                entry("topicOperator.image.tag", Environment.STRIMZI_TAG),
                entry("userOperator.image.tag", Environment.STRIMZI_TAG),
                entry("kafkaInit.image.tag", Environment.STRIMZI_TAG),
                entry("jmxTrans.image.tag", Environment.STRIMZI_TAG),
                entry("kafkaBridge.image.tag", Environment.useLatestReleasedBridge() ? "latest" : BridgeUtils.getBridgeVersion()),
                entry("image.pullPolicy", Environment.OPERATOR_IMAGE_PULL_POLICY),
                entry("resources.requests.memory", REQUESTS_MEMORY),
                entry("resources.requests.cpu", REQUESTS_CPU),
                entry("resources.limits.memory", LIMITS_MEMORY),
                entry("resources.limits.cpu", LIMITS_CPU),
                entry("logLevel", Environment.STRIMZI_LOG_LEVEL),
                entry("fullReconciliationIntervalMs", Long.toString(reconciliationInterval)),
                entry("operationTimeoutMs", Long.toString(operationTimeout)))
                .collect(TestUtils.entriesToMap()));

        Path pathToChart = new File(HELM_CHART).toPath();
        String oldNamespace = KubeClusterResource.getInstance().setNamespace("kube-system");
        InputStream helmAccountAsStream = HelmResource.class.getClassLoader().getResourceAsStream("helm/helm-service-account.yaml");
        String helmServiceAccount = TestUtils.readResource(helmAccountAsStream);
        cmdKubeClient().applyContent(helmServiceAccount);
        KubeClusterResource.getInstance().setNamespace(oldNamespace);
        ResourceManager.helmClient().install(pathToChart, HELM_RELEASE_NAME, values);
        ResourceManager.getPointerResources().push(HelmResource::deleteClusterOperator);
        DeploymentUtils.waitForDeploymentReady(ResourceManager.getCoDeploymentName());
    }

    /**
     * Delete CO deployed via helm chart.
     */
    public static void deleteClusterOperator() {
        ResourceManager.helmClient().delete(HELM_RELEASE_NAME);
        DeploymentUtils.waitForDeploymentDeletion(ResourceManager.getCoDeploymentName());
    }
}
