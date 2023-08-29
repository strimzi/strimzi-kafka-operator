/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.utils.FileUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.TestUtils.USER_PATH;
import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag(REGRESSION)
public abstract class AbstractClusterReportST extends AbstractST {
    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

    protected String buildOutPath(TestInfo testInfo, String clusterName) {
        String methodName = testInfo.getTestMethod().isPresent() ?
            testInfo.getTestMethod().get().getName() : UUID.randomUUID().toString();
        return USER_PATH + "/target/reports/" + clusterName + "/" + methodName;
    }

    protected <T> void assertValidYamls(String path, Class<T> clazz, String prefix, int num) throws IOException {
        final File[] files = FileUtils.listFilesWithPrefix(path, prefix);
        assertNotNull(files);
        assertThat(format("Missing files of type %s with prefix %s", clazz.getSimpleName(), prefix), files.length == num);
        for (File file : files) {
            String fileNameWithoutExt = file.getName().replaceFirst("[.][^.]+$", "");
            HasMetadata resource = (HasMetadata) MAPPER.readValue(file, clazz);
            assertThat(resource.getMetadata().getName(), is(fileNameWithoutExt));
        }
    }

    protected void assertValidFiles(String path, String prefix, int num) {
        final File[] files = FileUtils.listFilesWithPrefix(path, prefix);
        assertNotNull(files);
        assertThat(format("Missing files with prefix '%s'", prefix), files.length == num);
        for (File file : files) {
            assertThat(format("%s is empty", file.getAbsolutePath()), file.length(), greaterThan(0L));
        }
    }

    protected String clusterOperatorDeploymentName() {
        return "strimzi-cluster-operator";
    }

    protected String clusterOperatorMetricsAndLogConfigMapName() {
        return clusterOperatorDeploymentName();
    }

    protected String clusterOperatorClusterRoleBindingName() {
        return clusterOperatorDeploymentName();
    }

    protected String clusterOperatorBrokerDelegationClusterRoleBindingName() {
        return clusterOperatorDeploymentName() + "-kafka-broker-delegation";
    }

    protected String clusterOperatorClientDelegationClusterRoleBindingName() {
        return clusterOperatorDeploymentName() + "-kafka-client-delegation";
    }

    protected String clusterOperatorGlobalClusterRoleName() {
        return clusterOperatorDeploymentName() + "-global";
    }

    protected String clusterOperatorLeaderElectionClusterRoleName() {
        return clusterOperatorDeploymentName() + "-leader-election";
    }

    protected String clusterOperatorNamespacedClusterRoleName() {
        return clusterOperatorDeploymentName() + "-namespaced";
    }

    protected String clusterOperatorWatchedClusterRoleName() {
        return clusterOperatorDeploymentName() + "-watched";
    }

    protected String clusterOperatorKafkaClientClusterRoleName() {
        return "strimzi-kafka-client";
    }

    protected String clusterOperatorKafkaBrokerClusterRoleName() {
        return "strimzi-kafka-broker";
    }

    protected String kafkaCustomResourceDefinitionName(String name) {
        return name + ".kafka.strimzi.io";
    }

    protected String coreCustomResourceDefinitionName(String name) {
        return name + ".core.strimzi.io";
    }

    protected String bridgePodDisruptionBudgetName(String clusterName) {
        return clusterName + "-bridge";
    }

    protected String kafkaConnectStableIdentitiesPodName(String clusterName, int podNum) {
        return clusterName + "-connect-" + podNum;
    }

    protected String kafkaConnectPodDisruptionBudgetName(String clusterName) {
        return clusterName + "-connect";
    }

    protected String kafkaMirrorMaker2StableIdentitiesPodName(String clusterName, int podNum) {
        return clusterName + "-mirrormaker2-" + podNum;
    }

    protected String kafkaMirrorMaker2NetworkPolicyName(String clusterName) {
        return clusterName + "-mirrormaker2";
    }

    protected String kafkaMirrorMaker2PodDisruptionBudgetName(String clusterName) {
        return clusterName + "-mirrormaker2";
    }

    protected String kafkaConfigMapName(String clusterName, int podNum) {
        return KafkaResources.kafkaPodName(clusterName, podNum);
    }

    protected String kafkaNodePoolsPodName(String clusterName, String nodePoolName, int podNum) {
        return clusterName + "-" + nodePoolName + "-" + podNum;
    }

    protected String kafkaNodePoolsConfigMapName(String clusterName, String nodePoolName, int podNum) {
        return kafkaNodePoolsPodName(clusterName, nodePoolName, podNum);
    }

    protected String kafkaPodDisruptionBudgetName(String clusterName) {
        return clusterName + "-kafka";
    }

    protected String zookeeperPodDisruptionBudgetName(String clusterName) {
        return clusterName + "-zookeeper";
    }

    protected String entityOperatorNetworkPolicyName(String clusterName) {
        return clusterName + "-entity-operator";
    }

    protected String entityOperatorClusterRoleName() {
        return "strimzi-entity-operator";
    }
}
