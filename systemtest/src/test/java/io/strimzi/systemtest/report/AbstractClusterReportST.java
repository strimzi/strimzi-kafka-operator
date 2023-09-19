/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
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
    protected static final String BRIDGE_NAME = "my-bridge";
    protected static final String CONNECT_NAME = "my-connect";
    protected static final String MM2_NAME = "my-mm2";

    protected String buildOutPath(TestInfo testInfo, String clusterName) {
        String methodName = testInfo.getTestMethod().isPresent() ?
            testInfo.getTestMethod().get().getName() : UUID.randomUUID().toString();
        return USER_PATH + "/target/logs/reports/" + clusterName + "/" + methodName;
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
        return Constants.STRIMZI_DEPLOYMENT_NAME;
    }

    protected String clusterOperatorMetricsAndLogConfigMapName() {
        return clusterOperatorDeploymentName();
    }

    protected String clusterOperatorClusterRoleBindingName() {
        return clusterOperatorDeploymentName();
    }

    protected String clusterOperatorBrokerDelegationClusterRoleBindingName() {
        return Constants.BROKER_DELEGATION_CLUSTER_ROLE_BINDING;
    }

    protected String clusterOperatorClientDelegationClusterRoleBindingName() {
        return Constants.CLIENT_DELEGATION_CLUSTER_ROLE_BINDING;
    }

    protected String clusterOperatorGlobalClusterRoleName() {
        return Constants.CO_GLOBAL_CLUSTER_ROLE_BINDING;
    }

    protected String clusterOperatorLeaderElectionClusterRoleName() {
        return Constants.CO_LEADER_ELECTION_CLUSTER_ROLE;
    }

    protected String clusterOperatorNamespacedClusterRoleName() {
        return Constants.CO_NAMESPACED_CLUSTER_ROLE;
    }

    protected String clusterOperatorWatchedClusterRoleName() {
        return Constants.CO_WATCHED_CLUSTER_ROLE;
    }

    protected String clusterOperatorKafkaClientClusterRoleName() {
        return Constants.CO_KAFKA_CLIENT_CLUSTER_ROLE;
    }

    protected String clusterOperatorKafkaBrokerClusterRoleName() {
        return Constants.CO_KAFKA_BROKER_CLUSTER_ROLE;
    }

    protected String kafkaCustomResourceDefinitionName(String name) {
        return name + "." + Constants.KAFKA_CRD_GROUP;
    }

    protected String coreCustomResourceDefinitionName(String name) {
        return name + "." + Constants.CORE_CRD_GROUP;
    }

    protected String bridgePodDisruptionBudgetName(String clusterName) {
        return KafkaBridgeResources.deploymentName(clusterName);
    }

    protected String kafkaConnectStableIdentitiesPodName(String clusterName, int podNum) {
        return KafkaConnectResources.deploymentName(clusterName) + "-" + podNum;
    }

    protected String kafkaConnectPodDisruptionBudgetName(String clusterName) {
        return KafkaConnectResources.deploymentName(clusterName);
    }

    protected String kafkaMirrorMaker2StableIdentitiesPodName(String clusterName, int podNum) {
        return KafkaMirrorMaker2Resources.deploymentName(clusterName) + "-" + podNum;
    }

    protected String kafkaMirrorMaker2NetworkPolicyName(String clusterName) {
        return KafkaMirrorMaker2Resources.deploymentName(clusterName);
    }

    protected String kafkaMirrorMaker2PodDisruptionBudgetName(String clusterName) {
        return KafkaMirrorMaker2Resources.deploymentName(clusterName);
    }

    protected String kafkaConfigMapName(String clusterName, int podNum) {
        return KafkaResources.kafkaPodName(clusterName, podNum);
    }

    protected String kafkaNodePoolsPodName(String clusterName, String nodePoolName, int podNum) {
        return String.join("-", clusterName, nodePoolName, String.valueOf(podNum));
    }

    protected String kafkaNodePoolsConfigMapName(String clusterName, String nodePoolName, int podNum) {
        return kafkaNodePoolsPodName(clusterName, nodePoolName, podNum);
    }

    protected String kafkaPodDisruptionBudgetName(String clusterName) {
        return KafkaResources.kafkaStatefulSetName(clusterName);
    }

    protected String zookeeperPodDisruptionBudgetName(String clusterName) {
        return KafkaResources.zookeeperStatefulSetName(clusterName);
    }

    protected String entityOperatorNetworkPolicyName(String clusterName) {
        return KafkaResources.entityOperatorDeploymentName(clusterName);
    }

    protected String entityOperatorClusterRoleName() {
        return Constants.EO_CLUSTER_ROLE;
    }

    protected String kafkaPodSetName(String clusterName) {
        return KafkaResources.kafkaStatefulSetName(clusterName);
    }

    protected String zookeeperPodSetName(String clusterName) {
        return KafkaResources.zookeeperStatefulSetName(clusterName);
    }

    protected String kafkaConnectPodSetName(String clusterName) {
        return KafkaConnectResources.deploymentName(clusterName);
    }

    protected String kafkaMirrorMaker2PodSetName(String clusterName) {
        return KafkaMirrorMaker2Resources.deploymentName(clusterName);
    }

    protected String kafkaNodePoolPodSetName(String clusterName, String poolName) {
        return clusterName + "-" + poolName;
    }
}
