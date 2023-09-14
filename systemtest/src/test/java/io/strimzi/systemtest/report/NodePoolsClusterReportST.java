/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.report;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.ReplicaSet;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.strimzi.api.kafka.model.CruiseControlResources;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.NodePoolsOnly;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaRebalanceTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.test.executor.Exec;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.util.Arrays;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.TestUtils.USER_PATH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(REGRESSION)
@NodePoolsOnly
public class NodePoolsClusterReportST extends AbstractClusterReportST {
    private static final Logger LOGGER = LogManager.getLogger(NodePoolsClusterReportST.class);
    private static final String NODE_POOL_A_NAME = "pool-a";
    private static final String NODE_POOL_B_NAME = "pool-b";

    private TestStorage testStorage;

    @ParallelTest
    void createReport(TestInfo testInfo) throws IOException {
        final String outPath = buildOutPath(testInfo, testStorage.getClusterName());
        final String secretPath = outPath + "/reports/secrets/" + KafkaResources.clusterCaKeySecretName(testStorage.getClusterName()) + ".yaml";
        final String secretKey = "ca.key";

        LOGGER.info("Running report on {}/{}", testStorage.getNamespaceName(), testStorage.getClusterName());
        Exec.exec(USER_PATH + "/../tools/report.sh",
            "--namespace=" + testStorage.getNamespaceName(),
            "--cluster=" + testStorage.getClusterName(),
            "--out-dir=" + outPath,
            "--bridge=" + BRIDGE_NAME,
            "--connect=" + CONNECT_NAME,
            "--mm2=" + MM2_NAME
        );

        assertThat("Output directory does not exist", FileUtils.fileExists(outPath));
        assertThat("Output ZIP file does not exist", FileUtils.listFilesWithSuffix(outPath, ".zip").length == 1);
        Secret clusterCaSecret = SecretUtils.getSecretFromFile(secretPath);
        assertThat("The cluster-ca secret key {} is not present", clusterCaSecret.getData().get(secretKey) != null);
        assertThat("Keys are not hidden in secrets", clusterCaSecret.getData().get(secretKey).equals("<hidden>"));

        assertValidClusterRoleBindings(outPath);
        assertValidClusterRoles(outPath);
        assertValidConfigMaps(outPath, testStorage.getClusterName());
        assertValidCustomResourceDefinitions(outPath);
        assertValidDeployments(outPath, testStorage.getClusterName());
        assertValidNetworkPolicies(outPath, testStorage.getClusterName());
        assertValidPodDisruptionBudgets(outPath, testStorage.getClusterName());
        assertValidPods(outPath, testStorage.getClusterName());
        assertValidReplicaSets(outPath, testStorage.getClusterName());
        assertValidRoleBindings(outPath, testStorage.getClusterName());
        assertValidRoles(outPath, testStorage.getClusterName());
        assertValidSecrets(outPath, testStorage.getClusterName());
        assertValidServices(outPath, testStorage.getClusterName());

        assertValidKafkaBridges(outPath);
        assertValidKafkaConnects(outPath);
        assertValidKafkaMirrorMaker2s(outPath);
        assertValidKafkaNodePools(outPath);
        assertValidKafkaRebalances(outPath, testStorage.getClusterName());
        assertValidKafkas(outPath, testStorage.getClusterName());
        assertValidKafkaTopics(outPath);
        assertValidKafkaUsers(outPath);
        assertValidStrimziPodSets(outPath, testStorage.getClusterName());

        assertValidConfigs(outPath, testStorage.getClusterName());
        assertValidEvents(outPath);
        assertValidLogs(outPath, testStorage.getClusterName());
    }

    @ParallelTest
    void createReportWithSecretsAll(TestInfo testInfo) throws IOException {
        final String outPath = buildOutPath(testInfo, testStorage.getClusterName());
        final String secretPath = outPath + "/reports/secrets/" + KafkaResources.clusterCaKeySecretName(testStorage.getClusterName()) + ".yaml";
        final String secretKey = "ca.key";

        LOGGER.info("Running report on {}/{}", testStorage.getNamespaceName(), testStorage.getClusterName());
        Exec.exec(USER_PATH + "/../tools/report.sh",
            "--namespace=" + testStorage.getNamespaceName(),
            "--cluster=" + testStorage.getClusterName(),
            "--out-dir=" + outPath,
            "--secrets=all");

        Secret clusterCaSecret = SecretUtils.getSecretFromFile(secretPath);
        assertThat("The cluster-ca secret key {} is not present", clusterCaSecret.getData().get(secretKey) != null);
        assertThat("Keys are hidden in secrets", !clusterCaSecret.getData().get(secretKey).equals("<hidden>"));
    }

    @ParallelTest
    void createReportWithSecretsOff(TestInfo testInfo) {
        final String outPath = buildOutPath(testInfo, testStorage.getClusterName());
        final String secretPath = outPath + "/reports/secrets/" + KafkaResources.clusterCaKeySecretName(testStorage.getClusterName()) + ".yaml";

        LOGGER.info("Running report on {}/{}", testStorage.getNamespaceName(), testStorage.getClusterName());
        Exec.exec(USER_PATH + "/../tools/report.sh",
            "--namespace=" + testStorage.getNamespaceName(),
            "--cluster=" + testStorage.getClusterName(),
            "--out-dir=" + outPath,
            "--secrets=off");

        assertThat("Secrets are reported", !FileUtils.fileExists(secretPath));
    }

    @ParallelTest
    void createReportWithoutNsOpts() {
        LOGGER.info("Running report on {}/{}", testStorage.getNamespaceName(), testStorage.getClusterName());
        assertThrows(KubeClusterException.class, () -> Exec.exec(USER_PATH + "/../tools/report.sh", "--cluster=" + testStorage.getClusterName()));
    }

    @ParallelTest
    void createReportWithoutClusterOpts() {
        LOGGER.info("Running report on {}/{}", testStorage.getNamespaceName(), testStorage.getClusterName());
        assertThrows(KubeClusterException.class, () -> Exec.exec(USER_PATH + "/../tools/report.sh", "--namespace=" + testStorage.getNamespaceName()));
    }

    @ParallelTest
    void createReportWithNonExistingNs() {
        LOGGER.info("Running report on {}/{}", testStorage.getNamespaceName(), testStorage.getClusterName());
        assertThrows(KubeClusterException.class, () -> Exec.exec(USER_PATH + "/../tools/report.sh",
            "--namespace=non-existing-ns", "--cluster=" + testStorage.getClusterName()));
    }

    @ParallelTest
    void createReportWithNonExistingCluster() {
        LOGGER.info("Running report on {}/{}", testStorage.getNamespaceName(), testStorage.getClusterName());
        assertThrows(KubeClusterException.class, () -> Exec.exec(USER_PATH + "/../tools/report.sh",
            "--namespace=" + testStorage.getNamespaceName(), "--cluster=non-existing-k"));
    }

    @ParallelTest
    void createReportWithUnknownSecretVerbosity() {
        LOGGER.info("Running report on {}/{}", testStorage.getNamespaceName(), testStorage.getClusterName());
        assertThrows(KubeClusterException.class, () -> Exec.exec(USER_PATH + "/../tools/report.sh",
            "--namespace=" + testStorage.getNamespaceName(), "--cluster=" + testStorage.getClusterName(), "--secrets=unknown"));
    }

    @BeforeAll
    void setUp(final ExtensionContext extensionContext) {
        this.clusterOperator = this.clusterOperator
            .defaultInstallation(extensionContext)
            .withReplicas(2)
            .withExtraEnvVars(Arrays.asList(new EnvVar(Environment.STRIMZI_FEATURE_GATES_ENV, "+KafkaNodePools,+StableConnectIdentities", null)))
            .createInstallation()
            .runInstallation();
        this.testStorage = new TestStorage(extensionContext, Constants.CO_NAMESPACE);
        Kafka kafkaMainCr = KafkaTemplates.kafkaWithCruiseControl(testStorage.getClusterName(), 3, 3)
                .editOrNewMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled")
                .endMetadata()
            .build();
        KafkaNodePool kafkaNodePoolACr = KafkaNodePoolTemplates
            .defaultKafkaNodePool(Constants.CO_NAMESPACE, NODE_POOL_A_NAME, testStorage.getClusterName(), 2)
                .editMetadata()
                    .addToAnnotations("strimzi.io/next-node-ids", "[0-9]")
                .endMetadata()
                .editOrNewSpec()
                    .addToRoles(ProcessRoles.BROKER)
                    .withStorage(kafkaMainCr.getSpec().getKafka().getStorage())
                    .withJvmOptions(kafkaMainCr.getSpec().getKafka().getJvmOptions())
                    .withResources(kafkaMainCr.getSpec().getKafka().getResources())
                .endSpec()
            .build();
        KafkaNodePool kafkaNodePoolBCr = KafkaNodePoolTemplates
            .defaultKafkaNodePool(Constants.CO_NAMESPACE, "pool-b", testStorage.getClusterName(), 1)
                .editMetadata()
                    .addToAnnotations("strimzi.io/next-node-ids", "[10-19]")
                .endMetadata()
                .editOrNewSpec()
                    .addToRoles(ProcessRoles.BROKER)
                    .withStorage(kafkaMainCr.getSpec().getKafka().getStorage())
                    .withJvmOptions(kafkaMainCr.getSpec().getKafka().getJvmOptions())
                    .withResources(kafkaMainCr.getSpec().getKafka().getResources())
                .endSpec()
            .build();
        final String targetClusterName = testStorage.getClusterName() + "-tgt";
        KafkaNodePool kafkaNodePoolTgtCr = KafkaNodePoolTemplates
            .defaultKafkaNodePool(Constants.CO_NAMESPACE, "kafka", targetClusterName, 1)
                .editMetadata()
                    .addToAnnotations("strimzi.io/next-node-ids", "[20-29]")
                .endMetadata()
                .editOrNewSpec()
                    .addToRoles(ProcessRoles.BROKER)
                    .withStorage(kafkaMainCr.getSpec().getKafka().getStorage())
                    .withJvmOptions(kafkaMainCr.getSpec().getKafka().getJvmOptions())
                    .withResources(kafkaMainCr.getSpec().getKafka().getResources())
                .endSpec()
            .build();
        Kafka kafkaTgtCr = KafkaTemplates.kafkaEphemeral(targetClusterName, 1, 3)
                .editOrNewMetadata()
                    .addToAnnotations(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled")
                .endMetadata()
            .build();
        resourceManager.createResourceWithWait(extensionContext,
            kafkaNodePoolACr, kafkaNodePoolBCr, kafkaMainCr, kafkaNodePoolTgtCr, kafkaTgtCr);
        resourceManager.createResourceWithWait(extensionContext,
            KafkaTopicTemplates.topic(testStorage.getClusterName(), "my-topic", 1, 1, Constants.CO_NAMESPACE).build(),
            KafkaUserTemplates.tlsUser(Constants.CO_NAMESPACE, testStorage.getClusterName(), "my-user").build(),
            KafkaRebalanceTemplates.kafkaRebalance(testStorage.getClusterName()).build(),
            KafkaBridgeTemplates.kafkaBridge(BRIDGE_NAME, KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), 1).build(),
            KafkaConnectTemplates.kafkaConnect(CONNECT_NAME, Constants.CO_NAMESPACE, testStorage.getClusterName(), 1).build(),
            KafkaMirrorMaker2Templates.kafkaMirrorMaker2(MM2_NAME, targetClusterName, testStorage.getClusterName(), 1, false).build()
        );
    }

    private void assertValidClusterRoleBindings(String outPath) throws IOException {
        for (String s : Arrays.asList(
            clusterOperatorClusterRoleBindingName() + ".yaml",
            clusterOperatorBrokerDelegationClusterRoleBindingName() + ".yaml",
            clusterOperatorClientDelegationClusterRoleBindingName() + ".yaml"
        )) {
            assertValidYamls(outPath + "/reports/clusterrolebindings", ClusterRoleBinding.class, s, 1);
        }
    }

    private void assertValidClusterRoles(String outPath) throws IOException {
        for (String s : Arrays.asList(
            clusterOperatorGlobalClusterRoleName() + ".yaml",
            clusterOperatorLeaderElectionClusterRoleName() + ".yaml",
            clusterOperatorNamespacedClusterRoleName() + ".yaml",
            clusterOperatorWatchedClusterRoleName() + ".yaml",
            entityOperatorClusterRoleName() + ".yaml",
            clusterOperatorKafkaClientClusterRoleName() + ".yaml",
            clusterOperatorKafkaBrokerClusterRoleName() + ".yaml"
        )) {
            assertValidYamls(outPath + "/reports/clusterroles", ClusterRole.class, s, 1);
        }
    }

    private void assertValidConfigMaps(String outPath, String clusterName) throws IOException {
        for (String s : Arrays.asList(
            KafkaBridgeResources.metricsAndLogConfigMapName(BRIDGE_NAME) + ".yaml",
            clusterOperatorMetricsAndLogConfigMapName() + ".yaml",
            CruiseControlResources.logAndMetricsConfigMapName(clusterName) + ".yaml",
            KafkaResources.entityTopicOperatorLoggingConfigMapName(clusterName) + ".yaml",
            KafkaResources.entityUserOperatorLoggingConfigMapName(clusterName) + ".yaml",
            kafkaNodePoolsConfigMapName(clusterName, NODE_POOL_A_NAME, 0) + ".yaml",
            kafkaNodePoolsConfigMapName(clusterName, NODE_POOL_A_NAME, 1) + ".yaml",
            kafkaNodePoolsConfigMapName(clusterName, NODE_POOL_B_NAME, 10) + ".yaml",
            KafkaResources.zookeeperMetricsAndLogConfigMapName(clusterName) + ".yaml",
            KafkaConnectResources.metricsAndLogConfigMapName(CONNECT_NAME) + ".yaml",
            KafkaMirrorMaker2Resources.metricsAndLogConfigMapName(MM2_NAME) + ".yaml"
        )) {
            assertValidYamls(outPath + "/reports/configmaps", ConfigMap.class, s, 1);
        }
    }

    private void assertValidCustomResourceDefinitions(String outPath) throws IOException {
        for (String s : Arrays.asList(
            kafkaCustomResourceDefinitionName("kafkabridges") + ".yaml",
            kafkaCustomResourceDefinitionName("kafkaconnects") + ".yaml",
            kafkaCustomResourceDefinitionName("kafkamirrormaker2s") + ".yaml",
            kafkaCustomResourceDefinitionName("kafkanodepools") + ".yaml",
            kafkaCustomResourceDefinitionName("kafkarebalances") + ".yaml",
            kafkaCustomResourceDefinitionName("kafkas") + ".yaml",
            kafkaCustomResourceDefinitionName("kafkatopics") + ".yaml",
            kafkaCustomResourceDefinitionName("kafkausers") + ".yaml",
            coreCustomResourceDefinitionName("strimzipodsets") + ".yaml"
        )) {
            assertValidYamls(outPath + "/reports/customresourcedefinitions", CustomResourceDefinition.class, s, 1);
        }
    }

    private void assertValidDeployments(String outPath, String clusterName) throws IOException {
        for (String s : Arrays.asList(
            KafkaBridgeResources.deploymentName(BRIDGE_NAME) + ".yaml",
            CruiseControlResources.deploymentName(clusterName) + ".yaml",
            KafkaResources.entityOperatorDeploymentName(clusterName) + ".yaml",
            clusterOperatorDeploymentName() + ".yaml"
        )) {
            assertValidYamls(outPath + "/reports/deployments", Deployment.class, s, 1);
        }
    }

    private void assertValidNetworkPolicies(String outPath, String clusterName) throws IOException {
        for (String s : Arrays.asList(
            entityOperatorNetworkPolicyName(clusterName) + ".yaml",
            CruiseControlResources.networkPolicyName(clusterName) + ".yaml",
            KafkaResources.kafkaNetworkPolicyName(clusterName) + ".yaml",
            KafkaResources.zookeeperNetworkPolicyName(clusterName) + ".yaml",
            kafkaMirrorMaker2NetworkPolicyName(MM2_NAME) + ".yaml"
        )) {
            assertValidYamls(outPath + "/reports/networkpolicies", NetworkPolicy.class, s, 1);
        }
    }

    private void assertValidPodDisruptionBudgets(String outPath, String clusterName) throws IOException {
        for (String s : Arrays.asList(
            bridgePodDisruptionBudgetName(BRIDGE_NAME) + ".yaml",
            kafkaPodDisruptionBudgetName(clusterName) + ".yaml",
            zookeeperPodDisruptionBudgetName(clusterName) + ".yaml",
            kafkaConnectPodDisruptionBudgetName(CONNECT_NAME) + ".yaml",
            kafkaMirrorMaker2PodDisruptionBudgetName(MM2_NAME) + ".yaml"
        )) {
            assertValidYamls(outPath + "/reports/poddisruptionbudgets", PodDisruptionBudget.class, s, 1);
        }
    }

    private void assertValidPods(String outPath, String clusterName) throws IOException {
        for (String s : Arrays.asList(
            kafkaNodePoolsPodName(clusterName, NODE_POOL_A_NAME, 0) + ".yaml",
            kafkaNodePoolsPodName(clusterName, NODE_POOL_A_NAME, 1) + ".yaml",
            kafkaNodePoolsPodName(clusterName, NODE_POOL_B_NAME, 10) + ".yaml",
            KafkaResources.zookeeperPodName(clusterName, 0) + ".yaml",
            KafkaResources.zookeeperPodName(clusterName, 1) + ".yaml",
            KafkaResources.zookeeperPodName(clusterName, 2) + ".yaml",
            kafkaConnectStableIdentitiesPodName(CONNECT_NAME, 0) + ".yaml",
            kafkaMirrorMaker2StableIdentitiesPodName(MM2_NAME, 0) + ".yaml",
            KafkaBridgeResources.deploymentName(BRIDGE_NAME),
            CruiseControlResources.deploymentName(clusterName),
            KafkaResources.entityOperatorDeploymentName(clusterName)
        )) {
            assertValidYamls(outPath + "/reports/pods", Pod.class, s, 1);
        }
        assertValidYamls(outPath + "/reports/pods", Pod.class, "strimzi-cluster-operator", 2);
    }

    private void assertValidReplicaSets(String outPath, String clusterName) throws IOException {
        for (String s : Arrays.asList(
            KafkaBridgeResources.deploymentName(BRIDGE_NAME),
            CruiseControlResources.deploymentName(clusterName),
            KafkaResources.entityOperatorDeploymentName(clusterName),
            clusterOperatorDeploymentName()
        )) {
            assertValidYamls(outPath + "/reports/replicasets", ReplicaSet.class, s, 1);
        }
    }

    private void assertValidRoleBindings(String outPath, String clusterName) throws IOException {
        for (String s : Arrays.asList(
            KafkaResources.entityTopicOperatorRoleBinding(clusterName) + ".yaml",
            KafkaResources.entityUserOperatorRoleBinding(clusterName) + ".yaml"
        )) {
            assertValidYamls(outPath + "/reports/rolebindings", RoleBinding.class, s, 1);
        }
    }

    private void assertValidRoles(String outPath, String clusterName) throws IOException {
        assertValidYamls(outPath + "/reports/roles", Role.class, KafkaResources.entityOperatorDeploymentName(clusterName) + ".yaml", 1);
    }

    private void assertValidSecrets(String outPath, String clusterName) throws IOException {
        for (String s : Arrays.asList(
            KafkaResources.clientsCaKeySecretName(clusterName) + ".yaml",
            KafkaResources.clientsCaCertificateSecretName(clusterName) + ".yaml",
            KafkaResources.clusterCaKeySecretName(clusterName) + ".yaml",
            KafkaResources.clusterCaCertificateSecretName(clusterName) + ".yaml",
            KafkaResources.secretName(clusterName) + ".yaml",
            CruiseControlResources.apiSecretName(clusterName) + ".yaml",
            CruiseControlResources.secretName(clusterName) + ".yaml",
            KafkaResources.entityTopicOperatorSecretName(clusterName) + ".yaml",
            KafkaResources.entityUserOperatorSecretName(clusterName) + ".yaml",
            KafkaResources.kafkaSecretName(clusterName) + ".yaml",
            KafkaResources.zookeeperSecretName(clusterName) + ".yaml"
        )) {
            assertValidYamls(outPath + "/reports/secrets", Secret.class, s, 1);
        }
    }

    private void assertValidServices(String outPath, String clusterName) throws IOException {
        for (String s : Arrays.asList(
            KafkaBridgeResources.serviceName(BRIDGE_NAME) + ".yaml",
            CruiseControlResources.serviceName(clusterName) + ".yaml",
            KafkaResources.bootstrapServiceName(clusterName) + ".yaml",
            KafkaResources.brokersServiceName(clusterName) + ".yaml",
            KafkaResources.zookeeperServiceName(clusterName) + ".yaml",
            KafkaResources.zookeeperHeadlessServiceName(clusterName) + ".yaml",
            KafkaConnectResources.serviceName(CONNECT_NAME) + ".yaml",
            KafkaMirrorMaker2Resources.serviceName(MM2_NAME) + ".yaml"
        )) {
            assertValidYamls(outPath + "/reports/services", Service.class, s, 1);
        }
    }

    private void assertValidKafkaBridges(String outPath) throws IOException {
        assertValidYamls(outPath + "/reports/kafkabridges", KafkaBridge.class, BRIDGE_NAME + ".yaml", 1);
    }

    private void assertValidKafkaConnects(String outPath) throws IOException {
        assertValidYamls(outPath + "/reports/kafkaconnects", KafkaConnect.class, CONNECT_NAME + ".yaml", 1);
    }

    private void assertValidKafkaMirrorMaker2s(String outPath) throws IOException {
        assertValidYamls(outPath + "/reports/kafkamirrormaker2s", KafkaMirrorMaker2.class, MM2_NAME + ".yaml", 1);
    }

    private void assertValidKafkaNodePools(String outPath) throws IOException {
        assertValidYamls(outPath + "/reports/kafkanodepools", KafkaNodePool.class, NODE_POOL_A_NAME + ".yaml", 1);
        assertValidYamls(outPath + "/reports/kafkanodepools", KafkaNodePool.class, NODE_POOL_B_NAME + ".yaml", 1);
    }

    private void assertValidKafkaRebalances(String outPath, String clusterName) throws IOException {
        assertValidYamls(outPath + "/reports/kafkarebalances", KafkaRebalance.class, clusterName + ".yaml", 1);
    }

    private void assertValidKafkas(String outPath, String clusterName) throws IOException {
        assertValidYamls(outPath + "/reports/kafkas", Kafka.class, clusterName + ".yaml", 1);
    }

    private void assertValidKafkaTopics(String outPath) throws IOException {
        // skipping internal topics as they will not be visible with the UTO by default
        assertValidYamls(outPath + "/reports/kafkatopics", KafkaTopic.class, "my-topic.yaml", 1);
    }

    private void assertValidKafkaUsers(String outPath) throws IOException {
        assertValidYamls(outPath + "/reports/kafkausers", KafkaUser.class, "my-user.yaml", 1);
    }

    private void assertValidStrimziPodSets(String outPath, String clusterName) throws IOException {
        for (String s : Arrays.asList(
            kafkaNodePoolPodSetName(clusterName, NODE_POOL_A_NAME) + ".yaml",
            kafkaNodePoolPodSetName(clusterName, NODE_POOL_B_NAME) + ".yaml",
            zookeeperPodSetName(clusterName) + ".yaml",
            kafkaConnectPodSetName(CONNECT_NAME) + ".yaml",
            kafkaMirrorMaker2PodSetName(MM2_NAME) + ".yaml"
        )) {
            assertValidYamls(outPath + "/reports/strimzipodsets", StrimziPodSet.class, s, 1);
        }
    }

    private void assertValidConfigs(String outPath, String clusterName) {
        for (String s : Arrays.asList(
            kafkaNodePoolsPodName(clusterName, NODE_POOL_A_NAME, 0) + ".cfg",
            kafkaNodePoolsPodName(clusterName, NODE_POOL_A_NAME, 1) + ".cfg",
            kafkaNodePoolsPodName(clusterName, NODE_POOL_B_NAME, 10) + ".cfg",
            KafkaResources.zookeeperPodName(clusterName, 0) + ".cfg",
            KafkaResources.zookeeperPodName(clusterName, 1) + ".cfg",
            KafkaResources.zookeeperPodName(clusterName, 2) + ".cfg"
        )) {
            assertValidFiles(outPath + "/reports/configs", s, 1);
        }
    }

    private void assertValidEvents(String outPath) {
        assertValidFiles(outPath + "/reports/events", "events.txt", 1);
    }

    private void assertValidLogs(String outPath, String clusterName) {
        for (String s : Arrays.asList(
            kafkaNodePoolsPodName(clusterName, NODE_POOL_A_NAME, 0) + ".log",
            kafkaNodePoolsPodName(clusterName, NODE_POOL_A_NAME, 1) + ".log",
            kafkaNodePoolsPodName(clusterName, NODE_POOL_B_NAME, 10) + ".log",
            KafkaResources.zookeeperPodName(clusterName, 0) + ".log",
            KafkaResources.zookeeperPodName(clusterName, 1) + ".log",
            KafkaResources.zookeeperPodName(clusterName, 2) + ".log",
            kafkaConnectStableIdentitiesPodName(CONNECT_NAME, 0) + ".log",
            kafkaMirrorMaker2StableIdentitiesPodName(MM2_NAME, 0) + ".log",
            CruiseControlResources.deploymentName(clusterName),
            KafkaBridgeResources.deploymentName(BRIDGE_NAME)
        )) {
            assertValidFiles(outPath + "/reports/logs", s, 1);
        }
        assertValidFiles(outPath + "/reports/logs", KafkaResources.entityOperatorDeploymentName(clusterName), 3);
        assertValidFiles(outPath + "/reports/logs", clusterOperatorDeploymentName(), 2);
    }
}
