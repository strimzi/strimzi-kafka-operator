/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.report;

import io.fabric8.kubernetes.api.model.ConfigMap;
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
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaRebalance;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.systemtest.annotations.ParallelTest;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaRebalanceTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.FileUtils;
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
public class DefaultClusterReportST extends AbstractClusterReportST {
    private static final Logger LOGGER = LogManager.getLogger(DefaultClusterReportST.class);
    private TestStorage testStorage;

    @ParallelTest
    void createReport(TestInfo testInfo) throws IOException {
        final String outPath = buildOutPath(testInfo, testStorage.getClusterName());
        final String secretPath = outPath + "/reports/secrets/" + testStorage.getClusterName() + "-cluster-ca.yaml";
        final String secretKey = "ca.key";

        LOGGER.info("Running report on {}/{}", testStorage.getNamespaceName(), testStorage.getClusterName());
        Exec.exec(USER_PATH + "/../tools/report.sh",
            "--namespace=" + testStorage.getNamespaceName(),
            "--cluster=" + testStorage.getClusterName(),
            "--out-dir=" + outPath,
            "--bridge=my-bridge",
            "--connect=my-connect",
            "--mm2=my-mm2"
        );

        assertThat("Output directory does not exist", FileUtils.exists(outPath));
        assertThat("Output ZIP file does not exist", FileUtils.listFilesWithSuffix(outPath, ".zip").length == 1);

        assertValidClusterRoleBindings(outPath);
        assertValidClusterRoles(outPath);
        assertValidConfigMaps(outPath, testStorage.getClusterName());
        assertValidCustomResourceDefinitions(outPath);
        assertValidCustomDeployments(outPath, testStorage.getClusterName());
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
        assertValidKafkaRebalances(outPath, testStorage.getClusterName());
        assertValidKafkas(outPath, testStorage.getClusterName());
        assertValidKafkaTopics(outPath);
        assertValidKafkaUsers(outPath);
        assertValidStrimziPodSets(outPath, testStorage.getClusterName());

        assertValidConfigs(outPath, testStorage.getClusterName());
        assertValidEvents(outPath);
        assertValidLogs(outPath, testStorage.getClusterName());

        Secret clusterCaSecret = getSecretWithKeyFromFile(secretPath, secretKey);
        assertThat("Keys are not hidden in secrets", clusterCaSecret.getData().get(secretKey).equals("<hidden>"));

        assertThat("KafkaBridge CRD does not exist", FileUtils.exists(outPath + "/reports/customresourcedefinitions/kafkabridges.kafka.strimzi.io.yaml"));
        assertThat("KafkaBridge logs are missing", FileUtils.listFilesWithPrefix(outPath + "/reports/logs", "my-bridge-bridge").length == 1);

        assertThat("KafkaConnect CRD does not exist", FileUtils.exists(outPath + "/reports/customresourcedefinitions/kafkaconnects.kafka.strimzi.io.yaml"));
        assertThat("KafkaConnect logs are missing", FileUtils.listFilesWithPrefix(outPath + "/reports/logs", "my-connect-connect").length == 1);

        assertThat("KafkaMirrorMaker2 CRD does not exist", FileUtils.exists(outPath + "/reports/customresourcedefinitions/kafkamirrormaker2s.kafka.strimzi.io.yaml"));
        assertThat("KafkaMirrorMaker2 logs are missing", FileUtils.listFilesWithPrefix(outPath + "/reports/logs", "my-mm2-mirrormaker2").length == 1);
    }

    @ParallelTest
    void createReportWithSecretsAll(TestInfo testInfo) throws IOException {
        final String outPath = buildOutPath(testInfo, testStorage.getClusterName());
        final String secretPath = outPath + "/reports/secrets/" + testStorage.getClusterName() + "-cluster-ca.yaml";
        final String secretKey = "ca.key";

        LOGGER.info("Running report on {}/{}", testStorage.getNamespaceName(), testStorage.getClusterName());
        Exec.exec(USER_PATH + "/../tools/report.sh",
            "--namespace=" + testStorage.getNamespaceName(),
            "--cluster=" + testStorage.getClusterName(),
            "--out-dir=" + outPath,
            "--secrets=all");

        Secret clusterCaSecret = getSecretWithKeyFromFile(secretPath, secretKey);
        assertThat("Keys are hidden in secrets", !clusterCaSecret.getData().get(secretKey).equals("<hidden>"));
    }

    @ParallelTest
    void createReportWithSecretsOff(TestInfo testInfo) throws IOException {
        final String outPath = buildOutPath(testInfo, testStorage.getClusterName());
        final String secretPath = outPath + "/reports/secrets/" + testStorage.getClusterName() + "-cluster-ca.yaml";

        LOGGER.info("Running report on {}/{}", testStorage.getNamespaceName(), testStorage.getClusterName());
        Exec.exec(USER_PATH + "/../tools/report.sh",
            "--namespace=" + testStorage.getNamespaceName(),
            "--cluster=" + testStorage.getClusterName(),
            "--out-dir=" + outPath,
            "--secrets=off");

        assertThat("Secrets are reported", !FileUtils.exists(secretPath));
    }

    @ParallelTest
    void createReportWithoutNsOpts(TestInfo testInfo) {
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
            .createInstallation()
            .runInstallation();
        this.testStorage = new TestStorage(extensionContext);
        resourceManager.createResourceWithWait(extensionContext,
            KafkaTemplates.kafkaWithCruiseControl(testStorage.getClusterName(), 3, 3).build(),
            KafkaTemplates.kafkaEphemeral(testStorage.getClusterName() + "-tgt", 3).build()
        );
        resourceManager.createResourceWithWait(extensionContext,
            KafkaTopicTemplates.topic(testStorage.getClusterName(), "my-topic", 1, 1, testStorage.getNamespaceName()).build(),
            KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), testStorage.getClusterName(), "my-user").build(),
            KafkaRebalanceTemplates.kafkaRebalance(testStorage.getClusterName()).build(),
            KafkaBridgeTemplates.kafkaBridge("my-bridge", KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), 1).build(),
            KafkaConnectTemplates.kafkaConnect("my-connect", testStorage.getNamespaceName(), testStorage.getClusterName(), 1).build(),
            KafkaMirrorMaker2Templates.kafkaMirrorMaker2("my-mm2", testStorage.getClusterName() + "-tgt", testStorage.getClusterName(), 1, false).build()
        );
    }

    private void assertValidClusterRoleBindings(String outPath) throws IOException {
        for (String s : Arrays.asList(
            "strimzi-cluster-operator.yaml",
            "strimzi-cluster-operator-kafka-broker-delegation.yaml",
            "strimzi-cluster-operator-kafka-client-delegation.yaml"
        )) {
            assertValidYamls(outPath + "/reports/clusterrolebindings", ClusterRoleBinding.class, s, 1);
        }
    }

    private void assertValidClusterRoles(String outPath) throws IOException {
        for (String s : Arrays.asList(
            "strimzi-cluster-operator-global.yaml",
            "strimzi-cluster-operator-leader-election.yaml",
            "strimzi-cluster-operator-namespaced.yaml",
            "strimzi-cluster-operator-watched.yaml",
            "strimzi-entity-operator.yaml",
            "strimzi-kafka-client.yaml",
            "strimzi-kafka-broker.yaml"
        )) {
            assertValidYamls(outPath + "/reports/clusterroles", ClusterRole.class, s, 1);
        }
    }

    private void assertValidConfigMaps(String outPath, String clusterName) throws IOException {
        for (String s : Arrays.asList(
            "my-bridge-bridge-config.yaml",
            clusterName + "-cruise-control-config.yaml",
            clusterName + "-entity-topic-operator-config.yaml",
            clusterName + "-entity-user-operator-config.yaml",
            clusterName + "-kafka-0.yaml",
            clusterName + "-kafka-1.yaml",
            clusterName + "-kafka-2.yaml",
            clusterName + "-zookeeper-config.yaml",
            "my-connect-connect-config.yaml",
            "my-mm2-mirrormaker2-config.yaml"
        )) {
            assertValidYamls(outPath + "/reports/configmaps", ConfigMap.class, s, 1);
        }
    }

    private void assertValidCustomResourceDefinitions(String outPath) throws IOException {
        for (String s : Arrays.asList(
            "kafkabridges.kafka.strimzi.io.yaml",
            "kafkaconnects.kafka.strimzi.io.yaml",
            "kafkamirrormaker2s.kafka.strimzi.io.yaml",
            "kafkarebalances.kafka.strimzi.io.yaml",
            "kafkas.kafka.strimzi.io.yaml",
            "kafkatopics.kafka.strimzi.io.yaml",
            "kafkausers.kafka.strimzi.io.yaml",
            "strimzipodsets.core.strimzi.io.yaml"
        )) {
            assertValidYamls(outPath + "/reports/customresourcedefinitions", CustomResourceDefinition.class, s, 1);
        }
    }

    private void assertValidCustomDeployments(String outPath, String clusterName) throws IOException {
        for (String s : Arrays.asList(
            "my-bridge-bridge.yaml",
            clusterName + "-cruise-control.yaml",
            clusterName + "-entity-operator.yaml",
            "strimzi-cluster-operator.yaml"
        )) {
            assertValidYamls(outPath + "/reports/deployments", Deployment.class, s, 1);
        }
    }

    private void assertValidNetworkPolicies(String outPath, String clusterName) throws IOException {
        for (String s : Arrays.asList(
            clusterName + "-entity-operator.yaml",
            clusterName + "-network-policy-cruise-control.yaml",
            clusterName + "-network-policy-kafka.yaml",
            clusterName + "-network-policy-zookeeper.yaml",
            "my-mm2-mirrormaker2.yaml"
        )) {
            assertValidYamls(outPath + "/reports/networkpolicies", NetworkPolicy.class, s, 1);
        }
    }

    private void assertValidPodDisruptionBudgets(String outPath, String clusterName) throws IOException {
        for (String s : Arrays.asList(
            "my-bridge-bridge.yaml",
            clusterName + "-kafka.yaml",
            clusterName + "-zookeeper.yaml",
            "my-connect-connect.yaml",
            "my-mm2-mirrormaker2.yaml"
        )) {
            assertValidYamls(outPath + "/reports/poddisruptionbudgets", PodDisruptionBudget.class, s, 1);
        }
    }

    private void assertValidPods(String outPath, String clusterName) throws IOException {
        for (String s : Arrays.asList(
            clusterName + "-kafka-0.yaml",
            clusterName + "-kafka-1.yaml",
            clusterName + "-kafka-2.yaml",
            clusterName + "-zookeeper-0.yaml",
            clusterName + "-zookeeper-1.yaml",
            clusterName + "-zookeeper-2.yaml"
        )) {
            assertValidYamls(outPath + "/reports/pods", Pod.class, s, 1);
        }
        assertValidYamls(outPath + "/reports/pods", Pod.class, "my-bridge-bridge", 1);
        assertValidYamls(outPath + "/reports/pods", Pod.class, clusterName + "-cruise-control", 1);
        assertValidYamls(outPath + "/reports/pods", Pod.class, clusterName + "-entity-operator", 1);
        assertValidYamls(outPath + "/reports/pods", Pod.class, "my-connect-connect", 1);
        assertValidYamls(outPath + "/reports/pods", Pod.class, "my-mm2-mirrormaker2", 1);
        assertValidYamls(outPath + "/reports/pods", Pod.class, "strimzi-cluster-operator", 2);
    }

    private void assertValidReplicaSets(String outPath, String clusterName) throws IOException {
        assertValidYamls(outPath + "/reports/replicasets", ReplicaSet.class, "my-bridge-bridge", 1);
        assertValidYamls(outPath + "/reports/replicasets", ReplicaSet.class, clusterName + "-cruise-control", 1);
        assertValidYamls(outPath + "/reports/replicasets", ReplicaSet.class, clusterName + "-entity-operator", 1);
        assertValidYamls(outPath + "/reports/replicasets", ReplicaSet.class, "strimzi-cluster-operator", 1);
    }

    private void assertValidRoleBindings(String outPath, String clusterName) throws IOException {
        for (String s : Arrays.asList(
            clusterName + "-entity-topic-operator-role.yaml",
            clusterName + "-entity-user-operator-role.yaml"
        )) {
            assertValidYamls(outPath + "/reports/rolebindings", RoleBinding.class, s, 1);
        }
    }

    private void assertValidRoles(String outPath, String clusterName) throws IOException {
        assertValidYamls(outPath + "/reports/roles", Role.class, clusterName + "-entity-operator.yaml", 1);
    }

    private void assertValidSecrets(String outPath, String clusterName) throws IOException {
        for (String s : Arrays.asList(
            clusterName + "-clients-ca.yaml",
            clusterName + "-clients-ca-cert.yaml",
            clusterName + "-cluster-ca.yaml",
            clusterName + "-cluster-ca-cert.yaml",
            clusterName + "-cluster-operator-certs.yaml",
            clusterName + "-cruise-control-api.yaml",
            clusterName + "-cruise-control-certs.yaml",
            clusterName + "-entity-topic-operator-certs.yaml",
            clusterName + "-entity-user-operator-certs.yaml",
            clusterName + "-kafka-brokers.yaml",
            clusterName + "-zookeeper-nodes.yaml"
        )) {
            assertValidYamls(outPath + "/reports/secrets", Secret.class, s, 1);
        }
    }

    private void assertValidServices(String outPath, String clusterName) throws IOException {
        for (String s : Arrays.asList(
            "my-bridge-bridge-service.yaml",
            clusterName + "-cruise-control.yaml",
            clusterName + "-kafka-bootstrap.yaml",
            clusterName + "-kafka-brokers.yaml",
            clusterName + "-zookeeper-client.yaml",
            clusterName + "-zookeeper-nodes.yaml",
            "my-connect-connect-api.yaml",
            "my-mm2-mirrormaker2-api.yaml"
        )) {
            assertValidYamls(outPath + "/reports/services", Service.class, s, 1);
        }
    }

    private void assertValidKafkaBridges(String outPath) throws IOException {
        assertValidYamls(outPath + "/reports/kafkabridges", KafkaBridge.class, "my-bridge.yaml", 1);
    }

    private void assertValidKafkaConnects(String outPath) throws IOException {
        assertValidYamls(outPath + "/reports/kafkaconnects", KafkaConnect.class, "my-connect.yaml", 1);
    }

    private void assertValidKafkaMirrorMaker2s(String outPath) throws IOException {
        assertValidYamls(outPath + "/reports/kafkamirrormaker2s", KafkaMirrorMaker2.class, "my-mm2.yaml", 1);
    }

    private void assertValidKafkaRebalances(String outPath, String clusterName) throws IOException {
        assertValidYamls(outPath + "/reports/kafkarebalances",
            KafkaRebalance.class, clusterName + ".yaml", 1);
    }

    private void assertValidKafkas(String outPath, String clusterName) throws IOException {
        for (String s : Arrays.asList(
            clusterName + ".yaml",
            clusterName + "-tgt.yaml"
        )) {
            assertValidYamls(outPath + "/reports/kafkas", Kafka.class, s, 1);
        }
    }

    private void assertValidKafkaTopics(String outPath) throws IOException {
        // skipping internal topics as they will not be visible with the UTO by default
        assertValidYamls(outPath + "/reports/kafkatopics", KafkaTopic.class, "my-topic.yaml", 1);
    }

    private void assertValidKafkaUsers(String outPath) throws IOException {
        // skipping internal topics as they will not be visible with the UTO by default
        assertValidYamls(outPath + "/reports/kafkausers", KafkaUser.class, "my-user.yaml", 1);
    }

    private void assertValidStrimziPodSets(String outPath, String clusterName) throws IOException {
        for (String s : Arrays.asList(
            clusterName + "-kafka.yaml",
            clusterName + "-tgt-kafka.yaml",
            clusterName + "-tgt-zookeeper.yaml",
            clusterName + "-zookeeper.yaml"
        )) {
            assertValidYamls(outPath + "/reports/strimzipodsets", StrimziPodSet.class, s, 1);
        }
    }

    private void assertValidConfigs(String outPath, String clusterName) {
        for (String s : Arrays.asList(
            clusterName + "-kafka-0.cfg",
            clusterName + "-kafka-1.cfg",
            clusterName + "-kafka-2.cfg",
            clusterName + "-zookeeper-0.cfg",
            clusterName + "-zookeeper-1.cfg",
            clusterName + "-zookeeper-2.cfg")
        ) {
            assertValidFiles(outPath + "/reports/configs", s, 1);
        }
    }

    private void assertValidEvents(String outPath) {
        assertValidFiles(outPath + "/reports/events", "events.txt", 1);
    }

    private void assertValidLogs(String outPath, String clusterName) {
        for (String s : Arrays.asList(
            clusterName + "-kafka-0.log",
            clusterName + "-kafka-1.log",
            clusterName + "-kafka-2.log",
            clusterName + "-zookeeper-0.log",
            clusterName + "-zookeeper-1.log",
            clusterName + "-zookeeper-2.log"
        )) {
            assertValidFiles(outPath + "/reports/logs", s, 1);
        }
        assertValidFiles(outPath + "/reports/logs", clusterName + "-cruise-control", 1);
        assertValidFiles(outPath + "/reports/logs", "my-bridge-bridge", 1);
        assertValidFiles(outPath + "/reports/logs", clusterName + "-entity-operator", 3);
        assertValidFiles(outPath + "/reports/logs", "my-connect-connect", 1);
        assertValidFiles(outPath + "/reports/logs", "my-mm2-mirrormaker2", 1);
        assertValidFiles(outPath + "/reports/logs", "strimzi-cluster-operator", 2);
    }
}
