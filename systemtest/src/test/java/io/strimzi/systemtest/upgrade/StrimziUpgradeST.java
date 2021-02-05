/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.operator.BundleResource;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.UPGRADE;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(UPGRADE)
public class StrimziUpgradeST extends AbstractUpgradeST {

    private static final Logger LOGGER = LogManager.getLogger(StrimziUpgradeST.class);

    public static final String NAMESPACE = "strimzi-upgrade-test";

    // TODO: make testUpgradeKafkaWithoutVersion to run upgrade with config from StrimziUpgradeST.json
    // main idea of the test and usage of latestReleasedVersion: upgrade CO from version X, kafka Y, to CO version Z and kafka Y + 1 at the end
    private final String strimziReleaseWithOlderKafkaVersion = "0.20.0";
    private final String strimziReleaseWithOlderKafka = String.format("https://github.com/strimzi/strimzi-kafka-operator/releases/download/%s/strimzi-%s.zip",
        strimziReleaseWithOlderKafkaVersion, strimziReleaseWithOlderKafkaVersion);

    @ParameterizedTest(name = "testUpgradeStrimziVersion-{0}-{1}")
    @MethodSource("loadJsonUpgradeData")
    @Tag(INTERNAL_CLIENTS_USED)
    void testUpgradeStrimziVersion(String from, String to, JsonObject parameters) throws Exception {

        assumeTrue(StUtils.isAllowOnCurrentEnvironment(parameters.getJsonObject("environmentInfo").getString("flakyEnvVariable")));
        assumeTrue(StUtils.isAllowedOnCurrentK8sVersion(parameters.getJsonObject("environmentInfo").getString("maxK8sVersion")));

        LOGGER.debug("Running upgrade test from version {} to {}", from, to);
        performUpgrade(parameters, MESSAGE_COUNT, MESSAGE_COUNT);
    }

    @Test
    void testUpgradeKafkaWithoutVersion() throws IOException {
        File dir = FileUtils.downloadAndUnzip(strimziReleaseWithOlderKafka);
        File previousKafkaPersistent = new File(dir, "strimzi-" + strimziReleaseWithOlderKafkaVersion + "/examples/kafka/kafka-persistent.yaml");
        File previousKafkaVersionsYaml = FileUtils.downloadYaml("https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/" + strimziReleaseWithOlderKafkaVersion + "/kafka-versions.yaml");
        File latestKafkaVersionsYaml = new File(TestUtils.USER_PATH + "//../kafka-versions.yaml");

        coDir = new File(dir, "strimzi-" + strimziReleaseWithOlderKafkaVersion + "/install/cluster-operator/");

        String latestKafkaVersion = getValueForLastKafkaVersionInFile(latestKafkaVersionsYaml, "version");

        // Modify + apply installation files
        copyModifyApply(coDir, NAMESPACE);
        // Apply Kafka Persistent without version
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaFromYaml(previousKafkaPersistent, clusterName, 3, 3)
            .editSpec()
                .editKafka()
                    .withVersion(null)
                    .addToConfig("log.message.format.version", getValueForLastKafkaVersionInFile(previousKafkaVersionsYaml, "format"))
                    .addToConfig("inter.broker.protocol.version", getValueForLastKafkaVersionInFile(previousKafkaVersionsYaml, "protocol"))
                .endKafka()
            .endSpec()
            .build());

        assertNull(KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(clusterName).get().getSpec().getKafka().getVersion());

        Map<String, String> operatorSnapshot = DeploymentUtils.depSnapshot(ResourceManager.getCoDeploymentName());
        Map<String, String> zooSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(clusterName));
        Map<String, String> kafkaSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(clusterName));
        Map<String, String> eoSnapshot = DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(clusterName));

        // Update CRDs, CRB, etc.
        applyClusterOperatorInstallFiles(NAMESPACE);
        applyBindings(NAMESPACE);

        kubeClient().getClient().apps().deployments().inNamespace(NAMESPACE).withName(ResourceManager.getCoDeploymentName()).delete();
        kubeClient().getClient().apps().deployments().inNamespace(NAMESPACE).withName(ResourceManager.getCoDeploymentName()).create(BundleResource.defaultClusterOperator(NAMESPACE).build());

        DeploymentUtils.waitTillDepHasRolled(ResourceManager.getCoDeploymentName(), 1, operatorSnapshot);
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.zookeeperStatefulSetName(clusterName), 3, zooSnapshot);
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaSnapshot);
        DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoSnapshot);

        assertThat(KafkaUtils.getVersionFromKafkaPodLibs(KafkaResources.kafkaPodName(clusterName, 0)), containsString(latestKafkaVersion));
    }

    @Test
    void testUpgradeAcrossVersionsWithUnsupportedKafkaVersion() throws IOException {
        JsonObject acrossUpgradeData = buildDataForUpgradeAcrossVersions();

        String continuousTopicName = "continuous-topic";
        String producerName = "hello-world-producer";
        String consumerName = "hello-world-consumer";
        String continuousConsumerGroup = "continuous-consumer-group";

        // Setup env
        setupEnvAndUpgradeClusterOperator(acrossUpgradeData, MESSAGE_COUNT, MESSAGE_COUNT, producerName, consumerName, continuousTopicName, continuousConsumerGroup, acrossUpgradeData.getString("startingKafkaVersion"), NAMESPACE);
        // Make snapshots of all pods
        makeSnapshots(clusterName);
        // Upgrade CO
        changeClusterOperator(acrossUpgradeData, NAMESPACE);
        logPodImages(clusterName);
        //  Upgrade kafka
        changeKafkaAndLogFormatVersion(acrossUpgradeData.getJsonObject("proceduresAfterOperatorUpgrade"), clusterName, NAMESPACE);
        logPodImages(clusterName);
        checkAllImages(acrossUpgradeData.getJsonObject("imagesAfterKafkaUpgrade"));
        // Verify that pods are stable
        PodUtils.verifyThatRunningPodsAreStable(clusterName);
        // Verify upgrade
        verifyUpgradeProcedure(acrossUpgradeData, MESSAGE_COUNT, MESSAGE_COUNT, producerName, consumerName);
        // Check errors in CO log
        assertNoCoErrorsLogged(0);
    }

    @Test
    void testUpgradeAcrossVersionsWithNoKafkaVersion() throws IOException {
        JsonObject acrossUpgradeData = buildDataForUpgradeAcrossVersions();

        String continuousTopicName = "continuous-topic";
        String producerName = "hello-world-producer";
        String consumerName = "hello-world-consumer";
        String continuousConsumerGroup = "continuous-consumer-group";

        // Setup env
        setupEnvAndUpgradeClusterOperator(acrossUpgradeData, MESSAGE_COUNT, MESSAGE_COUNT, producerName, consumerName, continuousTopicName, continuousConsumerGroup, null, NAMESPACE);
        // Upgrade CO
        changeClusterOperator(acrossUpgradeData, NAMESPACE);
        // Wait till first upgrade finished
        zkPods = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.zookeeperStatefulSetName(clusterName), 3, zkPods);
        kafkaPods = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaPods);
        eoPods = DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoPods);

        LOGGER.info("Rolling to new images has finished!");
        logPodImages(clusterName);
        //  Upgrade kafka
        changeKafkaAndLogFormatVersion(acrossUpgradeData.getJsonObject("proceduresAfterOperatorUpgrade"), clusterName, NAMESPACE);
        logPodImages(clusterName);
        checkAllImages(acrossUpgradeData.getJsonObject("imagesAfterKafkaUpgrade"));
        // Verify that pods are stable
        PodUtils.verifyThatRunningPodsAreStable(clusterName);
        // Verify upgrade
        verifyUpgradeProcedure(acrossUpgradeData, MESSAGE_COUNT, MESSAGE_COUNT, producerName, consumerName);

        // Check errors in CO log
        assertNoCoErrorsLogged(0);
    }

    private JsonObject buildDataForUpgradeAcrossVersions() {
        List<TestKafkaVersion> sortedVersions = TestKafkaVersion.getKafkaVersions();
        TestKafkaVersion latestKafkaSupported = sortedVersions.get(sortedVersions.size() - 1);

        JsonArray upgradeJson = readUpgradeJson(UPGRADE_JSON_FILE);

        JsonObject acrossUpgradeData = upgradeJson.getJsonObject(upgradeJson.size() - 1);
        JsonObject startingVersion = getDataForStartUpgrade(upgradeJson);

        acrossUpgradeData.put("fromVersion", startingVersion.getValue("fromVersion"));
        acrossUpgradeData.put("fromExamples", startingVersion.getValue("fromExamples"));
        acrossUpgradeData.put("urlFrom", startingVersion.getValue("urlFrom"));

        acrossUpgradeData.put("urlTo", "HEAD");
        acrossUpgradeData.put("toVersion", "HEAD");
        acrossUpgradeData.put("toExamples", "HEAD");

        acrossUpgradeData.put("startingKafkaVersion", startingVersion.getString("oldestKafka"));

        // Generate procedures for upgrade
        JsonObject procedures = new JsonObject();
        procedures.put("kafkaVersion", latestKafkaSupported.version());
        procedures.put("logMessageVersion", latestKafkaSupported.messageVersion());
        procedures.put("interBrokerProtocolVersion", latestKafkaSupported.protocolVersion());
        acrossUpgradeData.put("proceduresAfterOperatorUpgrade", procedures);

        return acrossUpgradeData;
    }

    private JsonObject getDataForStartUpgrade(JsonArray upgradeJson) {
        List<TestKafkaVersion> sortedVersions = TestKafkaVersion.getKafkaVersions();
        List<String> versions = sortedVersions.stream().map(item -> item.version()).collect(Collectors.toList());

        Collections.reverse(upgradeJson.getList());

        JsonObject startingVersion = null;

        for (Object item : upgradeJson) {
            if (!versions.contains(((JsonObject) item).getString("oldestKafka"))) {
                startingVersion = (JsonObject) item;
                break;
            }
        }
        return startingVersion;
    }

    String getValueForLastKafkaVersionInFile(File kafkaVersions, String field) throws IOException {
        YAMLMapper mapper = new YAMLMapper();
        JsonNode node = mapper.readTree(kafkaVersions);
        ObjectNode kafkaVersionNode = (ObjectNode) node.get(node.size() - 1);

        return kafkaVersionNode.get(field).asText();
    }

    private void verifyUpgradeProcedure(JsonObject testParameters, int produceMessagesCount, int consumeMessagesCount,
                                        String producerName, String consumerName) {
        int continuousClientsMessageCount = testParameters.getJsonObject("client").getInteger("continuousClientsMessages");

        // Delete old clients
        kubeClient().deleteDeployment(clusterName + "-" + Constants.KAFKA_CLIENTS);
        DeploymentUtils.waitForDeploymentDeletion(clusterName + "-" + Constants.KAFKA_CLIENTS);

        KafkaUser kafkaUser = TestUtils.fromYamlString(cmdKubeClient().getResourceAsYaml("kafkauser", userName), KafkaUser.class);
        deployClients(testParameters.getJsonObject("client").getString("afterKafkaUpgradeDowngrade"), kafkaUser);

        final String afterUpgradeKafkaClientsPodName =
                kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
                .withUsingPodName(afterUpgradeKafkaClientsPodName)
                .withTopicName(topicName)
                .withNamespaceName(NAMESPACE)
                .withClusterName(clusterName)
                .withKafkaUsername(userName)
                .withMessageCount(produceMessagesCount)
                .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
                .withConsumerGroupName(ClientUtils.generateRandomConsumerGroup())
                .build();

        int received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(consumeMessagesCount));

        if (testParameters.getBoolean("generateTopics")) {
            // Check that topics weren't deleted/duplicated during upgrade procedures
            List<KafkaTopic> kafkaTopicList = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).list().getItems();
            int additionalTopics = testParameters.getInteger("additionalTopics", 0);
            assertThat("KafkaTopic list doesn't have expected size", kafkaTopicList.size(), is(expectedTopicCount + additionalTopics));
            assertThat("KafkaTopic " + topicName + " is not in expected topic list",
                    kafkaTopicList.contains(KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicName).get()), is(true));
            for (int x = 0; x < upgradeTopicCount; x++) {
                KafkaTopic kafkaTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicName + "-" + x).get();
                assertThat("KafkaTopic " + topicName + "-" + x + " is not in expected topic list", kafkaTopicList.contains(kafkaTopic), is(true));
            }
        }

        if (continuousClientsMessageCount != 0) {
            // ##############################
            // Validate that continuous clients finished successfully
            // ##############################
            ClientUtils.waitTillContinuousClientsFinish(producerName, consumerName, NAMESPACE, continuousClientsMessageCount);
            // ##############################
            // Delete jobs to make same names available for next upgrade run during chain upgrade
            kubeClient().deleteJob(producerName);
            kubeClient().deleteJob(consumerName);
        }

    }

    private void performUpgrade(JsonObject testParameters, int produceMessagesCount, int consumeMessagesCount) throws IOException {
        String continuousTopicName = "continuous-topic";
        String producerName = "hello-world-producer";
        String consumerName = "hello-world-consumer";
        String continuousConsumerGroup = "continuous-consumer-group";

        // Setup env
        setupEnvAndUpgradeClusterOperator(testParameters, produceMessagesCount, consumeMessagesCount, producerName, consumerName, continuousTopicName, continuousConsumerGroup, "", NAMESPACE);
        // Upgrade CO
        changeClusterOperator(testParameters, NAMESPACE);
        // Wait for Kafka cluster rolling update
        waitForKafkaClusterRollingUpdate();
        checkAllImages(testParameters.getJsonObject("imagesBeforeKafkaUpgrade"));
        logPodImages(clusterName);
        //  Upgrade kafka
        changeKafkaAndLogFormatVersion(testParameters.getJsonObject("proceduresAfterOperatorUpgrade"), clusterName, NAMESPACE);
        logPodImages(clusterName);
        checkAllImages(testParameters.getJsonObject("imagesAfterKafkaUpgrade"));

        // Verify upgrade
        verifyUpgradeProcedure(testParameters, produceMessagesCount, consumeMessagesCount, producerName, consumerName);
        // Verify that pods are stable
        PodUtils.verifyThatRunningPodsAreStable(clusterName);

        // Check errors in CO log
        assertNoCoErrorsLogged(0);
    }

    @BeforeEach
    void setupEnvironment() {
        cluster.createNamespace(NAMESPACE);
    }

    @Override
    protected void tearDownEnvironmentAfterEach() {
        deleteInstalledYamls(coDir, NAMESPACE);
        cluster.deleteNamespaces();
    }

    // There is no value of having teardown logic for class resources due to the fact that
    // CO was deployed by method StrimziUpgradeST.copyModifyApply() and removed by method StrimziUpgradeST.deleteInstalledYamls()
    @Override
    protected void tearDownEnvironmentAfterAll() {
    }
}
