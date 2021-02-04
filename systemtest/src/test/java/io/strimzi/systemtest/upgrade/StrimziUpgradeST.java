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
import io.strimzi.systemtest.logs.TestExecutionWatcher;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBasicExampleClients;
import io.strimzi.systemtest.resources.operator.BundleResource;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
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
    @MethodSource("loadJsonData")
    @Tag(INTERNAL_CLIENTS_USED)
    void testUpgradeStrimziVersion(String from, String to, JsonObject parameters) throws Exception {

        assumeTrue(StUtils.isAllowOnCurrentEnvironment(parameters.getJsonObject("environmentInfo").getString("flakyEnvVariable")));
        assumeTrue(StUtils.isAllowedOnCurrentK8sVersion(parameters.getJsonObject("environmentInfo").getString("maxK8sVersion")));

        LOGGER.debug("Running upgrade test from version {} to {}", from, to);

        try {
            performUpgrade(parameters, MESSAGE_COUNT, MESSAGE_COUNT);
            // Tidy up
        } catch (Exception e) {
            e.printStackTrace();
            TestExecutionWatcher.collectLogs(testClass, testName);
            try {
                if (kafkaYaml != null) {
                    cmdKubeClient().delete(kafkaYaml);
                }
            } catch (Exception ex) {
                LOGGER.warn("Failed to delete resources: {}", kafkaYaml.getName());
            }
            try {
                if (coDir != null) {
                    cmdKubeClient().delete(coDir);
                }
            } catch (Exception ex) {
                LOGGER.warn("Failed to delete resources: {}", coDir.getName());
            }

            throw e;
        } finally {
            deleteInstalledYamls(coDir, NAMESPACE);
        }
    }

    @Test
    @Tag(INTERNAL_CLIENTS_USED)
    void testChainUpgrade() throws Exception {
        JsonArray parameters = readUpgradeJson(UPGRADE_JSON_FILE);

        int consumedMessagesCount = MESSAGE_COUNT;

        try {
            for (Object testParameters : parameters) {
                JsonObject castTestParameters = (JsonObject) testParameters;
                if (StUtils.isAllowOnCurrentEnvironment(castTestParameters.getJsonObject("environmentInfo").getString("flakyEnvVariable")) &&
                    StUtils.isAllowedOnCurrentK8sVersion(castTestParameters.getJsonObject("environmentInfo").getString("maxK8sVersion"))) {
                    performUpgrade(castTestParameters, MESSAGE_COUNT, consumedMessagesCount);
                    consumedMessagesCount = consumedMessagesCount + MESSAGE_COUNT;
                } else {
                    LOGGER.info("Upgrade of Cluster Operator from version {} to version {} is not allowed on this K8S version!", castTestParameters.getString("fromVersion"), castTestParameters.getString("toVersion"));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            TestExecutionWatcher.collectLogs(testClass, testName);
            try {
                if (kafkaYaml != null) {
                    cmdKubeClient().delete(kafkaYaml);
                }
            } catch (Exception ex) {
                LOGGER.warn("Failed to delete resources: {}", kafkaYaml.getName());
            }
            try {
                if (coDir != null) {
                    cmdKubeClient().delete(coDir);
                }
            } catch (Exception ex) {
                LOGGER.warn("Failed to delete resources: {}", coDir.getName());
            }

            throw e;
        } finally {
            deleteInstalledYamls(coDir, NAMESPACE);
        }
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
        setupUpgradeEnvAndUpgradeClusterOperator(acrossUpgradeData, MESSAGE_COUNT, MESSAGE_COUNT, producerName, consumerName, continuousTopicName, continuousConsumerGroup, acrossUpgradeData.getString("startingKafkaVersion"));
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
        setupUpgradeEnvAndUpgradeClusterOperator(acrossUpgradeData, MESSAGE_COUNT, MESSAGE_COUNT, producerName, consumerName, continuousTopicName, continuousConsumerGroup, null);
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
        acrossUpgradeData.put("startingKafkaVersion", startingVersion.getString("oldestKafka"));
        acrossUpgradeData.getJsonObject("proceduresAfterOperatorUpgrade").put("kafkaVersion", latestKafkaSupported.version());
        acrossUpgradeData.getJsonObject("proceduresAfterOperatorUpgrade").put("logMessageVersion", latestKafkaSupported.messageVersion());
        acrossUpgradeData.getJsonObject("proceduresAfterOperatorUpgrade").put("interBrokerProtocolVersion", latestKafkaSupported.protocolVersion());

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

    private void setupUpgradeEnvAndUpgradeClusterOperator(JsonObject testParameters, int produceMessagesCount, int consumeMessagesCount,
                                                          String producerName, String consumerName,
                                                          String continuousTopicName, String continuousConsumerGroup,
                                                          String kafkaVersion) throws IOException {
        int continuousClientsMessageCount = testParameters.getJsonObject("client").getInteger("continuousClientsMessages");

        LOGGER.info("Going to test upgrade of Cluster Operator from version {} to version {}", testParameters.getString("fromVersion"), testParameters.getString("toVersion"));
        cluster.setNamespace(NAMESPACE);

        String url = testParameters.getString("urlFrom");
        File dir = FileUtils.downloadAndUnzip(url);

        coDir = new File(dir, testParameters.getString("fromExamples") + "/install/cluster-operator/");

        // Modify + apply installation files
        copyModifyApply(coDir, NAMESPACE);

        LOGGER.info("Waiting for CO deployment");
        DeploymentUtils.waitForDeploymentAndPodsReady(ResourceManager.getCoDeploymentName(), 1);
        LOGGER.info("CO ready");

        // In chainUpgrade we want to setup Kafka only at the begging and then upgrade it via CO
        if (KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(clusterName).get() == null) {
            // Deploy a Kafka cluster
            kafkaYaml = new File(dir, testParameters.getString("fromExamples") + "/examples/kafka/kafka-persistent.yaml");
            LOGGER.info("Going to deploy Kafka from: {}", kafkaYaml.getPath());
            // Change kafka version of it's empty (null is for remove the version)
            cmdKubeClient().applyContent(KafkaUtils.changeOrRemoveKafkaVersion(kafkaYaml, kafkaVersion));
            // Wait for readiness
            waitForReadinessOfKafkaCluster();
        }
        // We don't need to update KafkaUser during chain upgrade this way
        if (KafkaUserResource.kafkaUserClient().inNamespace(NAMESPACE).withName(userName).get() == null) {
            kafkaUserYaml = new File(dir, testParameters.getString("fromExamples") + "/examples/user/kafka-user.yaml");
            LOGGER.info("Going to deploy KafkaUser from: {}", kafkaUserYaml.getPath());
            cmdKubeClient().create(kafkaUserYaml);
        }
        // We don't need to update KafkaTopic during chain upgrade this way
        if (KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(topicName).get() == null) {
            kafkaTopicYaml = new File(dir, testParameters.getString("fromExamples") + "/examples/topic/kafka-topic.yaml");
            LOGGER.info("Going to deploy KafkaTopic from: {}", kafkaTopicYaml.getPath());
            cmdKubeClient().create(kafkaTopicYaml);
        }
        // Create bunch of topics for upgrade if it's specified in configuration
        if (testParameters.getBoolean("generateTopics")) {
            for (int x = 0; x < upgradeTopicCount; x++) {
                KafkaTopicResource.topicWithoutWait(KafkaTopicResource.defaultTopic(clusterName, topicName + "-" + x, 1, 1, 1)
                        .editSpec()
                        .withTopicName(topicName + "-" + x)
                        .endSpec()
                        .build());
            }
        }

        if (continuousClientsMessageCount != 0) {
            // ##############################
            // Attach clients which will continuously produce/consume messages to/from Kafka brokers during rolling update
            // ##############################
            // Setup topic, which has 3 replicas and 2 min.isr to see if producer will be able to work during rolling update
            if (KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).withName(continuousTopicName).get() == null) {
                KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(clusterName, continuousTopicName, 3, 3, 2).build());
            }

            String producerAdditionConfiguration = "delivery.timeout.ms=20000\nrequest.timeout.ms=20000";

            KafkaBasicExampleClients kafkaBasicClientJob = new KafkaBasicExampleClients.Builder()
                    .withProducerName(producerName)
                    .withConsumerName(consumerName)
                    .withBootstrapAddress(KafkaResources.plainBootstrapAddress(clusterName))
                    .withTopicName(continuousTopicName)
                    .withMessageCount(continuousClientsMessageCount)
                    .withAdditionalConfig(producerAdditionConfiguration)
                    .withConsumerGroup(continuousConsumerGroup)
                    .withDelayMs(1000)
                    .build();

            kafkaBasicClientJob.createAndWaitForReadiness(kafkaBasicClientJob.producerStrimzi().build());
            kafkaBasicClientJob.createAndWaitForReadiness(kafkaBasicClientJob.consumerStrimzi().build());
            // ##############################
        }

        // Wait until user will be created
        SecretUtils.waitForSecretReady(userName);
        TestUtils.waitFor("KafkaUser " + userName + " availability", Constants.GLOBAL_POLL_INTERVAL_MEDIUM,
                ResourceOperation.getTimeoutForResourceReadiness(KafkaUser.RESOURCE_KIND),
            () -> !cmdKubeClient().getResourceAsYaml("kafkauser", userName).equals(""));

        // Deploy clients and exchange messages
        KafkaUser kafkaUser = TestUtils.fromYamlString(cmdKubeClient().getResourceAsYaml("kafkauser", userName), KafkaUser.class);
        deployClients(testParameters.getJsonObject("client").getString("beforeKafkaUpgradeDowngrade"), kafkaUser);

        final String defaultKafkaClientsPodName =
                kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
                .withUsingPodName(defaultKafkaClientsPodName)
                .withTopicName(topicName)
                .withNamespaceName(NAMESPACE)
                .withClusterName(clusterName)
                .withKafkaUsername(userName)
                .withMessageCount(produceMessagesCount)
                .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
                .build();

        int sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(produceMessagesCount));

        internalKafkaClient.setMessageCount(consumeMessagesCount);

        int received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(consumeMessagesCount));

        makeSnapshots(clusterName);
        logPodImages(clusterName);
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

    @SuppressWarnings("MethodLength")
    private void performUpgrade(JsonObject testParameters, int produceMessagesCount, int consumeMessagesCount) throws IOException {
        String continuousTopicName = "continuous-topic";
        String producerName = "hello-world-producer";
        String consumerName = "hello-world-consumer";
        String continuousConsumerGroup = "continuous-consumer-group";

        // Setup env
        setupUpgradeEnvAndUpgradeClusterOperator(testParameters, produceMessagesCount, consumeMessagesCount, producerName, consumerName, continuousTopicName, continuousConsumerGroup, "");
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

        // Check errors in CO log
        assertNoCoErrorsLogged(0);
    }

    @BeforeEach
    void setupEnvironment() {
        cluster.createNamespace(NAMESPACE);
    }

    @Override
    protected void tearDownEnvironmentAfterEach() {
        cluster.deleteNamespaces();
    }

    // There is no value of having teardown logic for class resources due to the fact that
    // CO was deployed by method StrimziUpgradeST.copyModifyApply() and removed by method StrimziUpgradeST.deleteInstalledYamls()
    @Override
    protected void tearDownEnvironmentAfterAll() {
    }
}
