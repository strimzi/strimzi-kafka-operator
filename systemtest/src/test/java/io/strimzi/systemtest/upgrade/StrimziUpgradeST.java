/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.logs.TestExecutionWatcher;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.UPGRADE;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(UPGRADE)
public class StrimziUpgradeST extends AbstractUpgradeST {

    private static final Logger LOGGER = LogManager.getLogger(StrimziUpgradeST.class);

    public static final String NAMESPACE = "strimzi-upgrade-test";

    private final String kafkaClusterName = "my-cluster";
    private String zkStsName = KafkaResources.zookeeperStatefulSetName(kafkaClusterName);
    private String kafkaStsName = KafkaResources.kafkaStatefulSetName(kafkaClusterName);
    private String eoDepName = KafkaResources.entityOperatorDeploymentName(kafkaClusterName);

    private File coDir = null;
    private File kafkaTopicYaml = null;
    private File kafkaUserYaml = null;

    private final String topicName = "my-topic";
    private final String userName = "my-user";
    private final int upgradeTopicCount = 40;
    // ExpectedTopicCount contains additionally consumer-offset topic, my-topic and continuous-topic
    private final int expectedTopicCount = upgradeTopicCount + 3;

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
            deleteInstalledYamls(coDir);
        }
    }

    @Test
    @Tag(INTERNAL_CLIENTS_USED)
    void testChainUpgrade() throws Exception {
        JsonArray parameters = readUpgradeJson();

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
            deleteInstalledYamls(coDir);
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
        copyModifyApply(coDir);
        // Apply Kafka Persistent without version
        KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaFromYaml(previousKafkaPersistent, kafkaClusterName, 3, 3)
            .editSpec()
                .editKafka()
                    .withVersion(null)
                    .addToConfig("log.message.format.version", getValueForLastKafkaVersionInFile(previousKafkaVersionsYaml, "format"))
                    .addToConfig("inter.broker.protocol.version", getValueForLastKafkaVersionInFile(previousKafkaVersionsYaml, "protocol"))
                .endKafka()
            .endSpec()
            .build());

        assertNull(KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(kafkaClusterName).get().getSpec().getKafka().getVersion());

        Map<String, String> operatorSnapshot = DeploymentUtils.depSnapshot(ResourceManager.getCoDeploymentName());
        Map<String, String> zooSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(kafkaClusterName));
        Map<String, String> kafkaSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(kafkaClusterName));
        Map<String, String> eoSnapshot = DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(kafkaClusterName));

        // Update CRDs, CRB, etc.
        applyClusterOperatorInstallFiles(NAMESPACE);
        applyBindings(NAMESPACE);

        kubeClient().getClient().apps().deployments().inNamespace(NAMESPACE).withName(ResourceManager.getCoDeploymentName()).delete();
        kubeClient().getClient().apps().deployments().inNamespace(NAMESPACE).withName(ResourceManager.getCoDeploymentName()).create(BundleResource.defaultClusterOperator(NAMESPACE).build());

        DeploymentUtils.waitTillDepHasRolled(ResourceManager.getCoDeploymentName(), 1, operatorSnapshot);
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.zookeeperStatefulSetName(kafkaClusterName), 3, zooSnapshot);
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(kafkaClusterName), 3, kafkaSnapshot);
        DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(kafkaClusterName), 1, eoSnapshot);

        assertThat(KafkaUtils.getVersionFromKafkaPodLibs(KafkaResources.kafkaPodName(kafkaClusterName, 0)), containsString(latestKafkaVersion));
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
        makeSnapshots(kafkaClusterName);
        // Upgrade CO
        upgradeClusterOperator(acrossUpgradeData);
        logPodImages(kafkaClusterName);
        //  Upgrade kafka
        changeKafkaAndLogFormatVersion(acrossUpgradeData.getJsonObject("proceduresAfter"), kafkaClusterName, NAMESPACE);
        logPodImages(kafkaClusterName);
        checkAllImages(acrossUpgradeData.getJsonObject("imagesAfterKafkaUpdate"));
        // Verify that pods are stable
        PodUtils.verifyThatRunningPodsAreStable(kafkaClusterName);
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
        upgradeClusterOperator(acrossUpgradeData);
        // Wait till first upgrade finished
        zkPods = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.zookeeperStatefulSetName(kafkaClusterName), 3, zkPods);
        kafkaPods = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(kafkaClusterName), 3, kafkaPods);
        eoPods = DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(kafkaClusterName), 1, eoPods);

        LOGGER.info("Rolling to new images has finished!");
        logPodImages(kafkaClusterName);
        //  Upgrade kafka
        changeKafkaAndLogFormatVersion(acrossUpgradeData.getJsonObject("proceduresAfter"), kafkaClusterName, NAMESPACE);
        logPodImages(kafkaClusterName);
        checkAllImages(acrossUpgradeData.getJsonObject("imagesAfterKafkaUpdate"));
        // Verify that pods are stable
        PodUtils.verifyThatRunningPodsAreStable(kafkaClusterName);
        // Verify upgrade
        verifyUpgradeProcedure(acrossUpgradeData, MESSAGE_COUNT, MESSAGE_COUNT, producerName, consumerName);

        // Check errors in CO log
        assertNoCoErrorsLogged(0);
    }

    private JsonObject buildDataForUpgradeAcrossVersions() {
        List<TestKafkaVersion> sortedVersions = TestKafkaVersion.getKafkaVersions();
        TestKafkaVersion latestKafkaSupported = sortedVersions.get(sortedVersions.size() - 1);

        JsonArray upgradeJson = readUpgradeJson();

        JsonObject acrossUpgradeData = upgradeJson.getJsonObject(upgradeJson.size() - 1);
        JsonObject startingVersion = getDataForStartUpgrade(upgradeJson);

        acrossUpgradeData.put("fromVersion", startingVersion.getValue("fromVersion"));
        acrossUpgradeData.put("fromExamples", startingVersion.getValue("fromExamples"));
        acrossUpgradeData.put("urlFrom", startingVersion.getValue("urlFrom"));
        acrossUpgradeData.put("startingKafkaVersion", startingVersion.getString("oldestKafka"));
        acrossUpgradeData.getJsonObject("proceduresAfter").put("kafkaVersion", latestKafkaSupported.version());
        acrossUpgradeData.getJsonObject("proceduresAfter").put("logMessageVersion", latestKafkaSupported.messageVersion());
        acrossUpgradeData.getJsonObject("proceduresAfter").put("interBrokerProtocolVersion", latestKafkaSupported.protocolVersion());

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
        copyModifyApply(coDir);

        LOGGER.info("Waiting for CO deployment");
        DeploymentUtils.waitForDeploymentAndPodsReady(ResourceManager.getCoDeploymentName(), 1);
        LOGGER.info("CO ready");

        // In chainUpgrade we want to setup Kafka only at the begging and then upgrade it via CO
        if (KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(kafkaClusterName).get() == null) {
            // Deploy a Kafka cluster
            kafkaYaml = new File(dir, testParameters.getString("fromExamples") + "/examples/kafka/kafka-persistent.yaml");
            LOGGER.info("Going to deploy Kafka from: {}", kafkaYaml.getPath());
            // Change kafka version of it's empty (null is for remove the version)
            cmdKubeClient().applyContent(KafkaUtils.changeOrRemoveKafkaVersion(kafkaYaml, kafkaVersion));
            // Wait for readiness
            waitForReadinessOfKafkaCluster(kafkaClusterName);
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
                KafkaTopicResource.topicWithoutWait(KafkaTopicResource.defaultTopic(kafkaClusterName, topicName + "-" + x, 1, 1, 1)
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
                KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(kafkaClusterName, continuousTopicName, 3, 3, 2).build());
            }

            String producerAdditionConfiguration = "delivery.timeout.ms=20000\nrequest.timeout.ms=20000";

            KafkaBasicExampleClients kafkaBasicClientJob = new KafkaBasicExampleClients.Builder()
                    .withProducerName(producerName)
                    .withConsumerName(consumerName)
                    .withBootstrapAddress(KafkaResources.plainBootstrapAddress(kafkaClusterName))
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
        deployClients(testParameters.getJsonObject("client").getString("beforeKafkaUpdate"), kafkaUser);

        final String defaultKafkaClientsPodName =
                kubeClient().listPodsByPrefixInName(kafkaClusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
                .withUsingPodName(defaultKafkaClientsPodName)
                .withTopicName(topicName)
                .withNamespaceName(NAMESPACE)
                .withClusterName(kafkaClusterName)
                .withKafkaUsername(userName)
                .withMessageCount(produceMessagesCount)
                .withListenerName(Constants.TLS_LISTENER_DEFAULT_NAME)
                .build();

        int sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(produceMessagesCount));

        internalKafkaClient.setMessageCount(consumeMessagesCount);

        int received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(consumeMessagesCount));

        makeSnapshots(kafkaClusterName);
        logPodImages(kafkaClusterName);
    }

    private void verifyUpgradeProcedure(JsonObject testParameters, int produceMessagesCount, int consumeMessagesCount,
                                        String producerName, String consumerName) {
        int continuousClientsMessageCount = testParameters.getJsonObject("client").getInteger("continuousClientsMessages");

        // Delete old clients
        kubeClient().deleteDeployment(kafkaClusterName + "-" + Constants.KAFKA_CLIENTS);
        DeploymentUtils.waitForDeploymentDeletion(kafkaClusterName + "-" + Constants.KAFKA_CLIENTS);

        KafkaUser kafkaUser = TestUtils.fromYamlString(cmdKubeClient().getResourceAsYaml("kafkauser", userName), KafkaUser.class);
        deployClients(testParameters.getJsonObject("client").getString("afterKafkaUpdate"), kafkaUser);

        final String afterUpgradeKafkaClientsPodName =
                kubeClient().listPodsByPrefixInName(kafkaClusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
                .withUsingPodName(afterUpgradeKafkaClientsPodName)
                .withTopicName(topicName)
                .withNamespaceName(NAMESPACE)
                .withClusterName(kafkaClusterName)
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
        upgradeClusterOperator(testParameters);
        // Wait for Kafka cluster rolling update
        waitForKafkaClusterUpgrade(kafkaClusterName);
        checkAllImages(testParameters.getJsonObject("imagesBeforeKafkaUpdate"));
        logPodImages(kafkaClusterName);
        //  Upgrade kafka
        changeKafkaAndLogFormatVersion(testParameters.getJsonObject("proceduresAfter"), kafkaClusterName, NAMESPACE);
        logPodImages(kafkaClusterName);
        checkAllImages(testParameters.getJsonObject("imagesAfterKafkaUpdate"));

        // Verify upgrade
        verifyUpgradeProcedure(testParameters, produceMessagesCount, consumeMessagesCount, producerName, consumerName);

        // Check errors in CO log
        assertNoCoErrorsLogged(0);
    }

    private void upgradeClusterOperator(JsonObject testParameters) throws IOException {
        // Upgrade the CO
        // Modify + apply installation files
        LOGGER.info("Going to update CO from {} to {}", testParameters.getString("fromVersion"), testParameters.getString("toVersion"));
        if ("HEAD".equals(testParameters.getString("toVersion"))) {
            coDir = new File(TestUtils.USER_PATH + "/../install/cluster-operator");
        } else {
            String url = testParameters.getString("urlTo");
            File dir = FileUtils.downloadAndUnzip(url);
            coDir = new File(dir, testParameters.getString("toExamples") + "/install/cluster-operator/");
        }

        copyModifyApply(coDir);

        LOGGER.info("Waiting for CO upgrade");
        DeploymentUtils.waitTillDepHasRolled(ResourceManager.getCoDeploymentName(), 1, coPods);
    }

    private void copyModifyApply(File root) {
        Arrays.stream(Objects.requireNonNull(root.listFiles())).sorted().forEach(f -> {
            if (f.getName().matches(".*RoleBinding.*")) {
                cmdKubeClient().applyContent(TestUtils.changeRoleBindingSubject(f, NAMESPACE));
            } else if (f.getName().matches(".*Deployment.*")) {
                cmdKubeClient().applyContent(StUtils.changeDeploymentNamespace(f, NAMESPACE));
            } else {
                cmdKubeClient().apply(f);
            }
        });
    }

    private void deleteInstalledYamls(File root) {
        if (kafkaUserYaml != null) {
            LOGGER.info("Deleting KafkaUser configuration files");
            cmdKubeClient().delete(kafkaUserYaml);
        }
        if (kafkaTopicYaml != null) {
            LOGGER.info("Deleting KafkaTopic configuration files");
            cmdKubeClient().delete(kafkaTopicYaml);
        }
        if (kafkaYaml != null) {
            LOGGER.info("Deleting Kafka configuration files");
            cmdKubeClient().delete(kafkaYaml);
        }
        if (root != null) {
            Arrays.stream(Objects.requireNonNull(root.listFiles())).sorted().forEach(f -> {
                try {
                    if (f.getName().matches(".*RoleBinding.*")) {
                        cmdKubeClient().deleteContent(TestUtils.changeRoleBindingSubject(f, NAMESPACE));
                    } else {
                        cmdKubeClient().delete(f);
                    }
                } catch (Exception ex) {
                    LOGGER.warn("Failed to delete resources: {}", f.getName());
                }
            });
        }
    }

    private void checkAllImages(JsonObject images) {
        if (images.isEmpty()) {
            fail("There are no expected images");
        }
        String zkImage = images.getString("zookeeper");
        String kafkaImage = images.getString("kafka");
        String tOImage = images.getString("topicOperator");
        String uOImage = images.getString("userOperator");

        checkContainerImages(kubeClient().getStatefulSet(zkStsName).getSpec().getSelector().getMatchLabels(), zkImage);
        checkContainerImages(kubeClient().getStatefulSet(kafkaStsName).getSpec().getSelector().getMatchLabels(), kafkaImage);
        checkContainerImages(kubeClient().getDeployment(eoDepName).getSpec().getSelector().getMatchLabels(), tOImage);
        checkContainerImages(kubeClient().getDeployment(eoDepName).getSpec().getSelector().getMatchLabels(), 1, uOImage);
    }

    private void checkContainerImages(Map<String, String> matchLabels, String image) {
        checkContainerImages(matchLabels, 0, image);
    }

    private void checkContainerImages(Map<String, String> matchLabels, int container, String image) {
        List<Pod> pods1 = kubeClient().listPods(matchLabels);
        for (Pod pod : pods1) {
            if (!image.equals(pod.getSpec().getContainers().get(container).getImage())) {
                LOGGER.debug("Expected image for pod {}: {} \nCurrent image: {}", pod.getMetadata().getName(), StUtils.changeOrgAndTag(image), pod.getSpec().getContainers().get(container).getImage());
                assertThat("Used image for pod " + pod.getMetadata().getName() + " is not valid!", pod.getSpec().getContainers().get(container).getImage(), containsString(StUtils.changeOrgAndTag(image)));
            }
        }
    }

    void deployClients(String image, KafkaUser kafkaUser) {
        if (image.contains(":latest"))  {
            image = StUtils.changeOrgAndTag(image);
        }

        LOGGER.info("Deploying Kafka clients with image {}", image);

        // Deploy new clients
        KafkaClientsResource.createAndWaitForReadiness(KafkaClientsResource.deployKafkaClients(true, kafkaClusterName + "-" + Constants.KAFKA_CLIENTS, kafkaUser)
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .editFirstContainer()
                            .withImage(image)
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build());
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
