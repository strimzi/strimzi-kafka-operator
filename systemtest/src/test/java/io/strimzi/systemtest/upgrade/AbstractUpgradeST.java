/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBasicExampleClients;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.test.TestUtils;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.IOUtils;
import org.junit.jupiter.params.provider.Arguments;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

public class AbstractUpgradeST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(AbstractUpgradeST.class);

    protected File coDir = null;
    protected File kafkaTopicYaml = null;
    protected File kafkaUserYaml = null;

    protected Map<String, String> zkPods;
    protected Map<String, String> kafkaPods;
    protected Map<String, String> eoPods;
    protected Map<String, String> coPods;

    protected final String clusterName = "my-cluster";

    protected final String topicName = "my-topic";
    protected final String userName = "my-user";
    protected final int upgradeTopicCount = 40;
    // ExpectedTopicCount contains additionally consumer-offset topic, my-topic and continuous-topic
    protected final int expectedTopicCount = upgradeTopicCount + 3;

    public static final String UPGRADE_JSON_FILE = "StrimziUpgradeST.json";
    public static final String DOWNGRADE_JSON_FILE = "StrimziDowngradeST.json";

    protected File kafkaYaml;

    protected static JsonArray readUpgradeJson(String fileName) {
        try {
            String jsonStr = IOUtils.toString(new FileReader(TestUtils.USER_PATH + "/src/test/resources/upgrade/" + fileName));

            return new JsonArray(jsonStr);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(TestUtils.USER_PATH + "/src/test/resources/upgrade/" + fileName + " file was not found.");
        }
    }

    protected static Stream<Arguments> loadJsonUpgradeData() {
        JsonArray upgradeData = readUpgradeJson(UPGRADE_JSON_FILE);
        List<Arguments> parameters = new LinkedList<>();

        List<TestKafkaVersion> testKafkaVersions = TestKafkaVersion.getKafkaVersions();
        TestKafkaVersion testKafkaVersion = testKafkaVersions.get(testKafkaVersions.size() - 1);

        upgradeData.forEach(jsonData -> {
            JsonObject data = (JsonObject) jsonData;
            data.put("urlTo", "HEAD");
            data.put("toVersion", "HEAD");
            data.put("toExamples", "HEAD");

            // Generate procedures for upgrade
            JsonObject procedures = new JsonObject();
            procedures.put("kafkaVersion", testKafkaVersion.version());
            procedures.put("logMessageVersion", testKafkaVersion.messageVersion());
            procedures.put("interBrokerProtocolVersion", testKafkaVersion.protocolVersion());
            data.put("proceduresAfterOperatorUpgrade", procedures);

            parameters.add(Arguments.of(data.getString("fromVersion"), "HEAD", data));
        });

        return parameters.stream();
    }

    protected static Stream<Arguments> loadJsonDowngradeData() {
        JsonArray upgradeData = readUpgradeJson(DOWNGRADE_JSON_FILE);
        List<Arguments> parameters = new LinkedList<>();

        upgradeData.forEach(jsonData -> {
            JsonObject data = (JsonObject) jsonData;
            parameters.add(Arguments.of(data.getString("fromVersion"), data.getString("toVersion"), data));
        });

        return parameters.stream();
    }

    protected void makeSnapshots(String clusterName) {
        coPods = DeploymentUtils.depSnapshot(ResourceManager.getCoDeploymentName());
        zkPods = StatefulSetUtils.ssSnapshot(KafkaResources.zookeeperStatefulSetName(clusterName));
        kafkaPods = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(clusterName));
        eoPods = DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(clusterName));
    }

    protected void changeKafkaAndLogFormatVersion(JsonObject procedures, String clusterName, String namespace) {
        // Get Kafka configurations
        Object currentLogMessageFormat = KafkaResource.kafkaClient().inNamespace(namespace).withName(clusterName).get().getSpec().getKafka().getConfig().get("log.message.format.version");
        Object currentInterBrokerProtocol = KafkaResource.kafkaClient().inNamespace(namespace).withName(clusterName).get().getSpec().getKafka().getConfig().get("inter.broker.protocol.version");
        // Get Kafka version
        String kafkaVersionFromCR = KafkaResource.kafkaClient().inNamespace(namespace).withName(clusterName).get().getSpec().getKafka().getVersion();
        String kafkaVersionFromProcedure = procedures.getString("kafkaVersion");

        if (!procedures.isEmpty() && (currentLogMessageFormat != null || currentInterBrokerProtocol != null)) {
            if (!kafkaVersionFromProcedure.isEmpty() && !kafkaVersionFromCR.equals(kafkaVersionFromProcedure)) {
                LOGGER.info("Going to set Kafka version to " + kafkaVersionFromProcedure);
                KafkaResource.replaceKafkaResource(clusterName, k -> k.getSpec().getKafka().setVersion(kafkaVersionFromProcedure));
                LOGGER.info("Wait until kafka rolling update is finished");
                kafkaPods = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaPods);
            }

            String logMessageVersion = procedures.getString("logMessageVersion");
            String interBrokerProtocolVersion = procedures.getString("interBrokerProtocolVersion");

            if (!logMessageVersion.isEmpty() || !interBrokerProtocolVersion.isEmpty()) {
                KafkaResource.replaceKafkaResource(clusterName, k -> {
                    if (!logMessageVersion.isEmpty()) {
                        LOGGER.info("Going to set log message format version to " + logMessageVersion);
                        k.getSpec().getKafka().getConfig().put("log.message.format.version", logMessageVersion);
                    }

                    if (!interBrokerProtocolVersion.isEmpty()) {
                        LOGGER.info("Going to set inter-broker protocol version to " + interBrokerProtocolVersion);
                        k.getSpec().getKafka().getConfig().put("inter.broker.protocol.version", interBrokerProtocolVersion);
                    }
                });

                if ((currentInterBrokerProtocol != null && !currentInterBrokerProtocol.equals(interBrokerProtocolVersion)) ||
                        (currentLogMessageFormat != null) && !currentLogMessageFormat.equals(logMessageVersion)) {
                    LOGGER.info("Wait until kafka rolling update is finished");
                    kafkaPods = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaPods);
                }
                makeSnapshots(clusterName);
            }
        } else {
            LOGGER.info("Waiting for all rolling updates finished - update of version)");
            kafkaPods = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaPods);
        }
    }

    protected void logPodImages(String clusterName) {
        List<Pod> pods = kubeClient().listPods(kubeClient().getStatefulSetSelectors(KafkaResources.zookeeperStatefulSetName(clusterName)));
        for (Pod pod : pods) {
            LOGGER.info("Pod {} has image {}", pod.getMetadata().getName(), pod.getSpec().getContainers().get(0).getImage());
        }
        pods = kubeClient().listPods(kubeClient().getStatefulSetSelectors(KafkaResources.kafkaStatefulSetName(clusterName)));
        for (Pod pod : pods) {
            LOGGER.info("Pod {} has image {}", pod.getMetadata().getName(), pod.getSpec().getContainers().get(0).getImage());
        }
        pods = kubeClient().listPods(kubeClient().getDeploymentSelectors(KafkaResources.entityOperatorDeploymentName(clusterName)));
        for (Pod pod : pods) {
            LOGGER.info("Pod {} has image {}", pod.getMetadata().getName(), pod.getSpec().getContainers().get(0).getImage());
            LOGGER.info("Pod {} has image {}", pod.getMetadata().getName(), pod.getSpec().getContainers().get(1).getImage());
        }
    }

    protected void waitForKafkaClusterRollingUpdate() {
        LOGGER.info("Waiting for ZK StatefulSet roll");
        zkPods = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.zookeeperStatefulSetName(clusterName), 3, zkPods);
        LOGGER.info("Waiting for Kafka StatefulSet roll");
        kafkaPods = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(clusterName), 3, kafkaPods);
        LOGGER.info("Waiting for EO Deployment roll");
        // Check the TO and UO also got upgraded
        eoPods = DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoPods);
    }

    protected void waitForReadinessOfKafkaCluster() {
        LOGGER.info("Waiting for Zookeeper StatefulSet");
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.zookeeperStatefulSetName(clusterName), 3);
        LOGGER.info("Waiting for Kafka StatefulSet");
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(clusterName), 3);
        LOGGER.info("Waiting for EO Deployment");
        DeploymentUtils.waitForDeploymentAndPodsReady(KafkaResources.entityOperatorDeploymentName(clusterName), 1);
    }

    protected void changeClusterOperator(JsonObject testParameters, String namespace) throws IOException {
        File coDir;
        // Modify + apply installation files
        LOGGER.info("Going to update CO from {} to {}", testParameters.getString("fromVersion"), testParameters.getString("toVersion"));
        if ("HEAD".equals(testParameters.getString("toVersion"))) {
            coDir = new File(TestUtils.USER_PATH + "/../install/cluster-operator");
        } else {
            String url = testParameters.getString("urlTo");
            File dir = FileUtils.downloadAndUnzip(url);
            coDir = new File(dir, testParameters.getString("toExamples") + "/install/cluster-operator/");
        }

        copyModifyApply(coDir, namespace);

        LOGGER.info("Waiting for CO upgrade");
        DeploymentUtils.waitTillDepHasRolled(ResourceManager.getCoDeploymentName(), 1, coPods);
    }

    protected void copyModifyApply(File root, String namespace) {
        Arrays.stream(Objects.requireNonNull(root.listFiles())).sorted().forEach(f -> {
            if (f.getName().matches(".*RoleBinding.*")) {
                cmdKubeClient().replaceContent(TestUtils.changeRoleBindingSubject(f, namespace));
            } else if (f.getName().matches(".*Deployment.*")) {
                cmdKubeClient().replaceContent(StUtils.changeDeploymentNamespace(f, namespace));
            } else {
                cmdKubeClient().replaceContent(TestUtils.getContent(f, TestUtils::toYamlString));
            }
        });
    }

    protected void deleteInstalledYamls(File root, String namespace) {
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
                        cmdKubeClient().deleteContent(TestUtils.changeRoleBindingSubject(f, namespace));
                    } else {
                        cmdKubeClient().delete(f);
                    }
                } catch (Exception ex) {
                    LOGGER.warn("Failed to delete resources: {}", f.getName());
                }
            });
        }
    }

    protected void checkAllImages(JsonObject images) {
        if (images.isEmpty()) {
            fail("There are no expected images");
        }
        String zkImage = images.getString("zookeeper");
        String kafkaImage = images.getString("kafka");
        String tOImage = images.getString("topicOperator");
        String uOImage = images.getString("userOperator");

        checkContainerImages(kubeClient().getStatefulSet(KafkaResources.zookeeperStatefulSetName(clusterName)).getSpec().getSelector().getMatchLabels(), zkImage);
        checkContainerImages(kubeClient().getStatefulSet(KafkaResources.kafkaStatefulSetName(clusterName)).getSpec().getSelector().getMatchLabels(), kafkaImage);
        checkContainerImages(kubeClient().getDeployment(KafkaResources.entityOperatorDeploymentName(clusterName)).getSpec().getSelector().getMatchLabels(), tOImage);
        checkContainerImages(kubeClient().getDeployment(KafkaResources.entityOperatorDeploymentName(clusterName)).getSpec().getSelector().getMatchLabels(), 1, uOImage);
    }

    protected void checkContainerImages(Map<String, String> matchLabels, String image) {
        checkContainerImages(matchLabels, 0, image);
    }

    protected void checkContainerImages(Map<String, String> matchLabels, int container, String image) {
        List<Pod> pods1 = kubeClient().listPods(matchLabels);
        for (Pod pod : pods1) {
            if (!image.equals(pod.getSpec().getContainers().get(container).getImage())) {
                LOGGER.debug("Expected image for pod {}: {} \nCurrent image: {}", pod.getMetadata().getName(), StUtils.changeOrgAndTag(image), pod.getSpec().getContainers().get(container).getImage());
                assertThat("Used image for pod " + pod.getMetadata().getName() + " is not valid!", pod.getSpec().getContainers().get(container).getImage(), containsString(StUtils.changeOrgAndTag(image)));
            }
        }
    }

    protected void deployClients(String image, KafkaUser kafkaUser) {
        if (image.contains(":latest"))  {
            image = StUtils.changeOrgAndTag(image);
        }

        LOGGER.info("Deploying Kafka clients with image {}", image);

        // Deploy new clients
        KafkaClientsResource.createAndWaitForReadiness(KafkaClientsResource.deployKafkaClients(true, clusterName + "-" + Constants.KAFKA_CLIENTS, kafkaUser)
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

    protected void setupEnvAndUpgradeClusterOperator(JsonObject testParameters, int produceMessagesCount, int consumeMessagesCount,
                                                   String producerName, String consumerName,
                                                   String continuousTopicName, String continuousConsumerGroup,
                                                   String kafkaVersion, String namespace) throws IOException {
        int continuousClientsMessageCount = testParameters.getJsonObject("client").getInteger("continuousClientsMessages");

        LOGGER.info("Going to test upgrade of Cluster Operator from version {} to version {}", testParameters.getString("fromVersion"), testParameters.getString("toVersion"));
        cluster.setNamespace(namespace);

        String url = null;
        File dir = null;

        if ("HEAD".equals(testParameters.getString("fromVersion"))) {
            coDir = new File(TestUtils.USER_PATH + "/../install/cluster-operator");
        } else {
            url = testParameters.getString("urlFrom");
            dir = FileUtils.downloadAndUnzip(url);
            coDir = new File(dir, testParameters.getString("fromExamples") + "/install/cluster-operator/");
        }

        // Modify + apply installation files
        copyModifyApply(coDir, namespace);

        LOGGER.info("Waiting for CO deployment");
        DeploymentUtils.waitForDeploymentAndPodsReady(ResourceManager.getCoDeploymentName(), 1);
        LOGGER.info("CO ready");

        // In chainUpgrade we want to setup Kafka only at the begging and then upgrade it via CO
        if (KafkaResource.kafkaClient().inNamespace(namespace).withName(clusterName).get() == null) {
            // Deploy a Kafka cluster
            if ("HEAD".equals(testParameters.getString("fromVersion"))) {
                KafkaResource.createAndWaitForReadiness(KafkaResource.kafkaPersistent(clusterName, 3, 3).build());
            } else {
                kafkaYaml = new File(dir, testParameters.getString("fromExamples") + "/examples/kafka/kafka-persistent.yaml");
                LOGGER.info("Going to deploy Kafka from: {}", kafkaYaml.getPath());
                // Change kafka version of it's empty (null is for remove the version)
                cmdKubeClient().applyContent(KafkaUtils.changeOrRemoveKafkaVersion(kafkaYaml, kafkaVersion));
                // Wait for readiness
                waitForReadinessOfKafkaCluster();
            }
        }
        // We don't need to update KafkaUser during chain upgrade this way
        if (KafkaUserResource.kafkaUserClient().inNamespace(namespace).withName(userName).get() == null) {
            if ("HEAD".equals(testParameters.getString("fromVersion"))) {
                KafkaUserResource.createAndWaitForReadiness(KafkaUserResource.tlsUser(clusterName, userName).build());
            } else {
                kafkaUserYaml = new File(dir, testParameters.getString("fromExamples") + "/examples/user/kafka-user.yaml");
                LOGGER.info("Going to deploy KafkaUser from: {}", kafkaUserYaml.getPath());
                cmdKubeClient().create(kafkaUserYaml);
            }
        }
        // We don't need to update KafkaTopic during chain upgrade this way
        if (KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).withName(topicName).get() == null) {
            if ("HEAD".equals(testParameters.getString("fromVersion"))) {
                KafkaTopicResource.createAndWaitForReadiness(KafkaTopicResource.topic(clusterName, topicName).build());
            } else {
                kafkaTopicYaml = new File(dir, testParameters.getString("fromExamples") + "/examples/topic/kafka-topic.yaml");
                LOGGER.info("Going to deploy KafkaTopic from: {}", kafkaTopicYaml.getPath());
                cmdKubeClient().create(kafkaTopicYaml);
            }
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
            if (KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).withName(continuousTopicName).get() == null) {
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
            .withNamespaceName(namespace)
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

    protected void verifyProcedure(JsonObject testParameters, int produceMessagesCount, int consumeMessagesCount,
                                 String producerName, String consumerName, String namespace) {
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
            .withNamespaceName(namespace)
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
            List<KafkaTopic> kafkaTopicList = KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).list().getItems();
            int additionalTopics = testParameters.getInteger("additionalTopics", 0);
            assertThat("KafkaTopic list doesn't have expected size", kafkaTopicList.size(), is(expectedTopicCount + additionalTopics));
            assertThat("KafkaTopic " + topicName + " is not in expected topic list",
                    kafkaTopicList.contains(KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).withName(topicName).get()), is(true));
            for (int x = 0; x < upgradeTopicCount; x++) {
                KafkaTopic kafkaTopic = KafkaTopicResource.kafkaTopicClient().inNamespace(namespace).withName(topicName + "-" + x).get();
                assertThat("KafkaTopic " + topicName + "-" + x + " is not in expected topic list", kafkaTopicList.contains(kafkaTopic), is(true));
            }
        }

        if (continuousClientsMessageCount != 0) {
            // ##############################
            // Validate that continuous clients finished successfully
            // ##############################
            ClientUtils.waitTillContinuousClientsFinish(producerName, consumerName, namespace, continuousClientsMessageCount);
            // ##############################
            // Delete jobs to make same names available for next upgrade run during chain upgrade
            kubeClient().deleteJob(producerName);
            kubeClient().deleteJob(consumerName);
        }
    }
}
