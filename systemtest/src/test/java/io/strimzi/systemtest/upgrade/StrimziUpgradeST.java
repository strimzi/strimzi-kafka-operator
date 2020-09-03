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
import io.strimzi.systemtest.logs.TestExecutionWatcher;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.resources.operator.BundleResource;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.UPGRADE;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(UPGRADE)
public class StrimziUpgradeST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(StrimziUpgradeST.class);

    public static final String NAMESPACE = "strimzi-upgrade-test";
    private String zkStsName = KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME);
    private String kafkaStsName = KafkaResources.kafkaStatefulSetName(CLUSTER_NAME);
    private String eoDepName = KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME);

    private Map<String, String> zkPods;
    private Map<String, String> kafkaPods;
    private Map<String, String> eoPods;
    private Map<String, String> coPods;

    private File coDir = null;
    private File kafkaYaml = null;
    private File kafkaTopicYaml = null;
    private File kafkaUserYaml = null;

    private final String kafkaClusterName = "my-cluster";
    private final String topicName = "my-topic";
    private final String userName = "my-user";
    private final int upgradeTopicCount = 40;
    // ExpectedTopicCount contains additionally consumer-offset topic, my-topic and continuous-topic
    private final int expectedTopicCount = upgradeTopicCount + 3;

    private final String latestReleasedOperator = "https://github.com/strimzi/strimzi-kafka-operator/releases/download/0.19.0/strimzi-0.19.0.zip";


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
        } catch (KubeClusterException e) {
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
            for (JsonValue testParameters : parameters) {
                if (StUtils.isAllowOnCurrentEnvironment(testParameters.asJsonObject().getJsonObject("environmentInfo").getString("flakyEnvVariable")) &&
                    StUtils.isAllowedOnCurrentK8sVersion(testParameters.asJsonObject().getJsonObject("environmentInfo").getString("maxK8sVersion"))) {
                    performUpgrade(testParameters.asJsonObject(), MESSAGE_COUNT, consumedMessagesCount);
                    consumedMessagesCount = consumedMessagesCount + MESSAGE_COUNT;
                } else {
                    LOGGER.info("Upgrade of Cluster Operator from version {} to version {} is not allowed on this K8S version!", testParameters.asJsonObject().getString("fromVersion"), testParameters.asJsonObject().getString("toVersion"));
                }
            }
        } catch (KubeClusterException e) {
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
        File dir = FileUtils.downloadAndUnzip(latestReleasedOperator);

        coDir = new File(dir, "strimzi-0.19.0/install/cluster-operator/");

        // Modify + apply installation files
        copyModifyApply(coDir);

        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3, 3)
            .editSpec()
                .editKafka()
                    .withVersion(null)
                    .addToConfig("log.message.format.version", "2.5")
                .endKafka()
            .endSpec().done();

        Map<String, String> operatorSnapshot = DeploymentUtils.depSnapshot(ResourceManager.getCoDeploymentName());
        Map<String, String> kafkaSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        Map<String, String> eoSnapshot = DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME));

        // Update CRDs, CRB, etc.
        cluster.applyClusterOperatorInstallFiles();
        applyRoleBindings(NAMESPACE);

        kubeClient().getClient().apps().deployments().inNamespace(NAMESPACE).withName(ResourceManager.getCoDeploymentName()).delete();
        kubeClient().getClient().apps().deployments().inNamespace(NAMESPACE).withName(ResourceManager.getCoDeploymentName()).create(BundleResource.defaultClusterOperator(NAMESPACE).build());

        DeploymentUtils.waitTillDepHasRolled(ResourceManager.getCoDeploymentName(), 1, operatorSnapshot);
        StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1, eoSnapshot);

        assertThat(kubeClient().getStatefulSet(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME)).getSpec().getTemplate().getSpec().getContainers()
                .stream().filter(c -> c.getName().equals("kafka")).findFirst().get().getImage(), containsString("2.6.0"));
    }

    @SuppressWarnings("MethodLength")
    private void performUpgrade(JsonObject testParameters, int produceMessagesCount, int consumeMessagesCount) throws IOException {
        String continuousTopicName = "continuous-topic";
        int continuousClientsMessageCount = testParameters.getJsonObject("client").getInt("continuousClientsMessages");
        String producerName = "hello-world-producer";
        String consumerName = "hello-world-consumer";
        String continuousConsumerGroup = "continuous-consumer-group";

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
            cmdKubeClient().create(kafkaYaml);
            // Wait for readiness
            LOGGER.info("Waiting for Zookeeper StatefulSet");
            StatefulSetUtils.waitForAllStatefulSetPodsReady(CLUSTER_NAME + "-zookeeper", 3);
            LOGGER.info("Waiting for Kafka StatefulSet");
            StatefulSetUtils.waitForAllStatefulSetPodsReady(CLUSTER_NAME + "-kafka", 3);
            LOGGER.info("Waiting for EO Deployment");
            DeploymentUtils.waitForDeploymentAndPodsReady(CLUSTER_NAME + "-entity-operator", 1);
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
                KafkaTopicResource.topicWithoutWait(KafkaTopicResource.defaultTopic(CLUSTER_NAME, topicName + "-" + x, 1, 1, 1)
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
                KafkaTopicResource.topic(CLUSTER_NAME, continuousTopicName, 3, 3, 2).done();
            }

            String producerAdditionConfiguration = "delivery.timeout.ms=20000\nrequest.timeout.ms=20000";
            KafkaClientsResource.producerStrimzi(producerName, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), continuousTopicName, continuousClientsMessageCount, producerAdditionConfiguration).done();
            KafkaClientsResource.consumerStrimzi(consumerName, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), continuousTopicName, continuousClientsMessageCount, "", continuousConsumerGroup).done();
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
            .build();

        int sent = internalKafkaClient.sendMessagesTls();
        assertThat(sent, is(produceMessagesCount));

        internalKafkaClient.setMessageCount(consumeMessagesCount);

        int received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(consumeMessagesCount));

        makeSnapshots();
        logPodImages();
        // Execution of required procedures before upgrading CO
        changeKafkaAndLogFormatVersion(testParameters.getJsonObject("proceduresBefore"));

        // Upgrade the CO
        // Modify + apply installation files
        LOGGER.info("Going to update CO from {} to {}", testParameters.getString("fromVersion"), testParameters.getString("toVersion"));
        if ("HEAD".equals(testParameters.getString("toVersion"))) {
            coDir = new File(TestUtils.USER_PATH + "/../install/cluster-operator");
        } else {
            url = testParameters.getString("urlTo");
            dir = FileUtils.downloadAndUnzip(url);
            coDir = new File(dir, testParameters.getString("toExamples") + "/install/cluster-operator/");
        }
        upgradeClusterOperator(coDir, testParameters.getJsonObject("imagesBeforeKafkaUpdate"));

        // Make snapshots of all pods
        makeSnapshots();
        logPodImages();
        //  Upgrade kafka
        changeKafkaAndLogFormatVersion(testParameters.getJsonObject("proceduresAfter"));
        logPodImages();
        checkAllImages(testParameters.getJsonObject("imagesAfterKafkaUpdate"));

        // Delete old clients
        kubeClient().deleteDeployment(kafkaClusterName + "-" + Constants.KAFKA_CLIENTS);
        DeploymentUtils.waitForDeploymentDeletion(kafkaClusterName + "-" + Constants.KAFKA_CLIENTS);

        deployClients(testParameters.getJsonObject("client").getString("afterKafkaUpdate"), kafkaUser);

        final String afterUpgradeKafkaClientsPodName =
                kubeClient().listPodsByPrefixInName(kafkaClusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        internalKafkaClient.setPodName(afterUpgradeKafkaClientsPodName);
        internalKafkaClient.setConsumerGroup(ClientUtils.generateRandomConsumerGroup());

        received = internalKafkaClient.receiveMessagesTls();
        assertThat(received, is(consumeMessagesCount));

        if (testParameters.getBoolean("generateTopics")) {
            // Check that topics weren't deleted/duplicated during upgrade procedures
            assertThat("KafkaTopic list doesn't have expected size", KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).list().getItems().size(), is(expectedTopicCount));
            List<KafkaTopic> kafkaTopicList = KafkaTopicResource.kafkaTopicClient().inNamespace(NAMESPACE).list().getItems();
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
            kubeClient().getClient().batch().jobs().inNamespace(NAMESPACE).withName(producerName).delete();
            kubeClient().getClient().batch().jobs().inNamespace(NAMESPACE).withName(consumerName).delete();
        }

        // Check errors in CO log
        assertNoCoErrorsLogged(0);
    }

    private void upgradeClusterOperator(File coInstallDir, JsonObject images) {
        copyModifyApply(coInstallDir);

        LOGGER.info("Waiting for CO upgrade");
        DeploymentUtils.waitTillDepHasRolled(ResourceManager.getCoDeploymentName(), 1, coPods);
        LOGGER.info("Waiting for ZK StatefulSet roll");
        StatefulSetUtils.waitTillSsHasRolled(zkStsName, 3, zkPods);
        LOGGER.info("Waiting for Kafka StatefulSet roll");
        StatefulSetUtils.waitTillSsHasRolled(kafkaStsName, 3, kafkaPods);
        LOGGER.info("Waiting for EO Deployment roll");
        // Check the TO and UO also got upgraded
        DeploymentUtils.waitTillDepHasRolled(eoDepName, 1, eoPods);
        checkAllImages(images);
    }

    private void copyModifyApply(File root) {
        Arrays.stream(Objects.requireNonNull(root.listFiles())).sorted().forEach(f -> {
            if (f.getName().matches(".*RoleBinding.*")) {
                cmdKubeClient().applyContent(TestUtils.changeRoleBindingSubject(f, NAMESPACE));
            } else if (f.getName().matches(".*Deployment.*")) {
                cmdKubeClient().applyContent(TestUtils.changeDeploymentNamespaceUpgrade(f, NAMESPACE));
            } else {
                cmdKubeClient().apply(f);
            }
        });
    }

    private void deleteInstalledYamls(File root) {
        if (kafkaUserYaml != null) {
            cmdKubeClient().delete(kafkaUserYaml);
        }
        if (kafkaTopicYaml != null) {
            cmdKubeClient().delete(kafkaTopicYaml);
        }
        if (kafkaYaml != null) {
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

    private void makeSnapshots() {
        coPods = DeploymentUtils.depSnapshot(ResourceManager.getCoDeploymentName());
        zkPods = StatefulSetUtils.ssSnapshot(zkStsName);
        kafkaPods = StatefulSetUtils.ssSnapshot(kafkaStsName);
        eoPods = DeploymentUtils.depSnapshot(eoDepName);
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
                LOGGER.debug("Expected image for pod {}: {} \nCurrent image: {}", pod.getMetadata().getName(), image, pod.getSpec().getContainers().get(container).getImage());
                assertThat("Used image for pod " + pod.getMetadata().getName() + " is not valid!", pod.getSpec().getContainers().get(container).getImage(), containsString(image));
            }
        }
    }

    private void changeKafkaAndLogFormatVersion(JsonObject procedures) {
        if (!procedures.isEmpty()) {
            String kafkaVersion = procedures.getString("kafkaVersion");
            if (!kafkaVersion.isEmpty()) {
                LOGGER.info("Going to set Kafka version to " + kafkaVersion);
                KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getKafka().setVersion(kafkaVersion));
                LOGGER.info("Wait until kafka rolling update is finished");
                if (!kafkaVersion.equals("2.0.0")) {
                    StatefulSetUtils.waitTillSsHasRolled(kafkaStsName, 3, kafkaPods);
                }
                makeSnapshots();
            }

            String logMessageVersion = procedures.getString("logMessageVersion");
            if (!logMessageVersion.isEmpty()) {
                LOGGER.info("Going to set log message format version to " + logMessageVersion);
                KafkaResource.replaceKafkaResource(CLUSTER_NAME, k -> k.getSpec().getKafka().getConfig().put("log.message.format.version", logMessageVersion));
                LOGGER.info("Wait until kafka rolling update is finished");
                StatefulSetUtils.waitTillSsHasRolled(kafkaStsName, 3, kafkaPods);
                makeSnapshots();
            }
        }
    }

    void logPodImages() {
        List<Pod> pods = kubeClient().listPods(kubeClient().getStatefulSetSelectors(zkStsName));
        for (Pod pod : pods) {
            LOGGER.info("Pod {} has image {}", pod.getMetadata().getName(), pod.getSpec().getContainers().get(0).getImage());
        }
        pods = kubeClient().listPods(kubeClient().getStatefulSetSelectors(kafkaStsName));
        for (Pod pod : pods) {
            LOGGER.info("Pod {} has image {}", pod.getMetadata().getName(), pod.getSpec().getContainers().get(0).getImage());
        }
        pods = kubeClient().listPods(kubeClient().getDeploymentSelectors(eoDepName));
        for (Pod pod : pods) {
            LOGGER.info("Pod {} has image {}", pod.getMetadata().getName(), pod.getSpec().getContainers().get(0).getImage());
            LOGGER.info("Pod {} has image {}", pod.getMetadata().getName(), pod.getSpec().getContainers().get(1).getImage());
        }
    }

    void deployClients(String image, KafkaUser kafkaUser) {
        // Deploy new clients
        KafkaClientsResource.deployKafkaClients(true, CLUSTER_NAME + "-" + Constants.KAFKA_CLIENTS, kafkaUser)
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .editFirstContainer()
                            .withImage(image)
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec().done();
    }

    private static Stream<Arguments> loadJsonUpgradeData() throws FileNotFoundException {
        JsonArray upgradeData = readUpgradeJson();
        List<Arguments> parameters = new LinkedList<>();

        upgradeData.forEach(jsonData -> {
            JsonObject data = (JsonObject) jsonData;
            parameters.add(Arguments.of(data.getString("fromVersion"), data.getString("toVersion"), data));
        });

        return parameters.stream();
    }

    private static JsonArray readUpgradeJson() throws FileNotFoundException {
        InputStream fis = new FileInputStream(TestUtils.USER_PATH + "/src/main/resources/StrimziUpgradeST.json");
        JsonReader reader = Json.createReader(fis);
        return reader.readArray();
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
