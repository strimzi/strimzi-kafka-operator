/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.skodjob.testframe.resources.ResourceItem;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.connect.build.JarArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.Plugin;
import io.strimzi.api.kafka.model.connect.build.PluginBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.labels.LabelSelectors;
import io.strimzi.systemtest.resources.CrdClients;
import io.strimzi.systemtest.resources.NamespaceManager;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfiguration;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.crds.CrdUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.ReadWriteUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.stream.IntStream;

import static io.strimzi.systemtest.Environment.TEST_SUITE_NAMESPACE;
import static io.strimzi.systemtest.TestConstants.CO_NAMESPACE;
import static io.strimzi.systemtest.TestConstants.DEFAULT_SINK_FILE_PATH;
import static io.strimzi.systemtest.TestConstants.PATH_TO_KAFKA_TOPIC_CONFIG;
import static io.strimzi.systemtest.TestConstants.PATH_TO_PACKAGING;
import static io.strimzi.systemtest.TestConstants.PATH_TO_PACKAGING_EXAMPLES;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.fail;

public class AbstractKRaftUpgradeST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(AbstractKRaftUpgradeST.class);

    protected File dir = null;
    protected File coDir = null;
    protected File kafkaTopicYaml = null;
    protected File kafkaUserYaml = null;
    protected File kafkaConnectYaml;
    protected File kafkaYaml;

    protected Map<String, String> brokerPods;
    protected Map<String, String> controllerPods;
    protected Map<String, String> eoPods;
    protected Map<String, String> connectPods;

    protected static final String CONTROLLER_NODE_NAME = "controller";
    protected static final String BROKER_NODE_NAME = "broker";
    protected static final String CLUSTER_NAME = "my-cluster";
    protected static final String TOPIC_NAME = "my-topic";
    protected static final String USER_NAME = "my-user";
    protected static final int UPGRADE_TOPIC_COUNT = 20;
    protected static final int BTO_KAFKA_TOPICS_ONLY_COUNT = 3;

    protected final LabelSelector controllerSelector = LabelSelectors.nodePoolLabelSelector(CLUSTER_NAME, CONTROLLER_NODE_NAME, ProcessRoles.CONTROLLER);
    protected final LabelSelector brokerSelector = LabelSelectors.nodePoolLabelSelector(CLUSTER_NAME, BROKER_NODE_NAME, ProcessRoles.BROKER);
    protected final LabelSelector eoSelector = LabelSelectors.entityOperatorLabelSelector(CLUSTER_NAME);
    protected final LabelSelector coSelector = new LabelSelectorBuilder().withMatchLabels(Map.of(Labels.STRIMZI_KIND_LABEL, "cluster-operator")).build();
    protected final LabelSelector connectLabelSelector = LabelSelectors.connectLabelSelector(CLUSTER_NAME, KafkaConnectResources.componentName(CLUSTER_NAME));

    // We want to keep the default configuration (as configured via the env variables)
    protected final ClusterOperatorConfiguration clusterOperatorConfiguration = new ClusterOperatorConfiguration();

    protected void makeComponentsSnapshots(String componentsNamespaceName) {
        eoPods = DeploymentUtils.depSnapshot(componentsNamespaceName, KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME));
        controllerPods = PodUtils.podSnapshot(componentsNamespaceName, controllerSelector);
        brokerPods = PodUtils.podSnapshot(componentsNamespaceName, brokerSelector);
        connectPods = PodUtils.podSnapshot(componentsNamespaceName, connectLabelSelector);
    }

    /**
     * Performs the Kafka Connect and Kafka Connector upgrade/downgrade procedure.
     * It upgrades the Cluster Operator, Kafka Connect, and Kafka Connector while verifying each step.
     *
     * @param clusterOperatorNamespaceName Namespace of the Cluster Operator
     * @param testStorage                  Test-related configuration and storage
     * @param upgradeDowngradeData         Bundle version modification data
     * @param upgradeKafkaVersion          Kafka version details
     * @throws IOException if any I/O error occurs during the procedure
     */
    protected void doKafkaConnectAndKafkaConnectorUpgradeOrDowngradeProcedure(
        final String clusterOperatorNamespaceName,
        final TestStorage testStorage,
        final BundleVersionModificationData upgradeDowngradeData,
        final UpgradeKafkaVersion upgradeKafkaVersion) throws IOException {
        // 1. Setup Cluster Operator with KafkaConnect and KafkaConnector
        setupEnvAndUpgradeClusterOperator(clusterOperatorNamespaceName, testStorage, upgradeDowngradeData, upgradeKafkaVersion);
        deployKafkaConnectAndKafkaConnectorWithWaitForReadiness(testStorage, upgradeDowngradeData, upgradeKafkaVersion);

        // 2. Send messages
        produceMessagesAndVerify(testStorage);

        // 3. Make snapshots
        makeComponentsSnapshots(testStorage.getNamespaceName());
        logComponentsPodImagesWithConnect(testStorage.getNamespaceName());

        // 4. Verify KafkaConnector FileSink
        verifyKafkaConnectorFileSink(testStorage);

        // 5. Upgrade CO to HEAD and wait for readiness of ClusterOperator
        changeClusterOperator(clusterOperatorNamespaceName, testStorage.getNamespaceName(), upgradeDowngradeData);

        // 6. Wait for components to roll
        maybeWaitForRollingUpdate(testStorage.getNamespaceName(), upgradeKafkaVersion);
        logComponentsPodImagesWithConnect(testStorage.getNamespaceName());

        verifyPostUpgradeOrDowngradeProcedure(testStorage, upgradeDowngradeData);
    }

    /**
     * Verifies the environment after an upgrade or downgrade procedure
     * by sending new messages, checking connector output, stability, and final state.
     *
     * @param testStorage          Test-related configuration and storage
     * @param upgradeDowngradeData Bundle version modification data
     */
    private void verifyPostUpgradeOrDowngradeProcedure(final TestStorage testStorage,
                                                       final BundleVersionModificationData upgradeDowngradeData) {
        final KafkaClients clients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(CLUSTER_NAME))
            .withUsername(USER_NAME)
            .build();
        // send again new messages
        KubeResourceManager.get().createResourceWithWait(clients.producerTlsStrimzi(CLUSTER_NAME));

        // Verify that Producer finish successfully
        ClientUtils.waitForInstantProducerClientSuccess(testStorage.getNamespaceName(), testStorage);

        // Verify FileSink KafkaConnector
        verifyKafkaConnectorFileSink(testStorage);

        // Verify that pods are stable
        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), CLUSTER_NAME);

        // Verify upgrade
        verifyProcedure(testStorage.getNamespaceName(), upgradeDowngradeData, testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName());
    }

    /**
     * Verifies that the Kafka Connector FileSink is receiving messages as expected.
     *
     * @param testStorage Test-related configuration and storage
     */
    private void verifyKafkaConnectorFileSink(final TestStorage testStorage) {
        String connectorPodName = PodUtils.listPodNames(testStorage.getNamespaceName(),
            LabelSelectors.connectLabelSelector(CLUSTER_NAME, KafkaConnectResources.componentName(CLUSTER_NAME))).get(0);

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(
            testStorage.getNamespaceName(),
            connectorPodName,
            DEFAULT_SINK_FILE_PATH,
            testStorage.getMessageCount()
        );
    }

    /**
     * Waits for the Kafka cluster and Kafka Connect to roll if the target version is supported.
     *
     * @param namespaceName         name of the namespace
     * @param upgradeKafkaVersion   Kafka version details
     */
    private void maybeWaitForRollingUpdate(final String namespaceName,
                                           final UpgradeKafkaVersion upgradeKafkaVersion) {
        if (TestKafkaVersion.supportedVersionsContainsVersion(upgradeKafkaVersion.getVersion())) {
            waitForKafkaClusterRollingUpdate(namespaceName);
            connectPods = RollingUpdateUtils.waitTillComponentHasRolled(
                namespaceName,
                connectLabelSelector,
                1,
                connectPods
            );
            KafkaConnectorUtils.waitForConnectorReady(namespaceName, CLUSTER_NAME);
        }
    }

    /**
     * Produces messages and verifies they were successfully sent, using a TLS client.
     *
     * @param testStorage Test-related configuration and storage
     */
    private void produceMessagesAndVerify(TestStorage testStorage) {
        final KafkaClients clients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(CLUSTER_NAME))
            .withNamespaceName(testStorage.getNamespaceName())
            .withUsername(USER_NAME)
            .build();

        KubeResourceManager.get().createResourceWithWait(clients.producerTlsStrimzi(CLUSTER_NAME));
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);
    }

    protected void setupEnvAndUpgradeClusterOperator(String clusterOperatorNamespaceName, TestStorage testStorage, BundleVersionModificationData upgradeData, UpgradeKafkaVersion upgradeKafkaVersion) throws IOException {
        LOGGER.info("Test upgrade of Cluster Operator from version: {} to version: {}", upgradeData.getFromVersion(), upgradeData.getToVersion());

        this.deployCoWithWaitForReadiness(clusterOperatorNamespaceName, testStorage.getNamespaceName(), upgradeData);
        this.deployKafkaClusterWithWaitForReadiness(testStorage.getNamespaceName(), upgradeData, upgradeKafkaVersion);
        this.deployKafkaUserWithWaitForReadiness(testStorage.getNamespaceName(), upgradeData);
        this.deployKafkaTopicWithWaitForReadiness(testStorage.getNamespaceName(), upgradeData);

        // Create bunch of topics for upgrade if it's specified in configuration
        if (upgradeData.getAdditionalTopics() != null && upgradeData.getAdditionalTopics() > 0) {
            if (upgradeData.getFromVersion().equals("HEAD")) {
                kafkaTopicYaml = new File(dir, PATH_TO_PACKAGING_EXAMPLES + "/topic/kafka-topic.yaml");
            } else {
                kafkaTopicYaml = new File(dir, upgradeData.getFromExamples() + "/examples/topic/kafka-topic.yaml");
            }

            String topicNameTemplate = TOPIC_NAME + "-%s";
            IntStream.range(0, UPGRADE_TOPIC_COUNT)
                .mapToObj(topicNameTemplate::formatted)
                .map(this::getKafkaYamlWithName)
                .parallel()
                .forEach(KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName())::applyContent);
        }

        if (upgradeData.getContinuousClientsMessages() != 0) {
            // ##############################
            // Attach clients which will continuously produce/consume messages to/from Kafka brokers during rolling update
            // ##############################
            // Setup topic, which has 3 replicas and 2 min.isr to see if producer will be able to work during rolling update
            if (!KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).getResourcesAsYaml(getResourceApiVersion(KafkaTopic.RESOURCE_PLURAL)).contains(testStorage.getTopicName())) {
                String pathToTopicExamples = upgradeData.getFromExamples().equals("HEAD") ? PATH_TO_KAFKA_TOPIC_CONFIG : upgradeData.getFromExamples() + "/examples/topic/kafka-topic.yaml";

                kafkaTopicYaml = new File(dir, pathToTopicExamples);
                KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).applyContent(ReadWriteUtils.readFile(kafkaTopicYaml)
                    .replace("name: my-topic", "name: " + testStorage.getTopicName())
                    .replace("partitions: 1", "partitions: 3")
                    .replace("replicas: 1", "replicas: 3") +
                    "    min.insync.replicas: 2");

                KafkaTopicUtils.waitForKafkaTopicReady(testStorage.getNamespaceName(), testStorage.getTopicName());
            }

            // 40s is used within TF environment to make upgrade/downgrade more stable on slow env
            String producerAdditionConfiguration = "delivery.timeout.ms=40000\nrequest.timeout.ms=5000";

            KafkaClients kafkaBasicClientJob = ClientUtils.getContinuousPlainClientBuilder(testStorage)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
                .withMessageCount(upgradeData.getContinuousClientsMessages())
                .withAdditionalConfig(producerAdditionConfiguration)
                .withNamespaceName(testStorage.getNamespaceName())
                .build();

            KubeResourceManager.get().createResourceWithWait(
                kafkaBasicClientJob.producerStrimzi(),
                kafkaBasicClientJob.consumerStrimzi()
            );
            // ##############################
        }

        makeComponentsSnapshots(testStorage.getNamespaceName());
    }

    protected void deployCoWithWaitForReadiness(final String clusterOperatorNamespaceName, final String componentsNamespaceName, final BundleVersionModificationData upgradeData) throws IOException {
        LOGGER.info("Deploying CO: {} in Namespace: {}", clusterOperatorConfiguration.getOperatorDeploymentName(), clusterOperatorNamespaceName);

        if (upgradeData.getFromVersion().equals("HEAD")) {
            coDir = new File(TestUtils.USER_PATH + "/../packaging/install/cluster-operator");
        } else {
            final String url = upgradeData.getFromUrl();
            dir = FileUtils.downloadAndUnzip(url);
            coDir = new File(dir, upgradeData.getFromExamples() + "/install/cluster-operator/");
        }

        // Modify + apply installation files
        modifyApplyClusterOperatorWithCRDsFromFile(true, clusterOperatorNamespaceName, componentsNamespaceName, coDir, upgradeData.getFeatureGatesBefore());

        LOGGER.info("Waiting for Cluster Operator Deployment: {}", clusterOperatorConfiguration.getOperatorDeploymentName());
        DeploymentUtils.waitForDeploymentAndPodsReady(clusterOperatorNamespaceName, clusterOperatorConfiguration.getOperatorDeploymentName(), 1);
        LOGGER.info("{} is ready", clusterOperatorConfiguration.getOperatorDeploymentName());
    }

    protected void deployKafkaClusterWithWaitForReadiness(final String componentsNamespaceName,
                                                          final BundleVersionModificationData upgradeData,
                                                          final UpgradeKafkaVersion upgradeKafkaVersion) {
        LOGGER.info("Deploying Kafka: {}/{}", componentsNamespaceName, CLUSTER_NAME);

        if (!KubeResourceManager.get().kubeCmdClient().inNamespace(componentsNamespaceName).getResourcesAsYaml(getResourceApiVersion(Kafka.RESOURCE_PLURAL)).contains(CLUSTER_NAME)) {
            // Deploy a Kafka cluster
            if (upgradeData.getFromExamples().equals("HEAD")) {
                KubeResourceManager.get().createResourceWithWait(
                    KafkaNodePoolTemplates.controllerPoolPersistentStorage(componentsNamespaceName, CONTROLLER_NODE_NAME, CLUSTER_NAME, 3).build(),
                    KafkaNodePoolTemplates.brokerPoolPersistentStorage(componentsNamespaceName, BROKER_NODE_NAME, CLUSTER_NAME, 3).build(),
                    KafkaTemplates.kafka(componentsNamespaceName, CLUSTER_NAME, 3)
                        .editMetadata()
                            .addToAnnotations(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled")
                            .addToAnnotations(Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled")
                        .endMetadata()
                        .editSpec()
                            .editKafka()
                                .withVersion(upgradeKafkaVersion.getVersion())
                                .withMetadataVersion(upgradeKafkaVersion.getMetadataVersion())
                            .endKafka()
                        .endSpec()
                        .build());
            } else {
                kafkaYaml = new File(dir, upgradeData.getFromExamples() + upgradeData.getKafkaFilePathBefore());
                LOGGER.info("Deploying Kafka from: {}", kafkaYaml.getPath());
                // Change kafka version of it's empty (null is for remove the version)
                if (upgradeKafkaVersion == null) {
                    KubeResourceManager.get().kubeCmdClient().inNamespace(componentsNamespaceName).applyContent(KafkaUtils.changeOrRemoveKafkaInKRaft(kafkaYaml, null));
                } else {
                    KubeResourceManager.get().kubeCmdClient().inNamespace(componentsNamespaceName).applyContent(KafkaUtils.changeOrRemoveKafkaConfigurationInKRaft(kafkaYaml, upgradeKafkaVersion.getVersion(), upgradeKafkaVersion.getMetadataVersion()));
                }
                // Wait for readiness
                waitForReadinessOfKafkaCluster(componentsNamespaceName);
            }
        }
    }

    protected void deployKafkaUserWithWaitForReadiness(final String componentsNamespaceName, final BundleVersionModificationData upgradeData) {
        LOGGER.info("Deploying KafkaUser: {}/{}", componentsNamespaceName, USER_NAME);

        if (!KubeResourceManager.get().kubeCmdClient().inNamespace(componentsNamespaceName).getResourcesAsYaml(getResourceApiVersion(KafkaUser.RESOURCE_PLURAL)).contains(USER_NAME)) {
            if (upgradeData.getFromVersion().equals("HEAD")) {
                KubeResourceManager.get().createResourceWithWait(KafkaUserTemplates.tlsUser(componentsNamespaceName, USER_NAME, CLUSTER_NAME).build());
            } else {
                kafkaUserYaml = new File(dir, upgradeData.getFromExamples() + "/examples/user/kafka-user.yaml");
                LOGGER.info("Deploying KafkaUser from: {}", kafkaUserYaml.getPath());
                KubeResourceManager.get().kubeCmdClient().inNamespace(componentsNamespaceName).applyContent(KafkaUserUtils.removeKafkaUserPart(kafkaUserYaml, "authorization"));
                KafkaUserUtils.waitForKafkaUserReady(componentsNamespaceName, USER_NAME);
            }
        }
    }

    protected void deployKafkaTopicWithWaitForReadiness(final String componentsNamespaceName, final BundleVersionModificationData upgradeData) {
        LOGGER.info("Deploying KafkaTopic: {}/{}", componentsNamespaceName, TOPIC_NAME);

        if (!KubeResourceManager.get().kubeCmdClient().inNamespace(componentsNamespaceName).getResourcesAsYaml(getResourceApiVersion(KafkaTopic.RESOURCE_PLURAL)).contains(TOPIC_NAME)) {
            if (upgradeData.getFromVersion().equals("HEAD")) {
                kafkaTopicYaml = new File(dir, PATH_TO_PACKAGING_EXAMPLES + "/topic/kafka-topic.yaml");
            } else {
                kafkaTopicYaml = new File(dir, upgradeData.getFromExamples() + "/examples/topic/kafka-topic.yaml");
            }
            LOGGER.info("Deploying KafkaTopic from: {}", kafkaTopicYaml.getPath());
            KubeResourceManager.get().kubeCmdClient().inNamespace(componentsNamespaceName).create(kafkaTopicYaml);
            KafkaTopicUtils.waitForKafkaTopicReady(componentsNamespaceName, TOPIC_NAME);
        }
    }

    protected void deployKafkaConnectAndKafkaConnectorWithWaitForReadiness(
        final TestStorage testStorage,
        final BundleVersionModificationData acrossUpgradeData,
        final UpgradeKafkaVersion upgradeKafkaVersion
    ) {
        // setup KafkaConnect + KafkaConnector
        if (!KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).getResourcesAsYaml(getResourceApiVersion(KafkaConnect.RESOURCE_PLURAL)).contains(CLUSTER_NAME)) {
            if (acrossUpgradeData.getFromVersion().equals("HEAD")) {
                KubeResourceManager.get().createResourceWithWait(KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getNamespaceName(), CLUSTER_NAME, 1)
                    .editMetadata()
                        .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                    .endMetadata()
                    .editSpec()
                        .addToConfig("key.converter.schemas.enable", false)
                        .addToConfig("value.converter.schemas.enable", false)
                        .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                        .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                        .withVersion(upgradeKafkaVersion.getVersion())
                    .endSpec()
                    .build());
                KubeResourceManager.get().createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(), CLUSTER_NAME)
                    .editSpec()
                        .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                        .addToConfig("topics", testStorage.getTopicName())
                        .addToConfig("file", TestConstants.DEFAULT_SINK_FILE_PATH)
                    .endSpec()
                    .build());
            } else {
                kafkaConnectYaml = new File(dir, acrossUpgradeData.getFromExamples() + "/examples/connect/kafka-connect.yaml");

                final Plugin fileSinkPlugin = new PluginBuilder()
                    .withName("file-plugin")
                    .withArtifacts(
                        new JarArtifactBuilder()
                            .withUrl(Environment.ST_FILE_PLUGIN_URL)
                            .build()
                    )
                    .build();

                final String imageFullPath = Environment.getImageOutputRegistry(testStorage.getNamespaceName(), TestConstants.ST_CONNECT_BUILD_IMAGE_NAME, String.valueOf(new Random().nextInt(Integer.MAX_VALUE)));

                LOGGER.info("Deploying KafkaConnect from: {}", kafkaConnectYaml.getPath());

                KafkaConnect kafkaConnect = new KafkaConnectBuilder(ReadWriteUtils.readObjectFromYamlFilepath(kafkaConnectYaml, KafkaConnect.class))
                    .editMetadata()
                        .withName(CLUSTER_NAME)
                        .withNamespace(testStorage.getNamespaceName())
                        .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                    .endMetadata()
                    .editSpec()
                        .editOrNewBuild()
                            .withPlugins(fileSinkPlugin)
                            .withOutput(KafkaConnectTemplates.dockerOutput(imageFullPath))
                        .endBuild()
                        .addToConfig("key.converter.schemas.enable", false)
                        .addToConfig("value.converter.schemas.enable", false)
                        .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                        .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                        .withVersion(upgradeKafkaVersion.getVersion())
                    .endSpec()
                    .build();

                KubeResourceManager.get().createResourceWithWait(kafkaConnect);

                // in our examples is no sink connector and thus we are using the same as in HEAD verification
                KubeResourceManager.get().createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(), CLUSTER_NAME)
                    .editMetadata()
                        .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, kafkaConnect.getMetadata().getName())
                    .endMetadata()
                    .editSpec()
                        .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                        .addToConfig("topics", testStorage.getTopicName())
                        .addToConfig("file", DEFAULT_SINK_FILE_PATH)
                    .endSpec()
                    .build());
            }
        }
    }

    protected void waitForKafkaClusterRollingUpdate(final String componentsNamespaceName) {
        LOGGER.info("Waiting for Kafka Pods with controller role to be rolled");
        controllerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(componentsNamespaceName, controllerSelector, 3, controllerPods);
        LOGGER.info("Waiting for Kafka Pods with broker role to be rolled");
        brokerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(componentsNamespaceName, brokerSelector, 3, brokerPods);
        LOGGER.info("Waiting for EO Deployment to be rolled");
        // Check the TO and UO also got upgraded
        eoPods = DeploymentUtils.waitTillDepHasRolled(componentsNamespaceName, KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1, eoPods);
    }

    protected void waitForReadinessOfKafkaCluster(final String componentsNamespaceName) {
        LOGGER.info("Waiting for Kafka Pods with controller role to be ready");
        RollingUpdateUtils.waitForComponentAndPodsReady(componentsNamespaceName, controllerSelector, 3);
        LOGGER.info("Waiting for Kafka Pods with broker role to be ready");
        RollingUpdateUtils.waitForComponentAndPodsReady(componentsNamespaceName, brokerSelector, 3);
        LOGGER.info("Waiting for EO Deployment");
        DeploymentUtils.waitForDeploymentAndPodsReady(componentsNamespaceName, KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1);
    }

    protected void changeClusterOperator(String clusterOperatorNamespaceName, String componentsNamespaceName, BundleVersionModificationData versionModificationData) throws IOException {
        final Map<String, String> coPods = DeploymentUtils.depSnapshot(clusterOperatorNamespaceName, clusterOperatorConfiguration.getOperatorDeploymentName());

        File coDir;
        // Modify + apply installation files
        LOGGER.info("Update CO from {} to {}", versionModificationData.getFromVersion(), versionModificationData.getToVersion());
        if (versionModificationData.getToVersion().equals("HEAD")) {
            coDir = new File(TestUtils.USER_PATH + "/../packaging/install/cluster-operator");
        } else {
            String url = versionModificationData.getToUrl();
            File dir = FileUtils.downloadAndUnzip(url);
            coDir = new File(dir, versionModificationData.getToExamples() + "/install/cluster-operator/");
        }

        modifyApplyClusterOperatorWithCRDsFromFile(false, clusterOperatorNamespaceName, componentsNamespaceName, coDir, versionModificationData.getFeatureGatesAfter());

        LOGGER.info("Waiting for CO upgrade");
        DeploymentUtils.waitTillDepHasRolled(clusterOperatorNamespaceName, clusterOperatorConfiguration.getOperatorDeploymentName(), 1, coPods);
    }

    /**
     * Series of steps done when applying operator from files located in root directory. Operator deployment is modified
     * to watch multiple (single) namespace. All role based access control resources are modified so the subject is found
     * in operator namespace. Role bindings concerning operands are modified to be deployed in watched namespace.
     *
     * @param applyContent                   boolean value determining, if the content should be applied or replaced.
     * @param clusterOperatorNamespaceName   the name of the namespace where the Strimzi operator is deployed.
     * @param componentsNamespaceName        the name of the single namespace being watched and managed by the Strimzi operator.
     * @param root                           the root directory containing the YAML files to be processed.
     * @param strimziFeatureGatesValue       the value of the Strimzi feature gates to be injected into deployment configurations.
     */
    protected void modifyApplyClusterOperatorWithCRDsFromFile(
        boolean applyContent,
        String clusterOperatorNamespaceName,
        String componentsNamespaceName,
        File root,
        final String strimziFeatureGatesValue
    ) {
        KubeClusterResource.getInstance().setNamespace(clusterOperatorNamespaceName);

        final List<String> watchedNsRoleBindingFilePrefixes = List.of(
            "020-RoleBinding",  // rb to role for creating KNative resources
            "023-RoleBinding",  // rb to role for watching Strimzi CustomResources
            "031-RoleBinding"   // rb to role for entity operator
        );

        Arrays.stream(Objects.requireNonNull(root.listFiles())).sorted().forEach(f -> {
            String content;
            String namespaceName = clusterOperatorNamespaceName;

            if (watchedNsRoleBindingFilePrefixes.stream().anyMatch((rbFilePrefix) -> f.getName().startsWith(rbFilePrefix))) {
                content = StUtils.changeRoleBindingSubject(f, clusterOperatorNamespaceName);
                namespaceName = componentsNamespaceName;
            } else if (f.getName().matches(".*RoleBinding.*")) {
                content = StUtils.changeRoleBindingSubject(f, clusterOperatorNamespaceName);
            } else if (f.getName().matches(".*Deployment.*")) {
                content = StUtils.changeDeploymentConfiguration(componentsNamespaceName, f, strimziFeatureGatesValue);
            } else {
                content = ReadWriteUtils.readFile(f);
            }

            // in case that we are doing first deployment of CO, we want to use `apply` operation
            if (applyContent) {
                KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).applyContent(content);
            } else {
                try {
                    // otherwise, we want to replace the content
                    KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).replaceContent(content);
                } catch (Exception e) {
                    // in case that the replace fails (because for example CRD of some resource was removed or added into new version etc.)
                    // we want to apply the content instead of replacing it
                    KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).applyContent(content);
                }
            }
        });
    }

    protected void changeKafkaVersion(final String componentsNamespaceName, CommonVersionModificationData versionModificationData) throws IOException {
        changeKafkaVersion(componentsNamespaceName, versionModificationData, false);
    }

    /**
     * Method for changing Kafka `version` and `metadataVersion` fields in Kafka CR based on the current scenario
     * @param versionModificationData data structure holding information about the desired steps/versions that should be applied
     * @param replaceEvenIfMissing current workaround for the situation when `metadataVersion` is not set in Kafka CR -> that's because previous version of operator
     *     doesn't contain this kind of field, so even if we set this field in the Kafka CR, it is removed by the operator
     *     this is needed for correct functionality of the `testUpgradeAcrossVersionsWithUnsupportedKafkaVersion` test
     * @throws IOException exception during application of YAML files
     */
    @SuppressWarnings("CyclomaticComplexity")
    protected void changeKafkaVersion(final String componentsNamespaceName, CommonVersionModificationData versionModificationData, boolean replaceEvenIfMissing) throws IOException {
        // Get Kafka version
        String kafkaVersionFromCR = CrdClients.kafkaClient().inNamespace(componentsNamespaceName).withName(CLUSTER_NAME).get().getSpec().getKafka().getVersion();
        // Get Kafka metadata version
        String currentMetadataVersion = CrdClients.kafkaClient().inNamespace(componentsNamespaceName).withName(CLUSTER_NAME).get().getSpec().getKafka().getMetadataVersion();

        String kafkaVersionFromProcedure = versionModificationData.getProcedures().getVersion();

        // #######################################################################
        // #################    Update CRs to latest version   ###################
        // #######################################################################
        String examplesPath = downloadExamplesAndGetPath(versionModificationData);
        String kafkaFilePath = examplesPath + versionModificationData.getKafkaFilePathAfter();

        applyCustomResourcesFromPath(componentsNamespaceName, examplesPath, kafkaFilePath, kafkaVersionFromCR, currentMetadataVersion);

        // #######################################################################

        if (versionModificationData.getProcedures() != null && (currentMetadataVersion != null || replaceEvenIfMissing)) {

            if (kafkaVersionFromProcedure != null && !kafkaVersionFromProcedure.isEmpty() && !kafkaVersionFromCR.contains(kafkaVersionFromProcedure)) {
                LOGGER.info("Set Kafka version to " + kafkaVersionFromProcedure);
                KafkaUtils.replace(componentsNamespaceName, CLUSTER_NAME, kafka -> kafka.getSpec().getKafka().setVersion(kafkaVersionFromProcedure));

                waitForKafkaControllersAndBrokersFinishRollingUpdate(componentsNamespaceName);
            }

            String metadataVersion = versionModificationData.getProcedures().getMetadataVersion();

            if (metadataVersion != null && !metadataVersion.isEmpty()) {
                LOGGER.info("Set metadata version to {} (current version is {})", metadataVersion, currentMetadataVersion);
                KafkaUtils.replace(componentsNamespaceName, CLUSTER_NAME, kafka -> kafka.getSpec().getKafka().setMetadataVersion(metadataVersion));

                makeComponentsSnapshots(componentsNamespaceName);
            }
        }
    }

    protected void checkAllComponentsImages(String componentsNamespaceName, BundleVersionModificationData versionModificationData) {
        if (versionModificationData.getImagesAfterOperations().isEmpty()) {
            fail("There are no expected images");
        }

        checkContainerImages(componentsNamespaceName, controllerSelector, versionModificationData.getKafkaImage());
        checkContainerImages(componentsNamespaceName, brokerSelector, versionModificationData.getKafkaImage());
        checkContainerImages(componentsNamespaceName, eoSelector, versionModificationData.getTopicOperatorImage());
        checkContainerImages(componentsNamespaceName, eoSelector, 1, versionModificationData.getUserOperatorImage());
    }

    protected void checkContainerImages(String namespaceName, LabelSelector labelSelector, String image) {
        checkContainerImages(namespaceName, labelSelector, 0, image);
    }

    protected void checkContainerImages(String namespaceName, LabelSelector labelSelector, int container, String image) {
        List<Pod> pods1 = KubeResourceManager.get().kubeClient().listPods(namespaceName, labelSelector);
        for (Pod pod : pods1) {
            if (!image.equals(pod.getSpec().getContainers().get(container).getImage())) {
                LOGGER.debug("Expected image for Pod: {}/{}: {} \nCurrent image: {}", pod.getMetadata().getNamespace(), pod.getMetadata().getName(), image, pod.getSpec().getContainers().get(container).getImage());
                assertThat("Used image for Pod: " + pod.getMetadata().getNamespace() + "/" + pod.getMetadata().getName() + " is not valid!", pod.getSpec().getContainers().get(container).getImage(), containsString(image));
            }
        }
    }

    protected void logComponentsPodImagesWithConnect(String componentsNamespaceName) {
        logPodImages(componentsNamespaceName, controllerSelector, brokerSelector, eoSelector, connectLabelSelector);
    }

    protected void logComponentsPodImages(String componentsNamespaceName) {
        logPodImages(componentsNamespaceName, controllerSelector, brokerSelector, eoSelector);
    }

    protected void logClusterOperatorPodImage(String clusterOperatorNamespaceName) {
        logPodImages(clusterOperatorNamespaceName, coSelector);
    }

    /**
     * Logs images of Pods' containers in the specified {@param namespaceName}. Each image is logged per each label selector.
     *
     * @param namespaceName the name of the Kubernetes namespace where the pods are located
     * @param labelSelectors optional array of {@link LabelSelector} objects used to filter pods based on labels.
     *                       If no selectors are provided, no Pods are selected.
     */
    protected void logPodImages(String namespaceName, LabelSelector... labelSelectors) {
        Arrays.stream(labelSelectors)
            .parallel()
            .map(selector -> KubeResourceManager.get().kubeClient().listPods(namespaceName, selector))
            .flatMap(Collection::stream)
            .forEach(pod ->
                pod.getSpec().getContainers().forEach(container ->
                    LOGGER.info("Pod: {}/{} has image {}",
                        pod.getMetadata().getNamespace(), pod.getMetadata().getName(), pod.getSpec().getContainers().get(0).getImage())
                ));
    }

    protected void waitForKafkaControllersAndBrokersFinishRollingUpdate(String componentsNamespaceName) {
        LOGGER.info("Waiting for Kafka rolling update to finish");
        controllerPods = RollingUpdateUtils.waitTillComponentHasRolled(componentsNamespaceName, controllerSelector, 3, controllerPods);
        brokerPods = RollingUpdateUtils.waitTillComponentHasRolled(componentsNamespaceName, brokerSelector, 3, brokerPods);
    }

    protected void applyKafkaCustomResourceFromPath(String namespaceName, String kafkaFilePath, String kafkaVersionFromCR, String kafkaMetadataVersion) {
        // Change kafka version of it's empty (null is for remove the version)
        String metadataVersion = kafkaVersionFromCR == null ? null : kafkaMetadataVersion;

        kafkaYaml = new File(kafkaFilePath);
        LOGGER.info("Deploying Kafka from: {}", kafkaYaml.getPath());
        KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).applyContent(KafkaUtils.changeOrRemoveKafkaConfigurationInKRaft(kafkaYaml, kafkaVersionFromCR, metadataVersion));
    }

    protected void applyCustomResourcesFromPath(String namespaceName, String examplesPath, String kafkaFilePath, String kafkaVersionFromCR, String kafkaMetadataVersion) {
        applyKafkaCustomResourceFromPath(namespaceName, kafkaFilePath, kafkaVersionFromCR, kafkaMetadataVersion);

        kafkaUserYaml = new File(examplesPath + "/examples/user/kafka-user.yaml");
        LOGGER.info("Deploying KafkaUser from: {}, in Namespace: {}", kafkaUserYaml.getPath(), namespaceName);
        KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).applyContent(KafkaUserUtils.removeKafkaUserPart(kafkaUserYaml, "authorization"));

        kafkaTopicYaml = new File(examplesPath + "/examples/topic/kafka-topic.yaml");
        LOGGER.info("Deploying KafkaTopic from: {}, in Namespace {}", kafkaTopicYaml.getPath(), namespaceName);
        KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).applyContent(ReadWriteUtils.readFile(kafkaTopicYaml));
        KubeResourceManager.get().pushToStack(new ResourceItem<>(() -> CrdClients.kafkaTopicClient().inNamespace(namespaceName).withName("my-topic").delete()));
    }

    private String getKafkaYamlWithName(String name) {
        String initialName = "name: my-topic";
        String newName =  "name: %s".formatted(name);

        return ReadWriteUtils.readFile(kafkaTopicYaml).replace(initialName, newName);
    }
    
    protected void verifyProcedure(String componentsNamespaceNames, BundleVersionModificationData upgradeData, String producerName, String consumerName) {

        if (upgradeData.getAdditionalTopics() != null) {
            // Check that topics weren't deleted/duplicated during upgrade procedures
            String listedTopics = KubeResourceManager.get().kubeCmdClient().inNamespace(componentsNamespaceNames).getResourcesAsYaml(getResourceApiVersion(KafkaTopic.RESOURCE_PLURAL));
            int additionalTopics = upgradeData.getAdditionalTopics();
            assertThat("KafkaTopic list doesn't have expected size", Long.valueOf(listedTopics.lines().count() - 1).intValue(), greaterThanOrEqualTo(UPGRADE_TOPIC_COUNT + additionalTopics));
            assertThat("KafkaTopic " + TOPIC_NAME + " is not in expected Topic list",
                listedTopics.contains(TOPIC_NAME), is(true));
            for (int x = 0; x < UPGRADE_TOPIC_COUNT; x++) {
                assertThat("KafkaTopic " + TOPIC_NAME + "-" + x + " is not in expected Topic list", listedTopics.contains(TOPIC_NAME + "-" + x), is(true));
            }
        }

        if (upgradeData.getContinuousClientsMessages() != 0) {
            // ##############################
            // Validate that continuous clients finished successfully
            // ##############################
            ClientUtils.waitForClientsSuccess(componentsNamespaceNames, consumerName, producerName, upgradeData.getContinuousClientsMessages());
            // ##############################
        }
    }

    /**
     * Based on {@param isUTOUsed} and {@param wasUTOUsedBefore} it returns the expected count of KafkaTopics.
     * In case that UTO was used before and after, the expected number of KafkaTopics is {@link #UPGRADE_TOPIC_COUNT}.
     * In other cases - BTO was used before or after the upgrade/downgrade - the expected number of KafkaTopics is {@link #UPGRADE_TOPIC_COUNT}
     * with {@link #BTO_KAFKA_TOPICS_ONLY_COUNT}.
     * @param isUTOUsed boolean value determining if UTO is used after upgrade/downgrade of the CO
     * @param wasUTOUsedBefore boolean value determining if UTO was used before upgrade/downgrade of the CO
     * @return expected number of KafkaTopics
     */
    protected int getExpectedTopicCount(boolean isUTOUsed, boolean wasUTOUsedBefore) {
        if (isUTOUsed && wasUTOUsedBefore) {
            // topics that are just present in Kafka itself are not created as CRs in UTO, thus -3 topics in comparison to regular upgrade
            return UPGRADE_TOPIC_COUNT;
        }

        return UPGRADE_TOPIC_COUNT + BTO_KAFKA_TOPICS_ONLY_COUNT;
    }

    protected String getResourceApiVersion(String resourcePlural) {
        return resourcePlural + "." + Constants.V1BETA2 + "." + Constants.RESOURCE_GROUP_NAME;
    }

    protected String downloadExamplesAndGetPath(CommonVersionModificationData versionModificationData) throws IOException {
        if (versionModificationData.getToUrl().equals("HEAD")) {
            return PATH_TO_PACKAGING;
        } else {
            File dir = FileUtils.downloadAndUnzip(versionModificationData.getToUrl());
            return dir.getAbsolutePath() + "/" + versionModificationData.getToExamples();
        }
    }

    /**
     * Sets up the namespaces required for the file-based Strimzi upgrade test.
     * This method creates and prepares the necessary namespaces if operator is installed from example files
     */
    protected void setUpStrimziUpgradeTestNamespaces() {
        NamespaceManager.getInstance().createNamespaceAndPrepare(CO_NAMESPACE);
        NamespaceManager.getInstance().createNamespaceAndPrepare(TEST_SUITE_NAMESPACE);
    }

    /**
     * Cleans resources installed to namespaces from example files and namespaces themselves.
     */
    protected void cleanUpStrimziUpgradeTestNamespaces() {
        cleanUpKafkaTopics(TEST_SUITE_NAMESPACE);
        deleteInstalledYamls(CO_NAMESPACE, TEST_SUITE_NAMESPACE, coDir);
        NamespaceManager.getInstance().deleteNamespaceWithWait(CO_NAMESPACE);
        NamespaceManager.getInstance().deleteNamespaceWithWait(TEST_SUITE_NAMESPACE);
    }

    protected void cleanUpKafkaTopics(String componentsNamespaceName) {
        if (CrdUtils.isCrdPresent(KafkaTopic.RESOURCE_PLURAL, KafkaTopic.RESOURCE_GROUP)) {
            // delete all topics created in test
            List<KafkaTopic> kafkaTopics = CrdClients.kafkaTopicClient().inNamespace(componentsNamespaceName).list().getItems();
            KubeResourceManager.get().deleteResourceWithWait(kafkaTopics.toArray(new KafkaTopic[0]));
        } else {
            LOGGER.info("Kafka Topic CustomResource Definition does not exist, no KafkaTopic is being deleted");
        }
    }

    protected void deleteInstalledYamls(String clusterOperatorNamespaceName, String componentsNamespaceName, File root) {
        if (kafkaUserYaml != null) {
            LOGGER.info("Deleting KafkaUser configuration files");
            KubeResourceManager.get().kubeCmdClient().inNamespace(componentsNamespaceName).delete(kafkaUserYaml);
        }
        if (kafkaTopicYaml != null) {
            LOGGER.info("Deleting KafkaTopic configuration files");
            KafkaTopicUtils.setFinalizersInAllTopicsToNull(componentsNamespaceName);
            KubeResourceManager.get().kubeCmdClient().inNamespace(componentsNamespaceName).delete(kafkaTopicYaml);
        }
        if (kafkaYaml != null) {
            LOGGER.info("Deleting Kafka configuration files");
            KubeResourceManager.get().kubeCmdClient().inNamespace(componentsNamespaceName).delete(kafkaYaml);
        }
        if (root != null) {
            final List<String> watchedNsRoleBindingFilePrefixes = List.of(
                "020-RoleBinding",  // rb to role for creating KNative resources
                "023-RoleBinding",  // rb to role for watching Strimzi CustomResources
                "031-RoleBinding"   // rb to role for entity operator
            );

            Arrays.stream(Objects.requireNonNull(root.listFiles())).sorted().forEach(f -> {
                try {
                    if (watchedNsRoleBindingFilePrefixes.stream().anyMatch((rbFilePrefix) -> f.getName().startsWith(rbFilePrefix))) {
                        KubeResourceManager.get().kubeCmdClient().inNamespace(componentsNamespaceName).deleteContent(StUtils.changeRoleBindingSubject(f, clusterOperatorNamespaceName));
                    } else if (f.getName().matches(".*RoleBinding.*")) {
                        KubeResourceManager.get().kubeCmdClient().inNamespace(clusterOperatorNamespaceName).deleteContent(StUtils.changeRoleBindingSubject(f, clusterOperatorNamespaceName));
                    } else {
                        KubeResourceManager.get().kubeCmdClient().inNamespace(clusterOperatorNamespaceName).delete(f);
                    }
                } catch (Exception ex) {
                    LOGGER.warn("Failed to delete resources: {}", f.getName());
                }
            });
        }
    }
}
