/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.connect.build.JarArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.Plugin;
import io.strimzi.api.kafka.model.connect.build.PluginBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
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
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import static io.strimzi.systemtest.TestConstants.CO_NAMESPACE;
import static io.strimzi.systemtest.TestConstants.DEFAULT_SINK_FILE_PATH;
import static io.strimzi.systemtest.TestConstants.PATH_TO_KAFKA_TOPIC_CONFIG;
import static io.strimzi.systemtest.TestConstants.PATH_TO_PACKAGING;
import static io.strimzi.systemtest.TestConstants.PATH_TO_PACKAGING_EXAMPLES;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.fail;

public class AbstractUpgradeST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(AbstractUpgradeST.class);

    protected File dir = null;
    protected File coDir = null;
    protected File kafkaTopicYaml = null;
    protected File kafkaUserYaml = null;
    protected File kafkaConnectYaml;

    protected final String clusterName = "my-cluster";
    protected final String poolName = "kafka";

    protected Map<String, String> controllerPods;
    protected Map<String, String> brokerPods;
    protected Map<String, String> eoPods;
    protected Map<String, String> coPods;
    protected Map<String, String> connectPods;

    protected final LabelSelector brokerSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaComponentName(clusterName));
    protected final LabelSelector controllerSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.zookeeperComponentName(clusterName));
    protected final LabelSelector eoSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.entityOperatorDeploymentName(clusterName));
    protected final LabelSelector coSelector = new LabelSelectorBuilder().withMatchLabels(Map.of(Labels.STRIMZI_KIND_LABEL, "cluster-operator")).build();
    protected final LabelSelector connectLabelSelector = KafkaConnectResource.getLabelSelector(clusterName, KafkaConnectResources.componentName(clusterName));

    protected final String topicName = "my-topic";
    protected final String userName = "my-user";
    protected final int upgradeTopicCount = 40;
    protected final int btoKafkaTopicsOnlyCount = 3;

    // ExpectedTopicCount contains additionally consumer-offset topic, my-topic and continuous-topic
    protected File kafkaYaml;

    /**
     * Based on {@param isUTOUsed} and {@param wasUTOUsedBefore} it returns the expected count of KafkaTopics.
     * In case that UTO was used before and after, the expected number of KafkaTopics is {@link #upgradeTopicCount}.
     * In other cases - BTO was used before or after the upgrade/downgrade - the expected number of KafkaTopics is {@link #upgradeTopicCount}
     * with {@link #btoKafkaTopicsOnlyCount}.
     * @param isUTOUsed boolean value determining if UTO is used after upgrade/downgrade of the CO
     * @param wasUTOUsedBefore boolean value determining if UTO was used before upgrade/downgrade of the CO
     * @return expected number of KafkaTopics
     */
    protected int getExpectedTopicCount(boolean isUTOUsed, boolean wasUTOUsedBefore) {
        if (isUTOUsed && wasUTOUsedBefore) {
            // topics that are just present in Kafka itself are not created as CRs in UTO, thus -3 topics in comparison to regular upgrade
            return upgradeTopicCount;
        }

        return upgradeTopicCount + btoKafkaTopicsOnlyCount;
    }

    protected void makeSnapshots() {
        coPods = DeploymentUtils.depSnapshot(TestConstants.CO_NAMESPACE, ResourceManager.getCoDeploymentName());
        controllerPods = PodUtils.podSnapshot(TestConstants.CO_NAMESPACE, controllerSelector);
        brokerPods = PodUtils.podSnapshot(TestConstants.CO_NAMESPACE, brokerSelector);
        eoPods = DeploymentUtils.depSnapshot(TestConstants.CO_NAMESPACE, KafkaResources.entityOperatorDeploymentName(clusterName));
        connectPods = PodUtils.podSnapshot(TestConstants.CO_NAMESPACE, connectLabelSelector);
    }

    @SuppressWarnings("CyclomaticComplexity")
    protected void changeKafkaAndLogFormatVersion(CommonVersionModificationData versionModificationData) throws IOException {
        // Get Kafka configurations
        String currentLogMessageFormat = cmdKubeClient().getResourceJsonPath(getResourceApiVersion(Kafka.RESOURCE_PLURAL), clusterName, ".spec.kafka.config.log\\.message\\.format\\.version");
        String currentInterBrokerProtocol = cmdKubeClient().getResourceJsonPath(getResourceApiVersion(Kafka.RESOURCE_PLURAL), clusterName, ".spec.kafka.config.inter\\.broker\\.protocol\\.version");

        // Get Kafka version
        String kafkaVersionFromCR = cmdKubeClient().getResourceJsonPath(getResourceApiVersion(Kafka.RESOURCE_PLURAL), clusterName, ".spec.kafka.version");
        kafkaVersionFromCR = kafkaVersionFromCR.equals("") ? null : kafkaVersionFromCR;
        String kafkaVersionFromProcedure = versionModificationData.getProcedures().getVersion();

        // #######################################################################
        // #################    Update CRs to latest version   ###################
        // #######################################################################
        String examplesPath = "";
        if (versionModificationData.getToUrl().equals("HEAD")) {
            examplesPath = PATH_TO_PACKAGING;
        } else {
            File dir = FileUtils.downloadAndUnzip(versionModificationData.getToUrl());
            examplesPath = dir.getAbsolutePath() + "/" + versionModificationData.getToExamples();
        }

        kafkaYaml = new File(examplesPath + versionModificationData.getKafkaFilePathAfter());
        LOGGER.info("Deploying Kafka from: {}", kafkaYaml.getPath());
        // Change kafka version of it's empty (null is for remove the version)
        String defaultValueForVersions = kafkaVersionFromCR == null ? null : TestKafkaVersion.getSpecificVersion(kafkaVersionFromCR).messageVersion();
        cmdKubeClient().applyContent(KafkaUtils.changeOrRemoveKafkaConfiguration(kafkaYaml, kafkaVersionFromCR, defaultValueForVersions, defaultValueForVersions));

        kafkaUserYaml = new File(examplesPath + "/examples/user/kafka-user.yaml");
        LOGGER.info("Deploying KafkaUser from: {}", kafkaUserYaml.getPath());
        cmdKubeClient().applyContent(KafkaUserUtils.removeKafkaUserPart(kafkaUserYaml, "authorization"));

        kafkaTopicYaml = new File(examplesPath + "/examples/topic/kafka-topic.yaml");
        LOGGER.info("Deploying KafkaTopic from: {}", kafkaTopicYaml.getPath());
        cmdKubeClient().applyContent(TestUtils.readFile(kafkaTopicYaml));
        // #######################################################################


        if (versionModificationData.getProcedures() != null && (!currentLogMessageFormat.isEmpty() || !currentInterBrokerProtocol.isEmpty())) {
            if (kafkaVersionFromProcedure != null && !kafkaVersionFromProcedure.isEmpty() && !kafkaVersionFromCR.contains(kafkaVersionFromProcedure)
                    && ResourceManager.getTestContext().getTestClass().get().getSimpleName().toLowerCase(Locale.ROOT).contains("upgrade")) {
                LOGGER.info("Set Kafka version to " + kafkaVersionFromProcedure);
                cmdKubeClient().patchResource(getResourceApiVersion(Kafka.RESOURCE_PLURAL), clusterName, "/spec/kafka/version", kafkaVersionFromProcedure);
                LOGGER.info("Waiting for Kafka rolling update to finish");
                brokerPods = RollingUpdateUtils.waitTillComponentHasRolled(TestConstants.CO_NAMESPACE, brokerSelector, 3, brokerPods);
            }

            String logMessageVersion = versionModificationData.getProcedures().getLogMessageVersion();
            String interBrokerProtocolVersion = versionModificationData.getProcedures().getInterBrokerVersion();

            if (logMessageVersion != null && !logMessageVersion.isEmpty() || interBrokerProtocolVersion != null && !interBrokerProtocolVersion.isEmpty()) {
                if (!logMessageVersion.isEmpty()) {
                    LOGGER.info("Set log message format version to {} (current version is {})", logMessageVersion, currentLogMessageFormat);
                    cmdKubeClient().patchResource(getResourceApiVersion(Kafka.RESOURCE_PLURAL), clusterName, "/spec/kafka/config/log.message.format.version", logMessageVersion);
                }

                if (!interBrokerProtocolVersion.isEmpty()) {
                    LOGGER.info("Set inter-broker protocol version to {} (current version is {})", interBrokerProtocolVersion, currentInterBrokerProtocol);
                    LOGGER.info("Set inter-broker protocol version to " + interBrokerProtocolVersion);
                    cmdKubeClient().patchResource(getResourceApiVersion(Kafka.RESOURCE_PLURAL), clusterName, "/spec/kafka/config/inter.broker.protocol.version", interBrokerProtocolVersion);
                }

                if ((currentInterBrokerProtocol != null && !currentInterBrokerProtocol.equals(interBrokerProtocolVersion)) ||
                        (currentLogMessageFormat != null && !currentLogMessageFormat.isEmpty() && !currentLogMessageFormat.equals(logMessageVersion))) {
                    LOGGER.info("Waiting for Kafka rolling update to finish");
                    brokerPods = RollingUpdateUtils.waitTillComponentHasRolled(TestConstants.CO_NAMESPACE, brokerSelector, 3, brokerPods);
                }
                makeSnapshots();
            }

            if (kafkaVersionFromProcedure != null && !kafkaVersionFromProcedure.isEmpty() && !kafkaVersionFromCR.contains(kafkaVersionFromProcedure)
                    && ResourceManager.getTestContext().getTestClass().get().getSimpleName().toLowerCase(Locale.ROOT).contains("downgrade")) {
                LOGGER.info("Set Kafka version to " + kafkaVersionFromProcedure);
                cmdKubeClient().patchResource(getResourceApiVersion(Kafka.RESOURCE_PLURAL), clusterName, "/spec/kafka/version", kafkaVersionFromProcedure);
                LOGGER.info("Waiting for Kafka rolling update to finish");
                brokerPods = RollingUpdateUtils.waitTillComponentHasRolled(TestConstants.CO_NAMESPACE, brokerSelector, brokerPods);
            }
        }
    }

    protected void logPodImages(String namespaceName) {
        logPodImages(namespaceName, controllerSelector, brokerSelector, eoSelector, coSelector);
    }

    protected void logPodImagesWithConnect(String namespaceName) {
        logPodImages(namespaceName, controllerSelector, brokerSelector, eoSelector, connectLabelSelector, coSelector);
    }

    protected void logPodImages(String namespaceName, LabelSelector... labelSelectors) {
        for (LabelSelector labelSelector : labelSelectors) {
            List<Pod> pods = kubeClient().listPods(namespaceName, labelSelector);

            pods.forEach(pod ->
                pod.getSpec().getContainers().forEach(container ->
                    LOGGER.info("Pod: {}/{} has image {}",
                        pod.getMetadata().getNamespace(), pod.getMetadata().getName(), pod.getSpec().getContainers().get(0).getImage())
                )
            );
        }
    }

    protected void waitForKafkaClusterRollingUpdate() {
        LOGGER.info("Waiting for ZK StrimziPodSet roll");
        controllerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(TestConstants.CO_NAMESPACE, controllerSelector, 3, controllerPods);
        LOGGER.info("Waiting for Kafka StrimziPodSet roll");
        brokerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(TestConstants.CO_NAMESPACE, brokerSelector, 3, brokerPods);
        LOGGER.info("Waiting for EO Deployment roll");
        // Check the TO and UO also got upgraded
        eoPods = DeploymentUtils.waitTillDepHasRolled(TestConstants.CO_NAMESPACE, KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoPods);
    }

    protected void waitForReadinessOfKafkaCluster() {
        LOGGER.info("Waiting for ZooKeeper StrimziPodSet");
        RollingUpdateUtils.waitForComponentAndPodsReady(TestConstants.CO_NAMESPACE, controllerSelector, 3);
        LOGGER.info("Waiting for Kafka StrimziPodSet");
        RollingUpdateUtils.waitForComponentAndPodsReady(TestConstants.CO_NAMESPACE, brokerSelector, 3);
        LOGGER.info("Waiting for EO Deployment");
        DeploymentUtils.waitForDeploymentAndPodsReady(TestConstants.CO_NAMESPACE, KafkaResources.entityOperatorDeploymentName(clusterName), 1);
    }

    protected void changeClusterOperator(BundleVersionModificationData versionModificationData, String namespace) throws IOException {
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

        copyModifyApply(coDir, namespace, versionModificationData.getFeatureGatesAfter());

        LOGGER.info("Waiting for CO upgrade");
        DeploymentUtils.waitTillDepHasRolled(namespace, ResourceManager.getCoDeploymentName(), 1, coPods);
    }

    protected void copyModifyApply(File root, String namespace, final String strimziFeatureGatesValue) {
        KubeClusterResource.getInstance().setNamespace(namespace);

        Arrays.stream(Objects.requireNonNull(root.listFiles())).sorted().forEach(f -> {
            if (f.getName().matches(".*RoleBinding.*")) {
                cmdKubeClient().replaceContent(TestUtils.changeRoleBindingSubject(f, namespace));
            } else if (f.getName().matches(".*Deployment.*")) {
                cmdKubeClient().replaceContent(StUtils.changeDeploymentConfiguration(f, namespace, strimziFeatureGatesValue));
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
            KafkaTopicUtils.setFinalizersInAllTopicsToNull(namespace);
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

    protected void checkAllImages(BundleVersionModificationData versionModificationData, String namespaceName) {
        if (versionModificationData.getImagesAfterOperations().isEmpty()) {
            fail("There are no expected images");
        }

        checkContainerImages(controllerSelector, versionModificationData.getZookeeperImage());
        checkContainerImages(brokerSelector, versionModificationData.getKafkaImage());
        checkContainerImages(eoSelector, versionModificationData.getTopicOperatorImage());
        checkContainerImages(eoSelector, 1, versionModificationData.getUserOperatorImage());
    }

    protected void checkContainerImages(LabelSelector labelSelector, String image) {
        checkContainerImages(labelSelector, 0, image);
    }

    protected void checkContainerImages(LabelSelector labelSelector, int container, String image) {
        List<Pod> pods1 = kubeClient().listPods(labelSelector);
        for (Pod pod : pods1) {
            if (!image.equals(pod.getSpec().getContainers().get(container).getImage())) {
                LOGGER.debug("Expected image for Pod: {}/{}: {} \nCurrent image: {}", pod.getMetadata().getNamespace(), pod.getMetadata().getName(), image, pod.getSpec().getContainers().get(container).getImage());
                assertThat("Used image for Pod: " + pod.getMetadata().getNamespace() + "/" + pod.getMetadata().getName() + " is not valid!", pod.getSpec().getContainers().get(container).getImage(), containsString(image));
            }
        }
    }

    protected void setupEnvAndUpgradeClusterOperator(BundleVersionModificationData upgradeData, TestStorage testStorage, UpgradeKafkaVersion upgradeKafkaVersion, String namespace) throws IOException {
        LOGGER.info("Test upgrade of Cluster Operator from version: {} to version: {}", upgradeData.getFromVersion(), upgradeData.getToVersion());
        cluster.setNamespace(namespace);

        this.deployCoWithWaitForReadiness(upgradeData, namespace);
        this.deployKafkaClusterWithWaitForReadiness(upgradeData, upgradeKafkaVersion);
        this.deployKafkaUserWithWaitForReadiness(upgradeData, namespace);
        this.deployKafkaTopicWithWaitForReadiness(upgradeData);

        // Create bunch of topics for upgrade if it's specified in configuration
        if (upgradeData.getAdditionalTopics() != null && upgradeData.getAdditionalTopics() > 0) {
            if (upgradeData.getFromVersion().equals("HEAD")) {
                kafkaTopicYaml = new File(dir, PATH_TO_PACKAGING_EXAMPLES + "/topic/kafka-topic.yaml");
            } else {
                kafkaTopicYaml = new File(dir, upgradeData.getFromExamples() + "/examples/topic/kafka-topic.yaml");
            }
            for (int x = 0; x < upgradeTopicCount; x++) {
                cmdKubeClient().applyContent(TestUtils.getContent(kafkaTopicYaml, TestUtils::toYamlString).replace("name: \"my-topic\"", "name: \"" + topicName + "-" + x + "\""));
            }
        }

        if (upgradeData.getContinuousClientsMessages() != 0) {
            // ##############################
            // Attach clients which will continuously produce/consume messages to/from Kafka brokers during rolling update
            // ##############################
            // Setup topic, which has 3 replicas and 2 min.isr to see if producer will be able to work during rolling update
            if (!cmdKubeClient().getResources(getResourceApiVersion(KafkaTopic.RESOURCE_PLURAL)).contains(testStorage.getTopicName())) {
                String pathToTopicExamples = upgradeData.getFromExamples().equals("HEAD") ? PATH_TO_KAFKA_TOPIC_CONFIG : upgradeData.getFromExamples() + "/examples/topic/kafka-topic.yaml";

                kafkaTopicYaml = new File(dir, pathToTopicExamples);
                cmdKubeClient().applyContent(TestUtils.getContent(kafkaTopicYaml, TestUtils::toYamlString)
                        .replace("name: \"my-topic\"", "name: \"" + testStorage.getTopicName() + "\"")
                        .replace("partitions: 1", "partitions: 3")
                        .replace("replicas: 1", "replicas: 3") +
                        "    min.insync.replicas: 2");

                ResourceManager.waitForResourceReadiness(getResourceApiVersion(KafkaTopic.RESOURCE_PLURAL), testStorage.getTopicName());
            }

            String producerAdditionConfiguration = "delivery.timeout.ms=20000\nrequest.timeout.ms=20000";

            KafkaClients kafkaBasicClientJob = ClientUtils.getContinuousPlainClientBuilder(testStorage)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(clusterName))
                .withMessageCount(upgradeData.getContinuousClientsMessages())
                .withAdditionalConfig(producerAdditionConfiguration)
                .withNamespaceName(namespace)
                .build();

            resourceManager.createResourceWithWait(
                kafkaBasicClientJob.producerStrimzi(),
                kafkaBasicClientJob.consumerStrimzi()
            );
            // ##############################
        }

        makeSnapshots();
    }

    protected void verifyProcedure(BundleVersionModificationData upgradeData, String producerName, String consumerName, String namespace, boolean wasUTOUsedBefore) {

        if (upgradeData.getAdditionalTopics() != null) {
            boolean isUTOUsed = StUtils.isUnidirectionalTopicOperatorUsed(namespace, eoSelector);

            // Check that topics weren't deleted/duplicated during upgrade procedures
            String listedTopics = cmdKubeClient().getResources(getResourceApiVersion(KafkaTopic.RESOURCE_PLURAL));
            int additionalTopics = upgradeData.getAdditionalTopics();
            assertThat("KafkaTopic list doesn't have expected size", Long.valueOf(listedTopics.lines().count() - 1).intValue(), greaterThanOrEqualTo(getExpectedTopicCount(isUTOUsed, wasUTOUsedBefore) + additionalTopics));
            assertThat("KafkaTopic " + topicName + " is not in expected Topic list",
                    listedTopics.contains(topicName), is(true));
            for (int x = 0; x < upgradeTopicCount; x++) {
                assertThat("KafkaTopic " + topicName + "-" + x + " is not in expected Topic list", listedTopics.contains(topicName + "-" + x), is(true));
            }
        }

        if (upgradeData.getContinuousClientsMessages() != 0) {
            // ##############################
            // Validate that continuous clients finished successfully
            // ##############################
            ClientUtils.waitForClientsSuccess(producerName, consumerName, namespace, upgradeData.getContinuousClientsMessages());
            // ##############################
        }
    }

    protected String getResourceApiVersion(String resourcePlural) {
        return resourcePlural + "." + Constants.V1BETA2 + "." + Constants.RESOURCE_GROUP_NAME;
    }

    protected void deployCoWithWaitForReadiness(final BundleVersionModificationData upgradeData,
                                                final String namespaceName) throws IOException {
        LOGGER.info("Deploying CO: {} in Namespace: {}", ResourceManager.getCoDeploymentName(), namespaceName);

        if (upgradeData.getFromVersion().equals("HEAD")) {
            coDir = new File(TestUtils.USER_PATH + "/../packaging/install/cluster-operator");
        } else {
            final String url = upgradeData.getFromUrl();
            dir = FileUtils.downloadAndUnzip(url);
            coDir = new File(dir, upgradeData.getFromExamples() + "/install/cluster-operator/");
        }

        // Modify + apply installation files
        copyModifyApply(coDir, namespaceName, upgradeData.getFeatureGatesBefore());

        LOGGER.info("Waiting for Deployment: {}", ResourceManager.getCoDeploymentName());
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, ResourceManager.getCoDeploymentName(), 1);
        LOGGER.info("{} is ready", ResourceManager.getCoDeploymentName());
    }


    protected void deployKafkaClusterWithWaitForReadiness(final BundleVersionModificationData upgradeData,
                                                          final UpgradeKafkaVersion upgradeKafkaVersion) {
        LOGGER.info("Deploying Kafka: {} in Namespace: {}", clusterName, kubeClient().getNamespace());

        if (!cmdKubeClient().getResources(getResourceApiVersion(Kafka.RESOURCE_PLURAL)).contains(clusterName)) {
            // Deploy a Kafka cluster
            if (upgradeData.getFromExamples().equals("HEAD")) {
                resourceManager.createResourceWithWait(KafkaNodePoolTemplates.brokerPoolPersistentStorage(CO_NAMESPACE, poolName, clusterName, 3).build());
                resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(clusterName, 3, 3)
                    .editSpec()
                        .editKafka()
                            .withVersion(upgradeKafkaVersion.getVersion())
                            .addToConfig("log.message.format.version", upgradeKafkaVersion.getLogMessageVersion())
                            .addToConfig("inter.broker.protocol.version", upgradeKafkaVersion.getInterBrokerVersion())
                        .endKafka()
                    .endSpec()
                    .build());
            } else {
                kafkaYaml = new File(dir, upgradeData.getFromExamples() + upgradeData.getKafkaFilePathBefore());
                LOGGER.info("Deploying Kafka from: {}", kafkaYaml.getPath());
                // Change kafka version of it's empty (null is for remove the version)
                if (upgradeKafkaVersion == null) {
                    cmdKubeClient().applyContent(KafkaUtils.changeOrRemoveKafkaVersion(kafkaYaml, null));
                } else {
                    cmdKubeClient().applyContent(KafkaUtils.changeOrRemoveKafkaConfiguration(kafkaYaml, upgradeKafkaVersion.getVersion(), upgradeKafkaVersion.getLogMessageVersion(), upgradeKafkaVersion.getInterBrokerVersion()));
                }
                // Wait for readiness
                waitForReadinessOfKafkaCluster();
            }
        }
    }

    protected void deployKafkaUserWithWaitForReadiness(final BundleVersionModificationData upgradeData,
                                                       final String namespaceName) {
        LOGGER.info("Deploying KafkaUser: {}/{}", kubeClient().getNamespace(), userName);

        if (!cmdKubeClient().getResources(getResourceApiVersion(KafkaUser.RESOURCE_PLURAL)).contains(userName)) {
            if (upgradeData.getFromVersion().equals("HEAD")) {
                resourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(namespaceName, clusterName, userName).build());
            } else {
                kafkaUserYaml = new File(dir, upgradeData.getFromExamples() + "/examples/user/kafka-user.yaml");
                LOGGER.info("Deploying KafkaUser from: {}", kafkaUserYaml.getPath());
                cmdKubeClient().applyContent(KafkaUserUtils.removeKafkaUserPart(kafkaUserYaml, "authorization"));
                ResourceManager.waitForResourceReadiness(getResourceApiVersion(KafkaUser.RESOURCE_PLURAL), userName);
            }
        }
    }

    protected void deployKafkaTopicWithWaitForReadiness(final BundleVersionModificationData upgradeData) {
        LOGGER.info("Deploying KafkaTopic: {}/{}", kubeClient().getNamespace(), topicName);

        if (!cmdKubeClient().getResources(getResourceApiVersion(KafkaTopic.RESOURCE_PLURAL)).contains(topicName)) {
            if (upgradeData.getFromVersion().equals("HEAD")) {
                kafkaTopicYaml = new File(dir, PATH_TO_PACKAGING_EXAMPLES + "/topic/kafka-topic.yaml");
            } else {
                kafkaTopicYaml = new File(dir, upgradeData.getFromExamples() + "/examples/topic/kafka-topic.yaml");
            }
            LOGGER.info("Deploying KafkaTopic from: {}", kafkaTopicYaml.getPath());
            cmdKubeClient().create(kafkaTopicYaml);
            ResourceManager.waitForResourceReadiness(getResourceApiVersion(KafkaTopic.RESOURCE_PLURAL), topicName);
        }
    }

    protected void deployKafkaConnectAndKafkaConnectorWithWaitForReadiness(final BundleVersionModificationData acrossUpgradeData,
                                                                            final UpgradeKafkaVersion upgradeKafkaVersion,
                                                                            final TestStorage testStorage) {
        // setup KafkaConnect + KafkaConnector
        if (!cmdKubeClient().getResources(getResourceApiVersion(KafkaConnect.RESOURCE_PLURAL)).contains(clusterName)) {
            if (acrossUpgradeData.getFromVersion().equals("HEAD")) {
                resourceManager.createResourceWithWait(KafkaConnectTemplates.kafkaConnectWithFilePlugin(clusterName, testStorage.getNamespaceName(), 1)
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
                resourceManager.createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(clusterName)
                    .editMetadata()
                        .withNamespace(testStorage.getNamespaceName())
                    .endMetadata()
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

                KafkaConnect kafkaConnect = new KafkaConnectBuilder(TestUtils.configFromYaml(kafkaConnectYaml, KafkaConnect.class))
                    .editMetadata()
                        .withName(clusterName)
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

                LOGGER.info("Deploying KafkaConnect from: {}", kafkaConnectYaml.getPath());

                cmdKubeClient().applyContent(TestUtils.toYamlString(kafkaConnect));
                ResourceManager.waitForResourceReadiness(getResourceApiVersion(KafkaConnect.RESOURCE_PLURAL), kafkaConnect.getMetadata().getName());

                // in our examples is no sink connector and thus we are using the same as in HEAD verification
                resourceManager.createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(clusterName)
                    .editMetadata()
                        .withNamespace(testStorage.getNamespaceName())
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

    protected void doKafkaConnectAndKafkaConnectorUpgradeOrDowngradeProcedure(final BundleVersionModificationData bundleDowngradeDataWithFeatureGates,
                                                                              final TestStorage testStorage,
                                                                              final UpgradeKafkaVersion upgradeKafkaVersion) throws IOException {
        this.deployCoWithWaitForReadiness(bundleDowngradeDataWithFeatureGates, testStorage.getNamespaceName());
        this.deployKafkaClusterWithWaitForReadiness(bundleDowngradeDataWithFeatureGates, upgradeKafkaVersion);
        this.deployKafkaConnectAndKafkaConnectorWithWaitForReadiness(bundleDowngradeDataWithFeatureGates, upgradeKafkaVersion, testStorage);
        this.deployKafkaUserWithWaitForReadiness(bundleDowngradeDataWithFeatureGates, testStorage.getNamespaceName());

        final KafkaClients clients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(clusterName))
            .withUsername(userName)
            .build();

        resourceManager.createResourceWithWait(clients.producerTlsStrimzi(clusterName));
        // Verify that Producer finish successfully
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        makeSnapshots();
        logPodImagesWithConnect(TestConstants.CO_NAMESPACE);

        // Verify FileSink KafkaConnector before upgrade
        String connectorPodName = kubeClient().listPods(testStorage.getNamespaceName(), Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND)).get(0).getMetadata().getName();
        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(testStorage.getNamespaceName(), connectorPodName, DEFAULT_SINK_FILE_PATH, testStorage.getMessageCount());

        // Upgrade CO to HEAD and wait for readiness of ClusterOperator
        changeClusterOperator(bundleDowngradeDataWithFeatureGates, testStorage.getNamespaceName());

        // Verify that Kafka cluster RU
        waitForKafkaClusterRollingUpdate();
        // Verify that KafkaConnect pods are rolling and KafkaConnector is ready
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), connectLabelSelector, 1, connectPods);
        KafkaConnectorUtils.waitForConnectorReady(testStorage.getNamespaceName(), clusterName);

        // send again new messages
        resourceManager.createResourceWithWait(clients.producerTlsStrimzi(clusterName));
        // Verify that Producer finish successfully
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);
        // Verify FileSink KafkaConnector
        connectorPodName = kubeClient().listPods(testStorage.getNamespaceName(), Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND)).get(0).getMetadata().getName();
        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(testStorage.getNamespaceName(), connectorPodName, DEFAULT_SINK_FILE_PATH, testStorage.getMessageCount());

        // Verify that pods are stable
        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), clusterName);
    }

    protected String downloadExamplesAndGetPath(CommonVersionModificationData versionModificationData) throws IOException {
        if (versionModificationData.getToUrl().equals("HEAD")) {
            return PATH_TO_PACKAGING;
        } else {
            File dir = FileUtils.downloadAndUnzip(versionModificationData.getToUrl());
            return dir.getAbsolutePath() + "/" + versionModificationData.getToExamples();
        }
    }

    protected void cleanUpKafkaTopics() {
        List<KafkaTopic> topics = KafkaTopicResource.kafkaTopicClient().inNamespace(CO_NAMESPACE).list().getItems();
        boolean finalizersAreSet = topics.stream().anyMatch(kafkaTopic -> kafkaTopic.getFinalizers() != null);

        // in case that we are upgrading/downgrading from UTO to BTO, we have to set finalizers on topics to null before deleting them
        if (!StUtils.isUnidirectionalTopicOperatorUsed(CO_NAMESPACE, eoSelector) && finalizersAreSet) {
            KafkaTopicUtils.setFinalizersInAllTopicsToNull(CO_NAMESPACE);
        }

        // delete all topics created in test
        cmdKubeClient(TestConstants.CO_NAMESPACE).deleteAllByResource(KafkaTopic.RESOURCE_KIND);
        KafkaTopicUtils.waitForTopicWithPrefixDeletion(TestConstants.CO_NAMESPACE, topicName);
    }
}
