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
import io.strimzi.systemtest.resources.NamespaceManager;
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
import io.strimzi.test.ReadWriteUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
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
    protected Map<String, String> connectPods;

    protected final LabelSelector brokerSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaComponentName(clusterName));
    protected final LabelSelector controllerSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.zookeeperComponentName(clusterName));
    protected final LabelSelector eoSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.entityOperatorDeploymentName(clusterName));
    protected final LabelSelector coSelector = new LabelSelectorBuilder().withMatchLabels(Map.of(Labels.STRIMZI_KIND_LABEL, "cluster-operator")).build();
    protected final LabelSelector connectLabelSelector = KafkaConnectResource.getLabelSelector(clusterName, KafkaConnectResources.componentName(clusterName));

    protected final String topicName = "my-topic";
    protected final String userName = "my-user";
    protected final int upgradeTopicCount = 20;
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

    protected void makeComponentsSnapshots(String componentsNamespaceName) {
        controllerPods = PodUtils.podSnapshot(componentsNamespaceName, controllerSelector);
        brokerPods = PodUtils.podSnapshot(componentsNamespaceName, brokerSelector);
        eoPods = DeploymentUtils.depSnapshot(componentsNamespaceName, KafkaResources.entityOperatorDeploymentName(clusterName));
        connectPods = PodUtils.podSnapshot(componentsNamespaceName, connectLabelSelector);
    }

    @SuppressWarnings("CyclomaticComplexity")
    protected void changeKafkaVersion(String componentsNamespaceName, CommonVersionModificationData versionModificationData) throws IOException {
        // Get Kafka configurations
        String currentLogMessageFormat = cmdKubeClient(componentsNamespaceName).getResourceJsonPath(getResourceApiVersion(Kafka.RESOURCE_PLURAL), clusterName, ".spec.kafka.config.log\\.message\\.format\\.version");
        String currentInterBrokerProtocol = cmdKubeClient(componentsNamespaceName).getResourceJsonPath(getResourceApiVersion(Kafka.RESOURCE_PLURAL), clusterName, ".spec.kafka.config.inter\\.broker\\.protocol\\.version");

        // Get Kafka version
        String kafkaVersionFromCR = cmdKubeClient(componentsNamespaceName).getResourceJsonPath(getResourceApiVersion(Kafka.RESOURCE_PLURAL), clusterName, ".spec.kafka.version");
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
        cmdKubeClient(componentsNamespaceName).applyContent(KafkaUtils.changeOrRemoveKafkaConfiguration(kafkaYaml, kafkaVersionFromCR, defaultValueForVersions, defaultValueForVersions));

        kafkaUserYaml = new File(examplesPath + "/examples/user/kafka-user.yaml");
        LOGGER.info("Deploying KafkaUser from: {}", kafkaUserYaml.getPath());
        cmdKubeClient(componentsNamespaceName).applyContent(KafkaUserUtils.removeKafkaUserPart(kafkaUserYaml, "authorization"));

        kafkaTopicYaml = new File(examplesPath + "/examples/topic/kafka-topic.yaml");
        LOGGER.info("Deploying KafkaTopic from: {}", kafkaTopicYaml.getPath());
        cmdKubeClient(componentsNamespaceName).applyContent(ReadWriteUtils.readFile(kafkaTopicYaml));
        // #######################################################################


        if (versionModificationData.getProcedures() != null && (!currentLogMessageFormat.isEmpty() || !currentInterBrokerProtocol.isEmpty())) {
            if (kafkaVersionFromProcedure != null && !kafkaVersionFromProcedure.isEmpty() && !kafkaVersionFromCR.contains(kafkaVersionFromProcedure)
                    && ResourceManager.getTestContext().getTestClass().get().getSimpleName().toLowerCase(Locale.ROOT).contains("upgrade")) {
                LOGGER.info("Set Kafka version to " + kafkaVersionFromProcedure);
                cmdKubeClient(componentsNamespaceName).patchResource(getResourceApiVersion(Kafka.RESOURCE_PLURAL), clusterName, "/spec/kafka/version", kafkaVersionFromProcedure);
                LOGGER.info("Waiting for Kafka rolling update to finish");
                brokerPods = RollingUpdateUtils.waitTillComponentHasRolled(componentsNamespaceName, brokerSelector, 3, brokerPods);
            }

            String logMessageVersion = versionModificationData.getProcedures().getLogMessageVersion();
            String interBrokerProtocolVersion = versionModificationData.getProcedures().getInterBrokerVersion();

            if (logMessageVersion != null && !logMessageVersion.isEmpty() || interBrokerProtocolVersion != null && !interBrokerProtocolVersion.isEmpty()) {
                if (!logMessageVersion.isEmpty()) {
                    LOGGER.info("Set log message format version to {} (current version is {})", logMessageVersion, currentLogMessageFormat);
                    cmdKubeClient(componentsNamespaceName).patchResource(getResourceApiVersion(Kafka.RESOURCE_PLURAL), clusterName, "/spec/kafka/config/log.message.format.version", logMessageVersion);
                }

                if (!interBrokerProtocolVersion.isEmpty()) {
                    LOGGER.info("Set inter-broker protocol version to {} (current version is {})", interBrokerProtocolVersion, currentInterBrokerProtocol);
                    LOGGER.info("Set inter-broker protocol version to " + interBrokerProtocolVersion);
                    cmdKubeClient(componentsNamespaceName).patchResource(getResourceApiVersion(Kafka.RESOURCE_PLURAL), clusterName, "/spec/kafka/config/inter.broker.protocol.version", interBrokerProtocolVersion);
                }

                if ((currentInterBrokerProtocol != null && !currentInterBrokerProtocol.equals(interBrokerProtocolVersion)) ||
                        (currentLogMessageFormat != null && !currentLogMessageFormat.isEmpty() && !currentLogMessageFormat.equals(logMessageVersion))) {
                    LOGGER.info("Waiting for Kafka rolling update to finish");
                    brokerPods = RollingUpdateUtils.waitTillComponentHasRolled(componentsNamespaceName, brokerSelector, 3, brokerPods);
                }
                makeComponentsSnapshots(componentsNamespaceName);
            }

            if (kafkaVersionFromProcedure != null && !kafkaVersionFromProcedure.isEmpty() && !kafkaVersionFromCR.contains(kafkaVersionFromProcedure)
                    && ResourceManager.getTestContext().getTestClass().get().getSimpleName().toLowerCase(Locale.ROOT).contains("downgrade")) {
                LOGGER.info("Set Kafka version to " + kafkaVersionFromProcedure);
                cmdKubeClient(componentsNamespaceName).patchResource(getResourceApiVersion(Kafka.RESOURCE_PLURAL), clusterName, "/spec/kafka/version", kafkaVersionFromProcedure);
                LOGGER.info("Waiting for Kafka rolling update to finish");
                brokerPods = RollingUpdateUtils.waitTillComponentHasRolled(componentsNamespaceName, brokerSelector, brokerPods);
            }
        }
    }

    protected void changeKafkaVersionInKafkaConnect(String componentsNamespaceName, CommonVersionModificationData versionModificationData) {
        UpgradeKafkaVersion upgradeToKafkaVersion = new UpgradeKafkaVersion(versionModificationData.getProcedures().getVersion());

        LOGGER.info(String.format("Setting Kafka version to %s in Kafka Connect", upgradeToKafkaVersion.getVersion()));
        cmdKubeClient(componentsNamespaceName).patchResource(getResourceApiVersion(KafkaConnect.RESOURCE_PLURAL), clusterName, "/spec/version", upgradeToKafkaVersion.getVersion());

        LOGGER.info("Waiting for KafkaConnect rolling update to finish");
        connectPods = RollingUpdateUtils.waitTillComponentHasRolled(componentsNamespaceName, connectLabelSelector, 1, connectPods);
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

    protected void logPodImages(String namespaceName, LabelSelector... labelSelectors) {
        Arrays.stream(labelSelectors)
            .parallel()
            .map(selector -> kubeClient().listPods(namespaceName, selector))
            .flatMap(Collection::stream)
            .forEach(pod ->
                pod.getSpec().getContainers().forEach(container ->
                    LOGGER.info("Pod: {}/{} has image {}",
                        pod.getMetadata().getNamespace(), pod.getMetadata().getName(), pod.getSpec().getContainers().get(0).getImage())
                ));
    }

    protected void waitForKafkaClusterRollingUpdate(final String componentsNamespaceName) {
        LOGGER.info("Waiting for ZK StrimziPodSet roll");
        controllerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(componentsNamespaceName, controllerSelector, 3, controllerPods);
        LOGGER.info("Waiting for Kafka StrimziPodSet roll");
        brokerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(componentsNamespaceName, brokerSelector, 3, brokerPods);
        LOGGER.info("Waiting for EO Deployment roll");
        // Check the TO and UO also got upgraded
        eoPods = DeploymentUtils.waitTillDepHasRolled(componentsNamespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoPods);
    }

    protected void waitForReadinessOfKafkaCluster(final String componentsNamespaceName) {
        LOGGER.info("Waiting for ZooKeeper StrimziPodSet");
        RollingUpdateUtils.waitForComponentAndPodsReady(componentsNamespaceName, controllerSelector, 3);
        LOGGER.info("Waiting for Kafka StrimziPodSet");
        RollingUpdateUtils.waitForComponentAndPodsReady(componentsNamespaceName, brokerSelector, 3);
        LOGGER.info("Waiting for EO Deployment");
        DeploymentUtils.waitForDeploymentAndPodsReady(componentsNamespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), 1);
    }

    protected void changeClusterOperator(String clusterOperatorNamespaceName, String componentsNamespaceName, BundleVersionModificationData versionModificationData) throws IOException {
        final Map<String, String> coPods = DeploymentUtils.depSnapshot(clusterOperatorNamespaceName, ResourceManager.getCoDeploymentName());

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

        modifyApplyClusterOperatorWithCRDsFromFile(clusterOperatorNamespaceName, componentsNamespaceName, coDir, versionModificationData.getFeatureGatesAfter());

        LOGGER.info("Waiting for CO upgrade");
        DeploymentUtils.waitTillDepHasRolled(clusterOperatorNamespaceName, ResourceManager.getCoDeploymentName(), 1, coPods);
    }

    /**
     * Series of steps done when applying operator from files located in root directory. Operator deployment is modified
     * to watch multiple (single) namespace. All role based access control resources are modified so the subject is found
     * in operator namespace. Role bindings concerning operands are modified to be deployed in watched namespace.
     *
     * @param clusterOperatorNamespaceName   the name of the namespace where the Strimzi operator is deployed.
     * @param componentsNamespaceName    the name of the single namespace being watched and managed by the Strimzi operator.
     * @param root                    the root directory containing the YAML files to be processed.
     * @param strimziFeatureGatesValue the value of the Strimzi feature gates to be injected into deployment configurations.
     */
    protected void modifyApplyClusterOperatorWithCRDsFromFile(String clusterOperatorNamespaceName, String componentsNamespaceName, File root, final String strimziFeatureGatesValue) {
        KubeClusterResource.getInstance().setNamespace(clusterOperatorNamespaceName);

        final List<String> watchedNsRoleBindingFilePrefixes = List.of(
            "020-RoleBinding",  // rb to role for creating KNative resources
            "023-RoleBinding",  // rb to role for watching Strimzi custom resources
            "031-RoleBinding"   // rb to role for entity operator
        );

        Arrays.stream(Objects.requireNonNull(root.listFiles())).sorted().forEach(f -> {
            if (watchedNsRoleBindingFilePrefixes.stream().anyMatch((rbFilePrefix) -> f.getName().startsWith(rbFilePrefix))) {
                cmdKubeClient(componentsNamespaceName).replaceContent(StUtils.changeRoleBindingSubject(f, clusterOperatorNamespaceName));
            } else if (f.getName().matches(".*RoleBinding.*")) {
                cmdKubeClient(clusterOperatorNamespaceName).replaceContent(StUtils.changeRoleBindingSubject(f, clusterOperatorNamespaceName));
            } else if (f.getName().matches(".*Deployment.*")) {
                cmdKubeClient(clusterOperatorNamespaceName).replaceContent(StUtils.changeDeploymentConfiguration(componentsNamespaceName, f, strimziFeatureGatesValue));
            } else {
                cmdKubeClient(clusterOperatorNamespaceName).replaceContent(ReadWriteUtils.readFile(f));
            }
        });
    }

    protected void deleteInstalledYamls(String clusterOperatorNamespaceName, String componentsNamespaceName, File root) {
        if (kafkaUserYaml != null) {
            LOGGER.info("Deleting KafkaUser configuration files");
            cmdKubeClient(componentsNamespaceName).delete(kafkaUserYaml);
        }
        if (kafkaTopicYaml != null) {
            LOGGER.info("Deleting KafkaTopic configuration files");
            KafkaTopicUtils.setFinalizersInAllTopicsToNull(componentsNamespaceName);
            cmdKubeClient().delete(kafkaTopicYaml);
        }
        if (kafkaYaml != null) {
            LOGGER.info("Deleting Kafka configuration files");
            cmdKubeClient(componentsNamespaceName).delete(kafkaYaml);
        }
        if (root != null) {
            final List<String> watchedNsRoleBindingFilePrefixes = List.of(
                "020-RoleBinding",  // rb to role for creating KNative resources
                "023-RoleBinding",  // rb to role for watching Strimzi custom resources
                "031-RoleBinding"   // rb to role for entity operator
            );

            Arrays.stream(Objects.requireNonNull(root.listFiles())).sorted().forEach(f -> {
                try {
                    if (watchedNsRoleBindingFilePrefixes.stream().anyMatch((rbFilePrefix) -> f.getName().startsWith(rbFilePrefix))) {
                        cmdKubeClient(componentsNamespaceName).deleteContent(StUtils.changeRoleBindingSubject(f, clusterOperatorNamespaceName));
                    } else if (f.getName().matches(".*RoleBinding.*")) {
                        cmdKubeClient(clusterOperatorNamespaceName).deleteContent(StUtils.changeRoleBindingSubject(f, clusterOperatorNamespaceName));
                    } else {
                        cmdKubeClient(clusterOperatorNamespaceName).delete(f);
                    }
                } catch (Exception ex) {
                    LOGGER.warn("Failed to delete resources: {}", f.getName());
                }
            });
        }
    }

    protected void checkAllComponentsImages(String componentsNamespaceName, BundleVersionModificationData versionModificationData) {
        if (versionModificationData.getImagesAfterOperations().isEmpty()) {
            fail("There are no expected images");
        }

        checkContainerImages(componentsNamespaceName, controllerSelector, versionModificationData.getZookeeperImage());
        checkContainerImages(componentsNamespaceName, brokerSelector, versionModificationData.getKafkaImage());
        checkContainerImages(componentsNamespaceName, eoSelector, versionModificationData.getTopicOperatorImage());
        checkContainerImages(componentsNamespaceName, eoSelector, 1, versionModificationData.getUserOperatorImage());
    }

    protected void checkContainerImages(String namespaceName, LabelSelector labelSelector, String image) {
        checkContainerImages(namespaceName, labelSelector, 0, image);
    }

    protected void checkContainerImages(String namespaceName, LabelSelector labelSelector, int container, String image) {
        List<Pod> pods1 = kubeClient(namespaceName).listPods(labelSelector);
        for (Pod pod : pods1) {
            if (!image.equals(pod.getSpec().getContainers().get(container).getImage())) {
                LOGGER.debug("Expected image for Pod: {}/{}: {} \nCurrent image: {}", pod.getMetadata().getNamespace(), pod.getMetadata().getName(), image, pod.getSpec().getContainers().get(container).getImage());
                assertThat("Used image for Pod: " + pod.getMetadata().getNamespace() + "/" + pod.getMetadata().getName() + " is not valid!", pod.getSpec().getContainers().get(container).getImage(), containsString(image));
            }
        }
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

            String topicNameTemplate = topicName + "-%s";
            IntStream.range(0, upgradeTopicCount)
                .mapToObj(topicNameTemplate::formatted)
                .map(this::getKafkaYamlWithName)
                .parallel()
                .forEach(cmdKubeClient(testStorage.getNamespaceName())::applyContent);
        }

        if (upgradeData.getContinuousClientsMessages() != 0) {
            // ##############################
            // Attach clients which will continuously produce/consume messages to/from Kafka brokers during rolling update
            // ##############################
            // Setup topic, which has 3 replicas and 2 min.isr to see if producer will be able to work during rolling update
            if (!cmdKubeClient(testStorage.getNamespaceName()).getResources(getResourceApiVersion(KafkaTopic.RESOURCE_PLURAL)).contains(testStorage.getTopicName())) {
                String pathToTopicExamples = upgradeData.getFromExamples().equals("HEAD") ? PATH_TO_KAFKA_TOPIC_CONFIG : upgradeData.getFromExamples() + "/examples/topic/kafka-topic.yaml";

                kafkaTopicYaml = new File(dir, pathToTopicExamples);
                cmdKubeClient(testStorage.getNamespaceName()).applyContent(ReadWriteUtils.readFile(kafkaTopicYaml)
                        .replace("name: my-topic", "name: " + testStorage.getTopicName())
                        .replace("partitions: 1", "partitions: 3")
                        .replace("replicas: 1", "replicas: 3") +
                        "    min.insync.replicas: 2");

                ResourceManager.waitForResourceReadiness(testStorage.getNamespaceName(), getResourceApiVersion(KafkaTopic.RESOURCE_PLURAL), testStorage.getTopicName());
            }

            // 40s is used within TF environment to make upgrade/downgrade more stable on slow env
            String producerAdditionConfiguration = "delivery.timeout.ms=40000\nrequest.timeout.ms=5000";

            KafkaClients kafkaBasicClientJob = ClientUtils.getContinuousPlainClientBuilder(testStorage)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(clusterName))
                .withMessageCount(upgradeData.getContinuousClientsMessages())
                .withAdditionalConfig(producerAdditionConfiguration)
                .withNamespaceName(testStorage.getNamespaceName())
                .build();

            resourceManager.createResourceWithWait(
                kafkaBasicClientJob.producerStrimzi(),
                kafkaBasicClientJob.consumerStrimzi()
            );
            // ##############################
        }

        makeComponentsSnapshots(testStorage.getNamespaceName());
    }

    private String getKafkaYamlWithName(String name) {
        String initialName = "name: my-topic";
        String newName =  "name: %s".formatted(name);

        return ReadWriteUtils.readFile(kafkaTopicYaml).replace(initialName, newName);
    }

    protected void verifyProcedure(String componentsNamespaceNames, BundleVersionModificationData upgradeData, String producerName, String consumerName,  boolean wasUTOUsedBefore) {

        if (upgradeData.getAdditionalTopics() != null) {
            boolean isUTOUsed = StUtils.isUnidirectionalTopicOperatorUsed(componentsNamespaceNames, eoSelector);

            // Check that topics weren't deleted/duplicated during upgrade procedures
            String listedTopics = cmdKubeClient(componentsNamespaceNames).getResources(getResourceApiVersion(KafkaTopic.RESOURCE_PLURAL));
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
            ClientUtils.waitForClientsSuccess(componentsNamespaceNames, consumerName, producerName, upgradeData.getContinuousClientsMessages());
            // ##############################
        }
    }

    protected String getResourceApiVersion(String resourcePlural) {
        return resourcePlural + "." + Constants.V1BETA2 + "." + Constants.RESOURCE_GROUP_NAME;
    }

    protected void deployCoWithWaitForReadiness(final String clusterOperatorNamespaceName, final String componentsNamespaceName, final BundleVersionModificationData upgradeData) throws IOException {
        LOGGER.info("Deploying CO: {} in Namespace: {}", ResourceManager.getCoDeploymentName(), clusterOperatorNamespaceName);

        if (upgradeData.getFromVersion().equals("HEAD")) {
            coDir = new File(TestUtils.USER_PATH + "/../packaging/install/cluster-operator");
        } else {
            final String url = upgradeData.getFromUrl();
            dir = FileUtils.downloadAndUnzip(url);
            coDir = new File(dir, upgradeData.getFromExamples() + "/install/cluster-operator/");
        }

        // Modify + apply installation files
        modifyApplyClusterOperatorWithCRDsFromFile(clusterOperatorNamespaceName, componentsNamespaceName, coDir, upgradeData.getFeatureGatesBefore());

        LOGGER.info("Waiting for Cluster Operator Deployment: {}", ResourceManager.getCoDeploymentName());
        DeploymentUtils.waitForDeploymentAndPodsReady(clusterOperatorNamespaceName, ResourceManager.getCoDeploymentName(), 1);
        LOGGER.info("{} is ready", ResourceManager.getCoDeploymentName());
    }


    protected void deployKafkaClusterWithWaitForReadiness(final String componentsNamespaceName,
                                                          final BundleVersionModificationData upgradeData,
                                                          final UpgradeKafkaVersion upgradeKafkaVersion) {
        LOGGER.info("Deploying Kafka: {}/{}", componentsNamespaceName, clusterName);

        if (!cmdKubeClient(componentsNamespaceName).getResources(getResourceApiVersion(Kafka.RESOURCE_PLURAL)).contains(clusterName)) {
            // Deploy a Kafka cluster
            if (upgradeData.getFromExamples().equals("HEAD")) {
                resourceManager.createResourceWithWait(KafkaNodePoolTemplates.brokerPoolPersistentStorage(componentsNamespaceName, poolName, clusterName, 3).build());
                resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistentNodePools(componentsNamespaceName, clusterName, 3, 3)
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
                    cmdKubeClient(componentsNamespaceName).applyContent(KafkaUtils.changeOrRemoveKafkaVersion(kafkaYaml, null));
                } else {
                    cmdKubeClient(componentsNamespaceName).applyContent(KafkaUtils.changeOrRemoveKafkaConfiguration(kafkaYaml, upgradeKafkaVersion.getVersion(), upgradeKafkaVersion.getLogMessageVersion(), upgradeKafkaVersion.getInterBrokerVersion()));
                }
                // Wait for readiness
                waitForReadinessOfKafkaCluster(componentsNamespaceName);
            }
        }
    }

    protected void deployKafkaUserWithWaitForReadiness(final String componentsNamespaceName, final BundleVersionModificationData upgradeData) {
        LOGGER.info("Deploying KafkaUser: {}/{}", componentsNamespaceName, userName);

        if (!cmdKubeClient(componentsNamespaceName).getResources(getResourceApiVersion(KafkaUser.RESOURCE_PLURAL)).contains(userName)) {
            if (upgradeData.getFromVersion().equals("HEAD")) {
                resourceManager.createResourceWithWait(KafkaUserTemplates.tlsUser(componentsNamespaceName, userName, clusterName).build());
            } else {
                kafkaUserYaml = new File(dir, upgradeData.getFromExamples() + "/examples/user/kafka-user.yaml");
                LOGGER.info("Deploying KafkaUser from: {}", kafkaUserYaml.getPath());
                cmdKubeClient(componentsNamespaceName).applyContent(KafkaUserUtils.removeKafkaUserPart(kafkaUserYaml, "authorization"));
                ResourceManager.waitForResourceReadiness(componentsNamespaceName, getResourceApiVersion(KafkaUser.RESOURCE_PLURAL), userName);
            }
        }
    }

    protected void deployKafkaTopicWithWaitForReadiness(final String componentsNamespaceName, final BundleVersionModificationData upgradeData) {
        LOGGER.info("Deploying KafkaTopic: {}/{}", componentsNamespaceName, topicName);

        if (!cmdKubeClient(componentsNamespaceName).getResources(getResourceApiVersion(KafkaTopic.RESOURCE_PLURAL)).contains(topicName)) {
            if (upgradeData.getFromVersion().equals("HEAD")) {
                kafkaTopicYaml = new File(dir, PATH_TO_PACKAGING_EXAMPLES + "/topic/kafka-topic.yaml");
            } else {
                kafkaTopicYaml = new File(dir, upgradeData.getFromExamples() + "/examples/topic/kafka-topic.yaml");
            }
            LOGGER.info("Deploying KafkaTopic from: {}", kafkaTopicYaml.getPath());
            cmdKubeClient(componentsNamespaceName).create(kafkaTopicYaml);
            ResourceManager.waitForResourceReadiness(componentsNamespaceName, getResourceApiVersion(KafkaTopic.RESOURCE_PLURAL), topicName);
        }
    }

    protected void deployKafkaConnectAndKafkaConnectorWithWaitForReadiness(
        final TestStorage testStorage,
        final BundleVersionModificationData acrossUpgradeData,
        final UpgradeKafkaVersion upgradeKafkaVersion
    ) {
        // setup KafkaConnect + KafkaConnector
        if (!cmdKubeClient(testStorage.getNamespaceName()).getResources(getResourceApiVersion(KafkaConnect.RESOURCE_PLURAL)).contains(clusterName)) {
            if (acrossUpgradeData.getFromVersion().equals("HEAD")) {
                resourceManager.createResourceWithWait(KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getNamespaceName(), clusterName, 1)
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
                resourceManager.createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(), clusterName)
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

                KafkaConnect kafkaConnect = new KafkaConnectBuilder(ReadWriteUtils.readObjectFromYamlFilepath(kafkaConnectYaml, KafkaConnect.class))
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

                cmdKubeClient(testStorage.getNamespaceName()).applyContent(ReadWriteUtils.writeObjectToYamlString(kafkaConnect));
                ResourceManager.waitForResourceReadiness(testStorage.getNamespaceName(), getResourceApiVersion(KafkaConnect.RESOURCE_PLURAL), kafkaConnect.getMetadata().getName());

                // in our examples is no sink connector and thus we are using the same as in HEAD verification
                resourceManager.createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(), clusterName)
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

    protected void doKafkaConnectAndKafkaConnectorUpgradeOrDowngradeProcedure(
        final String clusterOperatorNamespaceName,
        final TestStorage testStorage,
        final BundleVersionModificationData upgradeDowngradeData,
        final UpgradeKafkaVersion upgradeKafkaVersion
    ) throws IOException {
        setupEnvAndUpgradeClusterOperator(clusterOperatorNamespaceName, testStorage, upgradeDowngradeData, upgradeKafkaVersion);
        this.deployKafkaConnectAndKafkaConnectorWithWaitForReadiness(testStorage, upgradeDowngradeData, upgradeKafkaVersion);

        // Check if UTO is used before changing the CO -> used for check for KafkaTopics
        boolean wasUTOUsedBefore = StUtils.isUnidirectionalTopicOperatorUsed(testStorage.getNamespaceName(), eoSelector);

        final KafkaClients clients = ClientUtils.getInstantTlsClientBuilder(testStorage, KafkaResources.tlsBootstrapAddress(clusterName))
            .withNamespaceName(testStorage.getNamespaceName())
            .withUsername(userName)
            .build();

        resourceManager.createResourceWithWait(clients.producerTlsStrimzi(clusterName));
        // Verify that Producer finish successfully
        ClientUtils.waitForInstantProducerClientSuccess(testStorage);

        makeComponentsSnapshots(testStorage.getNamespaceName());
        logComponentsPodImagesWithConnect(testStorage.getNamespaceName());

        // Verify FileSink KafkaConnector before upgrade
        String connectorPodName = kubeClient().listPods(testStorage.getNamespaceName(), Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND)).get(0).getMetadata().getName();
        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(testStorage.getNamespaceName(), connectorPodName, DEFAULT_SINK_FILE_PATH, testStorage.getMessageCount());

        // Upgrade CO to HEAD and wait for readiness of ClusterOperator
        changeClusterOperator(clusterOperatorNamespaceName, testStorage.getNamespaceName(), upgradeDowngradeData);

        if (TestKafkaVersion.supportedVersionsContainsVersion(upgradeKafkaVersion.getVersion())) {
            // Verify that Kafka and Connect Pods Rolled
            waitForKafkaClusterRollingUpdate(testStorage.getNamespaceName());
            connectPods = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), connectLabelSelector, 1, connectPods);
            KafkaConnectorUtils.waitForConnectorReady(testStorage.getNamespaceName(), clusterName);
        }

        logComponentsPodImagesWithConnect(testStorage.getNamespaceName());

        // Upgrade/Downgrade kafka
        changeKafkaVersion(testStorage.getNamespaceName(), upgradeDowngradeData);
        changeKafkaVersionInKafkaConnect(testStorage.getNamespaceName(), upgradeDowngradeData);

        logComponentsPodImagesWithConnect(testStorage.getNamespaceName());
        checkAllComponentsImages(testStorage.getNamespaceName(), upgradeDowngradeData);

        // send again new messages
        resourceManager.createResourceWithWait(clients.producerTlsStrimzi(clusterName));

        // Verify that Producer finish successfully
        ClientUtils.waitForInstantProducerClientSuccess(testStorage.getNamespaceName(), testStorage);

        // Verify FileSink KafkaConnector
        connectorPodName = kubeClient().listPods(testStorage.getNamespaceName(), Collections.singletonMap(Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND)).get(0).getMetadata().getName();
        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(testStorage.getNamespaceName(), connectorPodName, DEFAULT_SINK_FILE_PATH, testStorage.getMessageCount());

        // Verify that pods are stable
        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), clusterName);

        // Verify upgrade
        verifyProcedure(testStorage.getNamespaceName(), upgradeDowngradeData, testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName(), wasUTOUsedBefore);
    }

    protected String downloadExamplesAndGetPath(CommonVersionModificationData versionModificationData) throws IOException {
        if (versionModificationData.getToUrl().equals("HEAD")) {
            return PATH_TO_PACKAGING;
        } else {
            File dir = FileUtils.downloadAndUnzip(versionModificationData.getToUrl());
            return dir.getAbsolutePath() + "/" + versionModificationData.getToExamples();
        }
    }

    protected void cleanUpKafkaTopics(String componentsNamespaceName) {
        List<KafkaTopic> topics = KafkaTopicResource.kafkaTopicClient().inNamespace(componentsNamespaceName).list().getItems();
        boolean finalizersAreSet = topics.stream().anyMatch(kafkaTopic -> kafkaTopic.getFinalizers() != null);

        // in case that we are upgrading/downgrading from UTO to BTO, we have to set finalizers on topics to null before deleting them
        if (!StUtils.isUnidirectionalTopicOperatorUsed(componentsNamespaceName, eoSelector) && finalizersAreSet) {
            KafkaTopicUtils.setFinalizersInAllTopicsToNull(componentsNamespaceName);
        }

        // delete all topics created in test
        cmdKubeClient(componentsNamespaceName).deleteAllByResource(KafkaTopic.RESOURCE_KIND);
        KafkaTopicUtils.waitForTopicWithPrefixDeletion(componentsNamespaceName, topicName);
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
}
