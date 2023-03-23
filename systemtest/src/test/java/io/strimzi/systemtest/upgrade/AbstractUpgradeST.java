/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.connect.build.JarArtifactBuilder;
import io.strimzi.api.kafka.model.connect.build.Plugin;
import io.strimzi.api.kafka.model.connect.build.PluginBuilder;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import static io.strimzi.systemtest.Constants.DEFAULT_SINK_FILE_PATH;
import static io.strimzi.systemtest.Constants.PATH_TO_KAFKA_TOPIC_CONFIG;
import static io.strimzi.systemtest.Constants.PATH_TO_PACKAGING_EXAMPLES;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

@IsolatedSuite
public class AbstractUpgradeST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(AbstractUpgradeST.class);

    protected File dir = null;
    protected File coDir = null;
    protected File kafkaTopicYaml = null;
    protected File kafkaUserYaml = null;
    protected File kafkaConnectYaml;

    protected final String clusterName = "my-cluster";

    protected Map<String, String> zkPods;
    protected Map<String, String> kafkaPods;
    protected Map<String, String> eoPods;
    protected Map<String, String> coPods;
    protected Map<String, String> connectPods;

    protected final LabelSelector kafkaSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName));
    protected final LabelSelector zkSelector = KafkaResource.getLabelSelector(clusterName, KafkaResources.zookeeperStatefulSetName(clusterName));
    protected final LabelSelector connectLabelSelector = KafkaConnectResource.getLabelSelector(clusterName, KafkaConnectResources.deploymentName(clusterName));

    protected final String topicName = "my-topic";
    protected final String userName = "my-user";
    protected final int upgradeTopicCount = 40;
    // ExpectedTopicCount contains additionally consumer-offset topic, my-topic and continuous-topic
    protected final int expectedTopicCount = upgradeTopicCount + 3;
    protected File kafkaYaml;

    protected void makeSnapshots() {
        coPods = DeploymentUtils.depSnapshot(clusterOperator.getDeploymentNamespace(), ResourceManager.getCoDeploymentName());
        zkPods = PodUtils.podSnapshot(clusterOperator.getDeploymentNamespace(), zkSelector);
        kafkaPods = PodUtils.podSnapshot(clusterOperator.getDeploymentNamespace(), kafkaSelector);
        eoPods = DeploymentUtils.depSnapshot(clusterOperator.getDeploymentNamespace(), KafkaResources.entityOperatorDeploymentName(clusterName));
        connectPods = PodUtils.podSnapshot(clusterOperator.getDeploymentNamespace(), connectLabelSelector);
    }

    @SuppressWarnings("CyclomaticComplexity")
    protected void changeKafkaAndLogFormatVersion(CommonVersionModificationData versionModificationData, ExtensionContext extensionContext) throws IOException {
        // Get Kafka configurations
        String operatorVersion = versionModificationData.getToVersion();
        String currentLogMessageFormat = cmdKubeClient().getResourceJsonPath(getResourceApiVersion(Kafka.RESOURCE_PLURAL, operatorVersion), clusterName, ".spec.kafka.config.log\\.message\\.format\\.version");
        String currentInterBrokerProtocol = cmdKubeClient().getResourceJsonPath(getResourceApiVersion(Kafka.RESOURCE_PLURAL, operatorVersion), clusterName, ".spec.kafka.config.inter\\.broker\\.protocol\\.version");
        // Get Kafka version
        String kafkaVersionFromCR = cmdKubeClient().getResourceJsonPath(getResourceApiVersion(Kafka.RESOURCE_PLURAL, operatorVersion), clusterName, ".spec.kafka.version");
        kafkaVersionFromCR = kafkaVersionFromCR.equals("") ? null : kafkaVersionFromCR;
        String kafkaVersionFromProcedure = versionModificationData.getProcedures().getVersion();

        // #######################################################################
        // #################    Update CRs to latest version   ###################
        // #######################################################################
        String examplesPath = "";
        if (versionModificationData.getToUrl().equals("HEAD")) {
            examplesPath = PATH_TO_PACKAGING_EXAMPLES + "";
        } else {
            File dir = FileUtils.downloadAndUnzip(versionModificationData.getToUrl());
            examplesPath = dir.getAbsolutePath() + "/" + versionModificationData.getToExamples() + "/examples";
        }

        kafkaYaml = new File(examplesPath + "/kafka/kafka-persistent.yaml");
        LOGGER.info("Deploying Kafka from: {}", kafkaYaml.getPath());
        // Change kafka version of it's empty (null is for remove the version)
        String defaultValueForVersions = kafkaVersionFromCR == null ? null : TestKafkaVersion.getSpecificVersion(kafkaVersionFromCR).messageVersion();
        cmdKubeClient().applyContent(KafkaUtils.changeOrRemoveKafkaConfiguration(kafkaYaml, kafkaVersionFromCR, defaultValueForVersions, defaultValueForVersions));

        kafkaUserYaml = new File(examplesPath + "/user/kafka-user.yaml");
        LOGGER.info("Deploying KafkaUser from: {}", kafkaUserYaml.getPath());
        cmdKubeClient().applyContent(KafkaUserUtils.removeKafkaUserPart(kafkaUserYaml, "authorization"));

        kafkaTopicYaml = new File(examplesPath + "/topic/kafka-topic.yaml");
        LOGGER.info("Deploying KafkaTopic from: {}", kafkaTopicYaml.getPath());
        cmdKubeClient().applyContent(TestUtils.readFile(kafkaTopicYaml));
        // #######################################################################


        if (versionModificationData.getProcedures() != null && (!currentLogMessageFormat.isEmpty() || !currentInterBrokerProtocol.isEmpty())) {
            if (kafkaVersionFromProcedure != null && !kafkaVersionFromProcedure.isEmpty() && !kafkaVersionFromCR.contains(kafkaVersionFromProcedure) && extensionContext.getTestClass().get().getSimpleName().toLowerCase(Locale.ROOT).contains("upgrade")) {
                LOGGER.info("Set Kafka version to " + kafkaVersionFromProcedure);
                cmdKubeClient().patchResource(getResourceApiVersion(Kafka.RESOURCE_PLURAL, operatorVersion), clusterName, "/spec/kafka/version", kafkaVersionFromProcedure);
                LOGGER.info("Wait until Kafka rolling update is finished");
                kafkaPods = RollingUpdateUtils.waitTillComponentHasRolled(clusterOperator.getDeploymentNamespace(), kafkaSelector, 3, kafkaPods);
            }

            String logMessageVersion = versionModificationData.getProcedures().getLogMessageVersion();
            String interBrokerProtocolVersion = versionModificationData.getProcedures().getInterBrokerVersion();

            if (logMessageVersion != null && !logMessageVersion.isEmpty() || interBrokerProtocolVersion != null && !interBrokerProtocolVersion.isEmpty()) {
                if (!logMessageVersion.isEmpty()) {
                    LOGGER.info("Set log message format version to {} (current version is {})", logMessageVersion, currentLogMessageFormat);
                    cmdKubeClient().patchResource(getResourceApiVersion(Kafka.RESOURCE_PLURAL, operatorVersion), clusterName, "/spec/kafka/config/log.message.format.version", logMessageVersion);
                }

                if (!interBrokerProtocolVersion.isEmpty()) {
                    LOGGER.info("Set inter-broker protocol version to {} (current version is {})", interBrokerProtocolVersion, currentInterBrokerProtocol);
                    LOGGER.info("Set inter-broker protocol version to " + interBrokerProtocolVersion);
                    cmdKubeClient().patchResource(getResourceApiVersion(Kafka.RESOURCE_PLURAL, operatorVersion), clusterName, "/spec/kafka/config/inter.broker.protocol.version", interBrokerProtocolVersion);
                }

                if ((currentInterBrokerProtocol != null && !currentInterBrokerProtocol.equals(interBrokerProtocolVersion)) ||
                        (currentLogMessageFormat != null && !currentLogMessageFormat.isEmpty() && !currentLogMessageFormat.equals(logMessageVersion))) {
                    LOGGER.info("Wait until Kafka rolling update is finished");
                    kafkaPods = RollingUpdateUtils.waitTillComponentHasRolled(clusterOperator.getDeploymentNamespace(), kafkaSelector, 3, kafkaPods);
                }
                makeSnapshots();
            }

            if (kafkaVersionFromProcedure != null && !kafkaVersionFromProcedure.isEmpty() && !kafkaVersionFromCR.contains(kafkaVersionFromProcedure) && extensionContext.getTestClass().get().getSimpleName().toLowerCase(Locale.ROOT).contains("downgrade")) {
                LOGGER.info("Set Kafka version to " + kafkaVersionFromProcedure);
                cmdKubeClient().patchResource(getResourceApiVersion(Kafka.RESOURCE_PLURAL, operatorVersion), clusterName, "/spec/kafka/version", kafkaVersionFromProcedure);
                LOGGER.info("Wait until Kafka rolling update is finished");
                kafkaPods = RollingUpdateUtils.waitTillComponentHasRolled(clusterOperator.getDeploymentNamespace(), kafkaSelector, kafkaPods);
            }
        }
    }

    protected void logPodImages(String clusterName) {
        List<Pod> pods = kubeClient().listPods(KafkaResource.getLabelSelector(clusterName, KafkaResources.zookeeperStatefulSetName(clusterName)));
        for (Pod pod : pods) {
            LOGGER.info("Pod {} has image {}", pod.getMetadata().getName(), pod.getSpec().getContainers().get(0).getImage());
        }
        pods = kubeClient().listPods(KafkaResource.getLabelSelector(clusterName, KafkaResources.kafkaStatefulSetName(clusterName)));
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
        zkPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(clusterOperator.getDeploymentNamespace(), zkSelector, 3, zkPods);
        LOGGER.info("Waiting for Kafka StatefulSet roll");
        kafkaPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(clusterOperator.getDeploymentNamespace(), kafkaSelector, 3, kafkaPods);
        LOGGER.info("Waiting for EO Deployment roll");
        // Check the TO and UO also got upgraded
        eoPods = DeploymentUtils.waitTillDepHasRolled(clusterOperator.getDeploymentNamespace(), KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoPods);
    }

    protected void waitForReadinessOfKafkaCluster() {
        LOGGER.info("Waiting for Zookeeper StatefulSet");
        RollingUpdateUtils.waitForComponentAndPodsReady(clusterOperator.getDeploymentNamespace(), zkSelector, 3);
        LOGGER.info("Waiting for Kafka StatefulSet");
        RollingUpdateUtils.waitForComponentAndPodsReady(clusterOperator.getDeploymentNamespace(), kafkaSelector, 3);
        LOGGER.info("Waiting for EO Deployment");
        DeploymentUtils.waitForDeploymentAndPodsReady(clusterOperator.getDeploymentNamespace(), KafkaResources.entityOperatorDeploymentName(clusterName), 1);
    }

    protected void  changeClusterOperator(BundleVersionModificationData versionModificationData, String namespace, ExtensionContext extensionContext) throws IOException {
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

        copyModifyApply(coDir, namespace, extensionContext, versionModificationData.getFeatureGatesAfter());

        LOGGER.info("Waiting for CO upgrade");
        DeploymentUtils.waitTillDepHasRolled(namespace, ResourceManager.getCoDeploymentName(), 1, coPods);
    }

    protected void copyModifyApply(File root, String namespace, ExtensionContext extensionContext, final String strimziFeatureGatesValue) {
        Arrays.stream(Objects.requireNonNull(root.listFiles())).sorted().forEach(f -> {
            if (f.getName().matches(".*RoleBinding.*")) {
                cmdKubeClient().replaceContent(TestUtils.changeRoleBindingSubject(f, namespace));
            } else if (f.getName().matches(".*Deployment.*")) {
                cmdKubeClient().replaceContent(StUtils.changeDeploymentConfiguration(f, namespace, strimziFeatureGatesValue));
            } else {
                cmdKubeClient().replaceContent(TestUtils.getContent(f, TestUtils::toYamlString));
            }
        });
        // Set info that CO is already installed
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(io.strimzi.systemtest.Constants.PREPARE_OPERATOR_ENV_KEY + namespace, false);
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

    protected void checkAllImages(BundleVersionModificationData versionModificationData, String namespaceName) {
        if (versionModificationData.getImagesAfterOperations().isEmpty()) {
            fail("There are no expected images");
        }

        Map<String, String> zkSelector = Labels.EMPTY
                .withStrimziKind(Kafka.RESOURCE_KIND)
                .withStrimziCluster(clusterName)
                .withStrimziName(KafkaResources.zookeeperStatefulSetName(clusterName))
                .toMap();

        Map<String, String> kafkaSelector = Labels.EMPTY
                .withStrimziKind(Kafka.RESOURCE_KIND)
                .withStrimziCluster(clusterName)
                .withStrimziName(KafkaResources.kafkaStatefulSetName(clusterName))
                .toMap();

        checkContainerImages(zkSelector, versionModificationData.getZookeeperImage());
        checkContainerImages(kafkaSelector, versionModificationData.getKafkaImage());
        checkContainerImages(kubeClient().getDeployment(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName)).getSpec().getSelector().getMatchLabels(), versionModificationData.getTopicOperatorImage());
        checkContainerImages(kubeClient().getDeployment(namespaceName, KafkaResources.entityOperatorDeploymentName(clusterName)).getSpec().getSelector().getMatchLabels(), 1, versionModificationData.getUserOperatorImage());
    }

    protected void checkContainerImages(Map<String, String> matchLabels, String image) {
        checkContainerImages(matchLabels, 0, image);
    }

    protected void checkContainerImages(Map<String, String> matchLabels, int container, String image) {
        List<Pod> pods1 = kubeClient().listPods(matchLabels);
        for (Pod pod : pods1) {
            if (!image.equals(pod.getSpec().getContainers().get(container).getImage())) {
                LOGGER.debug("Expected image for pod {}: {} \nCurrent image: {}", pod.getMetadata().getName(), image, pod.getSpec().getContainers().get(container).getImage());
                assertThat("Used image for pod " + pod.getMetadata().getName() + " is not valid!", pod.getSpec().getContainers().get(container).getImage(), containsString(image));
            }
        }
    }

    protected void setupEnvAndUpgradeClusterOperator(ExtensionContext extensionContext, BundleVersionModificationData upgradeData, TestStorage testStorage, UpgradeKafkaVersion upgradeKafkaVersion, String namespace) throws IOException {
        LOGGER.info("Test upgrade of ClusterOperator from version {} to version {}", upgradeData.getFromVersion(), upgradeData.getToVersion());
        cluster.setNamespace(namespace);

        String operatorVersion = upgradeData.getFromVersion();

        this.deployCoWithWaitForReadiness(extensionContext, upgradeData, namespace);
        this.deployKafkaClusterWithWaitForReadiness(extensionContext, upgradeData, upgradeKafkaVersion);
        this.deployKafkaUserWithWaitForReadiness(extensionContext, upgradeData, namespace);
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
            if (!cmdKubeClient().getResources(getResourceApiVersion(KafkaTopic.RESOURCE_PLURAL, operatorVersion)).contains(testStorage.getTopicName())) {
                String pathToTopicExamples = upgradeData.getFromExamples().equals("HEAD") ? PATH_TO_KAFKA_TOPIC_CONFIG : upgradeData.getFromExamples() + "/examples/topic/kafka-topic.yaml";

                kafkaTopicYaml = new File(dir, pathToTopicExamples);
                cmdKubeClient().applyContent(TestUtils.getContent(kafkaTopicYaml, TestUtils::toYamlString)
                        .replace("name: \"my-topic\"", "name: \"" + testStorage.getTopicName() + "\"")
                        .replace("partitions: 1", "partitions: 3")
                        .replace("replicas: 1", "replicas: 3") +
                        "    min.insync.replicas: 2");

                ResourceManager.waitForResourceReadiness(getResourceApiVersion(KafkaTopic.RESOURCE_PLURAL, operatorVersion), testStorage.getTopicName());
            }

            String producerAdditionConfiguration = "delivery.timeout.ms=20000\nrequest.timeout.ms=20000";

            KafkaClients kafkaBasicClientJob = new KafkaClientsBuilder()
                .withProducerName(testStorage.getProducerName())
                .withConsumerName(testStorage.getConsumerName())
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(clusterName))
                .withTopicName(testStorage.getTopicName())
                .withMessageCount(upgradeData.getContinuousClientsMessages())
                .withAdditionalConfig(producerAdditionConfiguration)
                .withDelayMs(1000)
                .build();

            resourceManager.createResource(extensionContext, kafkaBasicClientJob.producerStrimzi());
            resourceManager.createResource(extensionContext, kafkaBasicClientJob.consumerStrimzi());
            // ##############################
        }

        makeSnapshots();
        logPodImages(clusterName);
    }

    protected void verifyProcedure(BundleVersionModificationData upgradeData, String producerName, String consumerName, String namespace) {

        if (upgradeData.getAdditionalTopics() != null) {
            // Check that topics weren't deleted/duplicated during upgrade procedures
            String listedTopics = cmdKubeClient().getResources(getResourceApiVersion(KafkaTopic.RESOURCE_PLURAL));
            int additionalTopics = upgradeData.getAdditionalTopics();
            assertThat("KafkaTopic list doesn't have expected size", Long.valueOf(listedTopics.lines().count() - 1).intValue(), is(expectedTopicCount + additionalTopics));
            assertThat("KafkaTopic " + topicName + " is not in expected topic list",
                    listedTopics.contains(topicName), is(true));
            for (int x = 0; x < upgradeTopicCount; x++) {
                assertThat("KafkaTopic " + topicName + "-" + x + " is not in expected topic list", listedTopics.contains(topicName + "-" + x), is(true));
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
        return getResourceApiVersion(resourcePlural, "HEAD");
    }

    protected String getResourceApiVersion(String resourcePlural, String coVersion) {
        if (coVersion.equals("HEAD") || TestKafkaVersion.compareDottedVersions(coVersion, "0.22.0") >= 0) {
            return resourcePlural + "." + Constants.V1BETA2 + "." + Constants.RESOURCE_GROUP_NAME;
        } else {
            return resourcePlural + "." + Constants.V1BETA1 + "." + Constants.RESOURCE_GROUP_NAME;
        }
    }

    protected void deployCoWithWaitForReadiness(final ExtensionContext extensionContext, final BundleVersionModificationData upgradeData,
                                                final String namespaceName) throws IOException {
        if (upgradeData.getFromVersion().equals("HEAD")) {
            coDir = new File(TestUtils.USER_PATH + "/../packaging/install/cluster-operator");
        } else {
            final String url = upgradeData.getFromUrl();
            dir = FileUtils.downloadAndUnzip(url);
            coDir = new File(dir, upgradeData.getFromExamples() + "/install/cluster-operator/");
        }

        // Modify + apply installation files
        copyModifyApply(coDir, namespaceName, extensionContext, upgradeData.getFeatureGatesBefore());

        LOGGER.info("Waiting for {} deployment", ResourceManager.getCoDeploymentName());
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceName, ResourceManager.getCoDeploymentName(), 1);
        LOGGER.info("{} is ready", ResourceManager.getCoDeploymentName());
    }


    protected void deployKafkaClusterWithWaitForReadiness(final ExtensionContext extensionContext, final BundleVersionModificationData upgradeData,
                                                          final UpgradeKafkaVersion upgradeKafkaVersion) {
        if (!cmdKubeClient().getResources(getResourceApiVersion(Kafka.RESOURCE_PLURAL, upgradeData.getFromVersion())).contains(clusterName)) {
            // Deploy a Kafka cluster
            if (upgradeData.getFromExamples().equals("HEAD")) {
                resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 3)
                    .editSpec()
                        .editKafka()
                            .withVersion(upgradeKafkaVersion.getVersion())
                            .addToConfig("log.message.format.version", upgradeKafkaVersion.getLogMessageVersion())
                            .addToConfig("inter.broker.protocol.version", upgradeKafkaVersion.getInterBrokerVersion())
                        .endKafka()
                    .endSpec()
                    .build());
            } else {
                kafkaYaml = new File(dir, upgradeData.getFromExamples() + "/examples/kafka/kafka-persistent.yaml");
                LOGGER.info("Deploy Kafka from: {}", kafkaYaml.getPath());
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

    protected void deployKafkaUserWithWaitForReadiness(final ExtensionContext extensionContext, final BundleVersionModificationData upgradeData,
                                                       final String namespaceName) {
        if (!cmdKubeClient().getResources(getResourceApiVersion(KafkaUser.RESOURCE_PLURAL, upgradeData.getFromVersion())).contains(userName)) {
            if (upgradeData.getFromVersion().equals("HEAD")) {
                resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(namespaceName, clusterName, userName).build());
            } else {
                kafkaUserYaml = new File(dir, upgradeData.getFromExamples() + "/examples/user/kafka-user.yaml");
                LOGGER.info("Deploy KafkaUser from: {}", kafkaUserYaml.getPath());
                cmdKubeClient().applyContent(KafkaUserUtils.removeKafkaUserPart(kafkaUserYaml, "authorization"));
                ResourceManager.waitForResourceReadiness(getResourceApiVersion(KafkaUser.RESOURCE_PLURAL, upgradeData.getFromVersion()), userName);
            }
        }
    }

    protected void deployKafkaTopicWithWaitForReadiness(final BundleVersionModificationData upgradeData) {
        if (!cmdKubeClient().getResources(getResourceApiVersion(KafkaTopic.RESOURCE_PLURAL, upgradeData.getFromVersion())).contains(topicName)) {
            if (upgradeData.getFromVersion().equals("HEAD")) {
                kafkaTopicYaml = new File(dir, PATH_TO_PACKAGING_EXAMPLES + "/topic/kafka-topic.yaml");
            } else {
                kafkaTopicYaml = new File(dir, upgradeData.getFromExamples() + "/examples/topic/kafka-topic.yaml");
            }
            LOGGER.info("Deploy KafkaTopic from: {}", kafkaTopicYaml.getPath());
            cmdKubeClient().create(kafkaTopicYaml);
            ResourceManager.waitForResourceReadiness(getResourceApiVersion(KafkaTopic.RESOURCE_PLURAL, upgradeData.getFromVersion()), topicName);
        }
    }

    protected void deployKafkaConnectAndKafkaConnectorWithWaitForReadiness(final ExtensionContext extensionContext,
                                                                            final BundleVersionModificationData acrossUpgradeData,
                                                                            final TestStorage testStorage) {
        // setup KafkaConnect + KafkaConnector
        if (!cmdKubeClient().getResources(getResourceApiVersion(KafkaConnect.RESOURCE_PLURAL, acrossUpgradeData.getFromVersion())).contains(clusterName)) {
            if (acrossUpgradeData.getFromVersion().equals("HEAD")) {
                resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnectWithFilePlugin(clusterName, testStorage.getNamespaceName(), 1)
                    .editMetadata()
                        .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                    .endMetadata()
                    .editSpec()
                        .addToConfig("key.converter.schemas.enable", false)
                        .addToConfig("value.converter.schemas.enable", false)
                        .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                        .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .endSpec()
                    .build());
                resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(clusterName)
                    .editMetadata()
                        .withNamespace(testStorage.getNamespaceName())
                    .endMetadata()
                    .editSpec()
                        .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                        .addToConfig("topics", testStorage.getTopicName())
                        .addToConfig("file", io.strimzi.systemtest.Constants.DEFAULT_SINK_FILE_PATH)
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

                final String imageName = Environment.getImageOutputRegistry() + "/" + testStorage.getNamespaceName() + "/connect-" + Util.hashStub(String.valueOf(new Random().nextInt(Integer.MAX_VALUE))) + ":latest";

                KafkaConnect kafkaConnect = new KafkaConnectBuilder(TestUtils.configFromYaml(kafkaConnectYaml, KafkaConnect.class))
                    .editMetadata()
                        .withName(clusterName)
                        .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                    .endMetadata()
                    .editSpec()
                        .editOrNewBuild()
                            .withPlugins(fileSinkPlugin)
                            .withNewDockerOutput()
                                .withImage(imageName)
                            .endDockerOutput()
                        .endBuild()
                        .addToConfig("key.converter.schemas.enable", false)
                        .addToConfig("value.converter.schemas.enable", false)
                        .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                        .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .endSpec()
                    .build();

                LOGGER.info("Deploy KafkaConnect from: {}", kafkaConnectYaml.getPath());

                cmdKubeClient().applyContent(TestUtils.toYamlString(kafkaConnect));
                ResourceManager.waitForResourceReadiness(getResourceApiVersion(KafkaConnect.RESOURCE_PLURAL, acrossUpgradeData.getFromVersion()), kafkaConnect.getMetadata().getName());

                // in our examples is no sink connector and thus we are using the same as in HEAD verification
                resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(clusterName)
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

    protected void doKafkaConnectAndKafkaConnectorUpgradeOrDowngradeProcedure(final ExtensionContext extensionContext,
                                                                              final BundleVersionModificationData bundleDowngradeDataWithFeatureGates,
                                                                              final TestStorage testStorage,
                                                                              final UpgradeKafkaVersion upgradeKafkaVersion) throws IOException {
        this.deployCoWithWaitForReadiness(extensionContext, bundleDowngradeDataWithFeatureGates, testStorage.getNamespaceName());
        this.deployKafkaClusterWithWaitForReadiness(extensionContext, bundleDowngradeDataWithFeatureGates, upgradeKafkaVersion);
        this.deployKafkaConnectAndKafkaConnectorWithWaitForReadiness(extensionContext, bundleDowngradeDataWithFeatureGates, testStorage);
        this.deployKafkaUserWithWaitForReadiness(extensionContext, bundleDowngradeDataWithFeatureGates, testStorage.getNamespaceName());

        final KafkaClients clients = new KafkaClientsBuilder()
                .withProducerName(testStorage.getProducerName())
                .withConsumerName(testStorage.getConsumerName())
                .withBootstrapAddress(KafkaResources.tlsBootstrapAddress(clusterName))
                .withTopicName(testStorage.getTopicName())
                .withUserName(userName)
                .withMessageCount(500)
                .withNamespaceName(testStorage.getNamespaceName())
                .build();

        resourceManager.createResource(extensionContext, clients.producerTlsStrimzi(clusterName));
        // Verify that Producer finish successfully
        ClientUtils.waitForProducerClientSuccess(testStorage);

        makeSnapshots();
        logPodImages(clusterName);

        // Verify FileSink KafkaConnector before upgrade
        String connectorPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), clusterName + "-connect").get(0).getMetadata().getName();
        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(testStorage.getNamespaceName(), connectorPodName, DEFAULT_SINK_FILE_PATH, "\"Hello-world - 499\"");

        // Upgrade CO to HEAD and wait for readiness of ClusterOperator
        changeClusterOperator(bundleDowngradeDataWithFeatureGates, testStorage.getNamespaceName(), extensionContext);

        // Verify that Kafka cluster RU
        waitForKafkaClusterRollingUpdate();
        // Verify that KafkaConnect pods are rolling and KafkaConnector is ready
        RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), connectLabelSelector, 1, connectPods);
        KafkaConnectorUtils.waitForConnectorReady(testStorage.getNamespaceName(), clusterName);

        // send again new messages
        resourceManager.createResource(extensionContext, clients.producerTlsStrimzi(clusterName));
        // Verify that Producer finish successfully
        ClientUtils.waitForProducerClientSuccess(testStorage);
        // Verify FileSink KafkaConnector
        connectorPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), clusterName + "-connect").get(0).getMetadata().getName();
        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(testStorage.getNamespaceName(), connectorPodName, DEFAULT_SINK_FILE_PATH, "\"Hello-world - 499\"");

        // Verify that pods are stable
        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), clusterName);
        // Check errors in CO log
        assertNoCoErrorsLogged(testStorage.getNamespaceName(), 0);
    }
}
