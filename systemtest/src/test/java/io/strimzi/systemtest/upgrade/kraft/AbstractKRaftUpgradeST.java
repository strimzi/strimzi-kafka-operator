/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade.kraft;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.resources.NamespaceManager;
import io.strimzi.systemtest.resources.crd.KafkaNodePoolResource;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.upgrade.AbstractUpgradeST;
import io.strimzi.systemtest.upgrade.BundleVersionModificationData;
import io.strimzi.systemtest.upgrade.CommonVersionModificationData;
import io.strimzi.systemtest.upgrade.UpgradeKafkaVersion;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.ReadWriteUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static io.strimzi.systemtest.Environment.TEST_SUITE_NAMESPACE;
import static io.strimzi.systemtest.TestConstants.CO_NAMESPACE;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static org.junit.jupiter.api.Assertions.fail;

public class AbstractKRaftUpgradeST extends AbstractUpgradeST {

    private static final Logger LOGGER = LogManager.getLogger(AbstractKRaftUpgradeST.class);

    protected Map<String, String> brokerPods;
    protected Map<String, String> controllerPods;

    protected static final String CONTROLLER_NODE_NAME = "controller";
    protected static final String BROKER_NODE_NAME = "broker";

    protected final LabelSelector controllerSelector = KafkaNodePoolResource.getLabelSelector(clusterName, CONTROLLER_NODE_NAME, ProcessRoles.CONTROLLER);
    protected final LabelSelector brokerSelector = KafkaNodePoolResource.getLabelSelector(clusterName, BROKER_NODE_NAME, ProcessRoles.BROKER);

    @Override
    protected void makeComponentsSnapshots(String componentsNamespaceName) {
        eoPods = DeploymentUtils.depSnapshot(componentsNamespaceName, KafkaResources.entityOperatorDeploymentName(clusterName));
        controllerPods = PodUtils.podSnapshot(componentsNamespaceName, controllerSelector);
        brokerPods = PodUtils.podSnapshot(componentsNamespaceName, brokerSelector);
        connectPods = PodUtils.podSnapshot(componentsNamespaceName, connectLabelSelector);
    }

    @Override
    protected void deployKafkaClusterWithWaitForReadiness(final String componentsNamespaceName,
                                                          final BundleVersionModificationData upgradeData,
                                                          final UpgradeKafkaVersion upgradeKafkaVersion) {
        LOGGER.info("Deploying Kafka: {}/{}", componentsNamespaceName, clusterName);

        if (!cmdKubeClient(componentsNamespaceName).getResources(getResourceApiVersion(Kafka.RESOURCE_PLURAL)).contains(clusterName)) {
            // Deploy a Kafka cluster
            if (upgradeData.getFromExamples().equals("HEAD")) {
                resourceManager.createResourceWithWait(
                    KafkaNodePoolTemplates.controllerPoolPersistentStorage(componentsNamespaceName, CONTROLLER_NODE_NAME, clusterName, 3).build(),
                    KafkaNodePoolTemplates.brokerPoolPersistentStorage(componentsNamespaceName, BROKER_NODE_NAME, clusterName, 3).build(),
                    KafkaTemplates.kafkaPersistentKRaft(componentsNamespaceName, clusterName, 3)
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
                kafkaYaml = new File(dir, upgradeData.getFromExamples() + upgradeData.getKafkaKRaftFilePathBefore());
                LOGGER.info("Deploying Kafka from: {}", kafkaYaml.getPath());
                // Change kafka version of it's empty (null is for remove the version)
                if (upgradeKafkaVersion == null) {
                    cmdKubeClient(componentsNamespaceName).applyContent(KafkaUtils.changeOrRemoveKafkaInKRaft(kafkaYaml, null));
                } else {
                    cmdKubeClient(componentsNamespaceName).applyContent(KafkaUtils.changeOrRemoveKafkaConfigurationInKRaft(kafkaYaml, upgradeKafkaVersion.getVersion(), upgradeKafkaVersion.getMetadataVersion()));
                }
                // Wait for readiness
                waitForReadinessOfKafkaCluster(componentsNamespaceName);
            }
        }
    }

    @Override
    protected void waitForKafkaClusterRollingUpdate(final String componentsNamespaceName) {
        LOGGER.info("Waiting for Kafka Pods with controller role to be rolled");
        controllerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(componentsNamespaceName, controllerSelector, 3, controllerPods);
        LOGGER.info("Waiting for Kafka Pods with broker role to be rolled");
        brokerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(componentsNamespaceName, brokerSelector, 3, brokerPods);
        LOGGER.info("Waiting for EO Deployment to be rolled");
        // Check the TO and UO also got upgraded
        eoPods = DeploymentUtils.waitTillDepHasRolled(componentsNamespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoPods);
    }

    @Override
    protected void waitForReadinessOfKafkaCluster(final String componentsNamespaceName) {
        LOGGER.info("Waiting for Kafka Pods with controller role to be ready");
        RollingUpdateUtils.waitForComponentAndPodsReady(componentsNamespaceName, controllerSelector, 3);
        LOGGER.info("Waiting for Kafka Pods with broker role to be ready");
        RollingUpdateUtils.waitForComponentAndPodsReady(componentsNamespaceName, brokerSelector, 3);
        LOGGER.info("Waiting for EO Deployment");
        DeploymentUtils.waitForDeploymentAndPodsReady(componentsNamespaceName, KafkaResources.entityOperatorDeploymentName(clusterName), 1);
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
        String kafkaVersionFromCR = cmdKubeClient(componentsNamespaceName).getResourceJsonPath(getResourceApiVersion(Kafka.RESOURCE_PLURAL), clusterName, ".spec.kafka.version");
        kafkaVersionFromCR = kafkaVersionFromCR.equals("") ? null : kafkaVersionFromCR;
        // Get Kafka metadata version
        String currentMetadataVersion = cmdKubeClient(componentsNamespaceName).getResourceJsonPath(getResourceApiVersion(Kafka.RESOURCE_PLURAL), clusterName, ".spec.kafka.metadataVersion");

        String kafkaVersionFromProcedure = versionModificationData.getProcedures().getVersion();

        // #######################################################################
        // #################    Update CRs to latest version   ###################
        // #######################################################################
        String examplesPath = downloadExamplesAndGetPath(versionModificationData);
        String kafkaFilePath = examplesPath + versionModificationData.getKafkaKRaftFilePathAfter();

        applyCustomResourcesFromPath(componentsNamespaceName, examplesPath, kafkaFilePath, kafkaVersionFromCR, currentMetadataVersion);

        // #######################################################################

        if (versionModificationData.getProcedures() != null && (!currentMetadataVersion.isEmpty() || replaceEvenIfMissing)) {

            if (kafkaVersionFromProcedure != null && !kafkaVersionFromProcedure.isEmpty() && !kafkaVersionFromCR.contains(kafkaVersionFromProcedure)) {
                LOGGER.info("Set Kafka version to " + kafkaVersionFromProcedure);
                cmdKubeClient(componentsNamespaceName).patchResource(getResourceApiVersion(Kafka.RESOURCE_PLURAL), clusterName, "/spec/kafka/version", kafkaVersionFromProcedure);

                waitForKafkaControllersAndBrokersFinishRollingUpdate(componentsNamespaceName);
            }

            String metadataVersion = versionModificationData.getProcedures().getMetadataVersion();

            if (metadataVersion != null && !metadataVersion.isEmpty()) {
                LOGGER.info("Set metadata version to {} (current version is {})", metadataVersion, currentMetadataVersion);
                cmdKubeClient(componentsNamespaceName).patchResource(getResourceApiVersion(Kafka.RESOURCE_PLURAL), clusterName, "/spec/kafka/metadataVersion", metadataVersion);

                makeComponentsSnapshots(componentsNamespaceName);
            }
        }
    }

    @Override
    protected void checkAllComponentsImages(String componentsNamespaceName, BundleVersionModificationData versionModificationData) {
        if (versionModificationData.getImagesAfterOperations().isEmpty()) {
            fail("There are no expected images");
        }

        checkContainerImages(componentsNamespaceName, controllerSelector, versionModificationData.getKafkaImage());
        checkContainerImages(componentsNamespaceName, brokerSelector, versionModificationData.getKafkaImage());
        checkContainerImages(componentsNamespaceName, eoSelector, versionModificationData.getTopicOperatorImage());
        checkContainerImages(componentsNamespaceName, eoSelector, 1, versionModificationData.getUserOperatorImage());
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

    protected void waitForKafkaControllersAndBrokersFinishRollingUpdate(String componentsNamespaceName) {
        LOGGER.info("Waiting for Kafka rolling update to finish");
        controllerPods = RollingUpdateUtils.waitTillComponentHasRolled(componentsNamespaceName, controllerSelector, 3, controllerPods);
        brokerPods = RollingUpdateUtils.waitTillComponentHasRolled(componentsNamespaceName, brokerSelector, 3, brokerPods);
    }

    protected void applyKafkaCustomResourceFromPath(String kafkaFilePath, String kafkaVersionFromCR, String kafkaMetadataVersion) {
        // Change kafka version of it's empty (null is for remove the version)
        String metadataVersion = kafkaVersionFromCR == null ? null : kafkaMetadataVersion;

        kafkaYaml = new File(kafkaFilePath);
        LOGGER.info("Deploying Kafka from: {}", kafkaYaml.getPath());
        cmdKubeClient().applyContent(KafkaUtils.changeOrRemoveKafkaConfigurationInKRaft(kafkaYaml, kafkaVersionFromCR, metadataVersion));
    }

    protected void applyCustomResourcesFromPath(String namespaceName, String examplesPath, String kafkaFilePath, String kafkaVersionFromCR, String kafkaMetadataVersion) {
        applyKafkaCustomResourceFromPath(kafkaFilePath, kafkaVersionFromCR, kafkaMetadataVersion);

        kafkaUserYaml = new File(examplesPath + "/examples/user/kafka-user.yaml");
        LOGGER.info("Deploying KafkaUser from: {}, in Namespace: {}", kafkaUserYaml.getPath(), namespaceName);
        cmdKubeClient(namespaceName).applyContent(KafkaUserUtils.removeKafkaUserPart(kafkaUserYaml, "authorization"));

        kafkaTopicYaml = new File(examplesPath + "/examples/topic/kafka-topic.yaml");
        LOGGER.info("Deploying KafkaTopic from: {}, in Namespace {}", kafkaTopicYaml.getPath(), namespaceName);
        cmdKubeClient(namespaceName).applyContent(ReadWriteUtils.readFile(kafkaTopicYaml));
    }

    @BeforeEach
    void setupEnvironment() {
        NamespaceManager.getInstance().createNamespaceAndPrepare(CO_NAMESPACE);
        NamespaceManager.getInstance().createNamespaceAndPrepare(TEST_SUITE_NAMESPACE);
    }

    @AfterEach
    void afterEach() {
        cleanUpKafkaTopics(TEST_SUITE_NAMESPACE);
        deleteInstalledYamls(CO_NAMESPACE, TEST_SUITE_NAMESPACE, coDir);
        NamespaceManager.getInstance().deleteNamespaceWithWait(CO_NAMESPACE);
        NamespaceManager.getInstance().deleteNamespaceWithWait(TEST_SUITE_NAMESPACE);
    }
}
