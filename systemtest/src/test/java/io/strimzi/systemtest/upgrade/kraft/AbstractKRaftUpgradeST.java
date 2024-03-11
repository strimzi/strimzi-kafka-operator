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
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceManager;
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
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
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
    protected void makeSnapshots() {
        coPods = DeploymentUtils.depSnapshot(TestConstants.CO_NAMESPACE, ResourceManager.getCoDeploymentName());
        eoPods = DeploymentUtils.depSnapshot(TestConstants.CO_NAMESPACE, KafkaResources.entityOperatorDeploymentName(clusterName));
        controllerPods = PodUtils.podSnapshot(TestConstants.CO_NAMESPACE, controllerSelector);
        brokerPods = PodUtils.podSnapshot(TestConstants.CO_NAMESPACE, brokerSelector);
        connectPods = PodUtils.podSnapshot(TestConstants.CO_NAMESPACE, connectLabelSelector);
    }

    @Override
    protected void deployKafkaClusterWithWaitForReadiness(final BundleVersionModificationData upgradeData,
                                                          final UpgradeKafkaVersion upgradeKafkaVersion) {
        LOGGER.info("Deploying Kafka: {} in Namespace: {}", clusterName, kubeClient().getNamespace());

        if (!cmdKubeClient().getResources(getResourceApiVersion(Kafka.RESOURCE_PLURAL)).contains(clusterName)) {
            // Deploy a Kafka cluster
            if (upgradeData.getFromExamples().equals("HEAD")) {
                resourceManager.createResourceWithWait(
                    KafkaNodePoolTemplates.controllerPoolPersistentStorage(TestConstants.CO_NAMESPACE, CONTROLLER_NODE_NAME, clusterName, 3).build(),
                    KafkaNodePoolTemplates.brokerPoolPersistentStorage(TestConstants.CO_NAMESPACE, BROKER_NODE_NAME, clusterName, 3).build(),
                    KafkaTemplates.kafkaPersistentKRaft(clusterName, 3)
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
                    cmdKubeClient().applyContent(KafkaUtils.changeOrRemoveKafkaInKRaft(kafkaYaml, null));
                } else {
                    cmdKubeClient().applyContent(KafkaUtils.changeOrRemoveKafkaConfigurationInKRaft(kafkaYaml, upgradeKafkaVersion.getVersion(), upgradeKafkaVersion.getMetadataVersion()));
                }
                // Wait for readiness
                waitForReadinessOfKafkaCluster();
            }
        }
    }

    @Override
    protected void waitForKafkaClusterRollingUpdate() {
        LOGGER.info("Waiting for Kafka Pods with controller role to be rolled");
        controllerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(TestConstants.CO_NAMESPACE, controllerSelector, 3, controllerPods);
        LOGGER.info("Waiting for Kafka Pods with broker role to be rolled");
        brokerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(TestConstants.CO_NAMESPACE, brokerSelector, 3, brokerPods);
        LOGGER.info("Waiting for EO Deployment to be rolled");
        // Check the TO and UO also got upgraded
        eoPods = DeploymentUtils.waitTillDepHasRolled(TestConstants.CO_NAMESPACE, KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoPods);
    }

    @Override
    protected void waitForReadinessOfKafkaCluster() {
        LOGGER.info("Waiting for Kafka Pods with controller role to be ready");
        RollingUpdateUtils.waitForComponentAndPodsReady(TestConstants.CO_NAMESPACE, controllerSelector, 3);
        LOGGER.info("Waiting for Kafka Pods with broker role to be ready");
        RollingUpdateUtils.waitForComponentAndPodsReady(TestConstants.CO_NAMESPACE, brokerSelector, 3);
        LOGGER.info("Waiting for EO Deployment");
        DeploymentUtils.waitForDeploymentAndPodsReady(TestConstants.CO_NAMESPACE, KafkaResources.entityOperatorDeploymentName(clusterName), 1);
    }

    protected void changeKafkaAndMetadataVersion(CommonVersionModificationData versionModificationData) throws IOException {
        changeKafkaAndMetadataVersion(versionModificationData, false);
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
    protected void changeKafkaAndMetadataVersion(CommonVersionModificationData versionModificationData, boolean replaceEvenIfMissing) throws IOException {
        // Get Kafka version
        String kafkaVersionFromCR = cmdKubeClient().getResourceJsonPath(getResourceApiVersion(Kafka.RESOURCE_PLURAL), clusterName, ".spec.kafka.version");
        kafkaVersionFromCR = kafkaVersionFromCR.equals("") ? null : kafkaVersionFromCR;
        // Get Kafka metadata version
        String currentMetadataVersion = cmdKubeClient().getResourceJsonPath(getResourceApiVersion(Kafka.RESOURCE_PLURAL), clusterName, ".spec.kafka.metadataVersion");

        String kafkaVersionFromProcedure = versionModificationData.getProcedures().getVersion();

        // #######################################################################
        // #################    Update CRs to latest version   ###################
        // #######################################################################
        String examplesPath = downloadExamplesAndGetPath(versionModificationData);
        String kafkaFilePath = examplesPath + versionModificationData.getKafkaKRaftFilePathAfter();

        applyCustomResourcesFromPath(examplesPath, kafkaFilePath, kafkaVersionFromCR, currentMetadataVersion);

        // #######################################################################

        if (versionModificationData.getProcedures() != null && (!currentMetadataVersion.isEmpty() || replaceEvenIfMissing)) {

            if (kafkaVersionFromProcedure != null && !kafkaVersionFromProcedure.isEmpty() && !kafkaVersionFromCR.contains(kafkaVersionFromProcedure)) {
                LOGGER.info("Set Kafka version to " + kafkaVersionFromProcedure);
                cmdKubeClient().patchResource(getResourceApiVersion(Kafka.RESOURCE_PLURAL), clusterName, "/spec/kafka/version", kafkaVersionFromProcedure);

                waitForKafkaControllersAndBrokersFinishRollingUpdate();
            }

            String metadataVersion = versionModificationData.getProcedures().getMetadataVersion();

            if (metadataVersion != null && !metadataVersion.isEmpty()) {
                LOGGER.info("Set metadata version to {} (current version is {})", metadataVersion, currentMetadataVersion);
                cmdKubeClient().patchResource(getResourceApiVersion(Kafka.RESOURCE_PLURAL), clusterName, "/spec/kafka/metadataVersion", metadataVersion);

                makeSnapshots();
            }
        }
    }

    @Override
    protected void checkAllImages(BundleVersionModificationData versionModificationData, String namespaceName) {
        if (versionModificationData.getImagesAfterOperations().isEmpty()) {
            fail("There are no expected images");
        }

        checkContainerImages(controllerSelector, versionModificationData.getKafkaImage());
        checkContainerImages(brokerSelector, versionModificationData.getKafkaImage());
        checkContainerImages(eoSelector, versionModificationData.getTopicOperatorImage());
        checkContainerImages(eoSelector, 1, versionModificationData.getUserOperatorImage());
    }

    @Override
    protected void logPodImages(String namespaceName) {
        logPodImages(namespaceName, controllerSelector, brokerSelector, eoSelector, coSelector);
    }

    @Override
    protected void logPodImagesWithConnect(String namespaceName) {
        logPodImages(namespaceName, controllerSelector, brokerSelector, eoSelector, coSelector);
    }

    protected void waitForKafkaControllersAndBrokersFinishRollingUpdate() {
        LOGGER.info("Waiting for Kafka rolling update to finish");
        controllerPods = RollingUpdateUtils.waitTillComponentHasRolled(TestConstants.CO_NAMESPACE, controllerSelector, 3, controllerPods);
        brokerPods = RollingUpdateUtils.waitTillComponentHasRolled(TestConstants.CO_NAMESPACE, brokerSelector, 3, brokerPods);
    }

    protected void applyKafkaCustomResourceFromPath(String kafkaFilePath, String kafkaVersionFromCR, String kafkaMetadataVersion) {
        // Change kafka version of it's empty (null is for remove the version)
        String metadataVersion = kafkaVersionFromCR == null ? null : kafkaMetadataVersion;

        kafkaYaml = new File(kafkaFilePath);
        LOGGER.info("Deploying Kafka from: {}", kafkaYaml.getPath());
        cmdKubeClient().applyContent(KafkaUtils.changeOrRemoveKafkaConfigurationInKRaft(kafkaYaml, kafkaVersionFromCR, metadataVersion));
    }

    protected void applyCustomResourcesFromPath(String examplesPath, String kafkaFilePath, String kafkaVersionFromCR, String kafkaMetadataVersion) {
        applyKafkaCustomResourceFromPath(kafkaFilePath, kafkaVersionFromCR, kafkaMetadataVersion);

        kafkaUserYaml = new File(examplesPath + "/examples/user/kafka-user.yaml");
        LOGGER.info("Deploying KafkaUser from: {}", kafkaUserYaml.getPath());
        cmdKubeClient().applyContent(KafkaUserUtils.removeKafkaUserPart(kafkaUserYaml, "authorization"));

        kafkaTopicYaml = new File(examplesPath + "/examples/topic/kafka-topic.yaml");
        LOGGER.info("Deploying KafkaTopic from: {}", kafkaTopicYaml.getPath());
        cmdKubeClient().applyContent(TestUtils.readFile(kafkaTopicYaml));
    }
}
