/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade.kraft;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.resources.NamespaceManager;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.upgrade.BundleVersionModificationData;
import io.strimzi.systemtest.upgrade.UpgradeKafkaVersion;
import io.strimzi.systemtest.upgrade.VersionModificationDataLoader;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Map;

import static io.strimzi.systemtest.TestConstants.CO_NAMESPACE;
import static io.strimzi.systemtest.TestConstants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestConstants.KRAFT_UPGRADE;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Class for testing upgrade process of Strimzi with its components when running in KRaft mode
 *      -> KRaft to KRaft upgrades
 * Metadata for the following tests are collected from systemtest/src/test/resources/upgrade/BundleUpgrade.yaml
 */
@Tag(KRAFT_UPGRADE)
public class KRaftStrimziUpgradeST extends AbstractKRaftUpgradeST {

    private static final Logger LOGGER = LogManager.getLogger(KRaftStrimziUpgradeST.class);
    private final BundleVersionModificationData acrossUpgradeData = new VersionModificationDataLoader(VersionModificationDataLoader.ModificationType.BUNDLE_UPGRADE).buildDataForUpgradeAcrossVersionsForKRaft();

    @ParameterizedTest(name = "from: {0} (using FG <{2}>) to: {1} (using FG <{3}>) ")
    @MethodSource("io.strimzi.systemtest.upgrade.VersionModificationDataLoader#loadYamlUpgradeDataForKRaft")
    @Tag(INTERNAL_CLIENTS_USED)
    void testUpgradeStrimziVersion(String fromVersion, String toVersion, String fgBefore, String fgAfter, BundleVersionModificationData upgradeData, ExtensionContext extensionContext) throws Exception {
        assumeTrue(StUtils.isAllowOnCurrentEnvironment(upgradeData.getEnvFlakyVariable()));
        assumeTrue(StUtils.isAllowedOnCurrentK8sVersion(upgradeData.getEnvMaxK8sVersion()));

        performUpgrade(upgradeData, extensionContext);
    }

    @IsolatedTest
    void testUpgradeKafkaWithoutVersion() throws IOException {
        UpgradeKafkaVersion upgradeKafkaVersion = UpgradeKafkaVersion.getKafkaWithVersionFromUrl(acrossUpgradeData.getFromKafkaVersionsUrl(), acrossUpgradeData.getStartingKafkaVersion());
        upgradeKafkaVersion.setVersion(null);

        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        // Setup env
        setupEnvAndUpgradeClusterOperator(acrossUpgradeData, testStorage, upgradeKafkaVersion, TestConstants.CO_NAMESPACE);

        Map<String, String> controllerSnapshot = PodUtils.podSnapshot(TestConstants.CO_NAMESPACE, controllerSelector);
        Map<String, String> brokerSnapshot = PodUtils.podSnapshot(TestConstants.CO_NAMESPACE, brokerSelector);
        Map<String, String> eoSnapshot = PodUtils.podSnapshot(TestConstants.CO_NAMESPACE, eoSelector);

        // Make snapshots of all Pods
        makeSnapshots();

        // Check if UTO is used before changing the CO -> used for check for KafkaTopics
        boolean wasUTOUsedBefore = StUtils.isUnidirectionalTopicOperatorUsed(TestConstants.CO_NAMESPACE, eoSelector);

        // Upgrade CO
        changeClusterOperator(acrossUpgradeData, TestConstants.CO_NAMESPACE);

        logPodImages(TestConstants.CO_NAMESPACE);

        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(TestConstants.CO_NAMESPACE, controllerSelector, 3, controllerSnapshot);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(TestConstants.CO_NAMESPACE, brokerSelector, 3, brokerSnapshot);
        DeploymentUtils.waitTillDepHasRolled(TestConstants.CO_NAMESPACE, KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoSnapshot);

        logPodImages(TestConstants.CO_NAMESPACE);
        checkAllImages(acrossUpgradeData, TestConstants.CO_NAMESPACE);

        // Verify that Pods are stable
        PodUtils.verifyThatRunningPodsAreStable(TestConstants.CO_NAMESPACE, clusterName);
        // Verify upgrade
        verifyProcedure(acrossUpgradeData, testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName(), TestConstants.CO_NAMESPACE, wasUTOUsedBefore);

        String controllerPodName = kubeClient().listPodsByPrefixInName(TestConstants.CO_NAMESPACE, KafkaResource.getStrimziPodSetName(clusterName, CONTROLLER_NODE_NAME)).get(0).getMetadata().getName();
        String brokerPodName = kubeClient().listPodsByPrefixInName(TestConstants.CO_NAMESPACE, KafkaResource.getStrimziPodSetName(clusterName, BROKER_NODE_NAME)).get(0).getMetadata().getName();

        assertThat(KafkaUtils.getVersionFromKafkaPodLibs(controllerPodName), containsString(acrossUpgradeData.getProcedures().getVersion()));
        assertThat(KafkaUtils.getVersionFromKafkaPodLibs(brokerPodName), containsString(acrossUpgradeData.getProcedures().getVersion()));
    }

    @IsolatedTest
    void testUpgradeAcrossVersionsWithUnsupportedKafkaVersion(ExtensionContext extensionContext) throws IOException {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        UpgradeKafkaVersion upgradeKafkaVersion = UpgradeKafkaVersion.getKafkaWithVersionFromUrl(acrossUpgradeData.getFromKafkaVersionsUrl(), acrossUpgradeData.getStartingKafkaVersion());

        // Setup env
        setupEnvAndUpgradeClusterOperator(acrossUpgradeData, testStorage, upgradeKafkaVersion, TestConstants.CO_NAMESPACE);

        // Make snapshots of all Pods
        makeSnapshots();

        // Check if UTO is used before changing the CO -> used for check for KafkaTopics
        boolean wasUTOUsedBefore = StUtils.isUnidirectionalTopicOperatorUsed(TestConstants.CO_NAMESPACE, eoSelector);

        // Upgrade CO
        changeClusterOperator(acrossUpgradeData, TestConstants.CO_NAMESPACE);

        waitForKafkaClusterRollingUpdate();

        logPodImages(TestConstants.CO_NAMESPACE);

        // Upgrade kafka
        changeKafkaAndMetadataVersion(acrossUpgradeData, true);

        logPodImages(TestConstants.CO_NAMESPACE);

        checkAllImages(acrossUpgradeData, TestConstants.CO_NAMESPACE);

        // Verify that Pods are stable
        PodUtils.verifyThatRunningPodsAreStable(TestConstants.CO_NAMESPACE, clusterName);

        // Verify upgrade
        verifyProcedure(acrossUpgradeData, testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName(), TestConstants.CO_NAMESPACE, wasUTOUsedBefore);
    }

    @IsolatedTest
    void testUpgradeAcrossVersionsWithNoKafkaVersion(ExtensionContext extensionContext) throws IOException {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        // Setup env
        setupEnvAndUpgradeClusterOperator(acrossUpgradeData, testStorage, null, TestConstants.CO_NAMESPACE);

        // Check if UTO is used before changing the CO -> used for check for KafkaTopics
        boolean wasUTOUsedBefore = StUtils.isUnidirectionalTopicOperatorUsed(TestConstants.CO_NAMESPACE, eoSelector);

        // Upgrade CO
        changeClusterOperator(acrossUpgradeData, TestConstants.CO_NAMESPACE);

        // Wait till first upgrade finished
        controllerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(TestConstants.CO_NAMESPACE, controllerSelector, 3, controllerPods);
        brokerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(TestConstants.CO_NAMESPACE, brokerSelector, 3, brokerPods);
        eoPods = DeploymentUtils.waitTillDepHasRolled(TestConstants.CO_NAMESPACE, KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoPods);

        LOGGER.info("Rolling to new images has finished!");
        logPodImages(TestConstants.CO_NAMESPACE);

        // Upgrade kafka
        changeKafkaAndMetadataVersion(acrossUpgradeData);

        logPodImages(TestConstants.CO_NAMESPACE);

        checkAllImages(acrossUpgradeData, TestConstants.CO_NAMESPACE);

        // Verify that Pods are stable
        PodUtils.verifyThatRunningPodsAreStable(TestConstants.CO_NAMESPACE, clusterName);

        // Verify upgrade
        verifyProcedure(acrossUpgradeData, testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName(), TestConstants.CO_NAMESPACE, wasUTOUsedBefore);
    }

    @IsolatedTest
    void testUpgradeOfKafkaConnectAndKafkaConnector(final ExtensionContext extensionContext) throws IOException {
        final TestStorage testStorage = new TestStorage(extensionContext, TestConstants.CO_NAMESPACE);
        final UpgradeKafkaVersion upgradeKafkaVersion = new UpgradeKafkaVersion(acrossUpgradeData.getDefaultKafka());

        doKafkaConnectAndKafkaConnectorUpgradeOrDowngradeProcedure(acrossUpgradeData, testStorage, upgradeKafkaVersion);
    }

    private void performUpgrade(BundleVersionModificationData upgradeData, ExtensionContext extensionContext) throws IOException {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        // leave empty, so the original Kafka version from appropriate Strimzi's yaml will be used
        UpgradeKafkaVersion upgradeKafkaVersion = new UpgradeKafkaVersion();

        // Setup env
        setupEnvAndUpgradeClusterOperator(upgradeData, testStorage, upgradeKafkaVersion, TestConstants.CO_NAMESPACE);

        // Upgrade CO to HEAD
        logPodImages(TestConstants.CO_NAMESPACE);

        // Check if UTO is used before changing the CO -> used for check for KafkaTopics
        boolean wasUTOUsedBefore = StUtils.isUnidirectionalTopicOperatorUsed(TestConstants.CO_NAMESPACE, eoSelector);

        changeClusterOperator(upgradeData, TestConstants.CO_NAMESPACE);

        if (TestKafkaVersion.supportedVersionsContainsVersion(upgradeData.getDefaultKafkaVersionPerStrimzi())) {
            waitForKafkaClusterRollingUpdate();
        }

        logPodImages(TestConstants.CO_NAMESPACE);

        // Upgrade kafka
        changeKafkaAndMetadataVersion(upgradeData, true);

        logPodImages(TestConstants.CO_NAMESPACE);

        checkAllImages(upgradeData, TestConstants.CO_NAMESPACE);

        // Verify that Pods are stable
        PodUtils.verifyThatRunningPodsAreStable(TestConstants.CO_NAMESPACE, clusterName);

        // Verify upgrade
        verifyProcedure(upgradeData, testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName(), TestConstants.CO_NAMESPACE, wasUTOUsedBefore);
    }

    @BeforeEach
    void setupEnvironment() {
        NamespaceManager.getInstance().createNamespaceAndPrepare(CO_NAMESPACE);
    }

    protected void afterEachMayOverride(ExtensionContext extensionContext) {
        cleanUpKafkaTopics();
        ResourceManager.getInstance().deleteResources();
        NamespaceManager.getInstance().deleteNamespaceWithWait(CO_NAMESPACE);
    }
}
