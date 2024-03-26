/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade.regular;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.resources.NamespaceManager;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.upgrade.AbstractUpgradeST;
import io.strimzi.systemtest.upgrade.BundleVersionModificationData;
import io.strimzi.systemtest.upgrade.UpgradeKafkaVersion;
import io.strimzi.systemtest.upgrade.VersionModificationDataLoader;
import io.strimzi.systemtest.upgrade.VersionModificationDataLoader.ModificationType;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Map;

import static io.strimzi.systemtest.TestConstants.CO_NAMESPACE;
import static io.strimzi.systemtest.TestConstants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestConstants.UPGRADE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * This test class contains tests for Strimzi upgrade from version X to version X + 1.
 * Metadata for upgrade procedure are available in resource file StrimziUpgrade.json
 * Kafka upgrade is done as part of those tests as well, but the tests for Kafka upgrade/downgrade are in {@link KafkaUpgradeDowngradeST}.
 */
@Tag(UPGRADE)
@KRaftNotSupported("Strimzi and Kafka upgrade is not supported with KRaft mode")
public class StrimziUpgradeST extends AbstractUpgradeST {

    private static final Logger LOGGER = LogManager.getLogger(StrimziUpgradeST.class);
    private final BundleVersionModificationData acrossUpgradeData = new VersionModificationDataLoader(ModificationType.BUNDLE_UPGRADE).buildDataForUpgradeAcrossVersions();

    @ParameterizedTest(name = "from: {0} (using FG <{2}>) to: {1} (using FG <{3}>) ")
    @MethodSource("io.strimzi.systemtest.upgrade.VersionModificationDataLoader#loadYamlUpgradeData")
    @Tag(INTERNAL_CLIENTS_USED)
    void testUpgradeStrimziVersion(String fromVersion, String toVersion, String fgBefore, String fgAfter, BundleVersionModificationData upgradeData) throws Exception {
        assumeTrue(StUtils.isAllowOnCurrentEnvironment(upgradeData.getEnvFlakyVariable()));
        assumeTrue(StUtils.isAllowedOnCurrentK8sVersion(upgradeData.getEnvMaxK8sVersion()));

        LOGGER.debug("Running upgrade test from version {} to {} (FG: {} -> {})",
                fromVersion, toVersion, fgBefore, fgAfter);
        performUpgrade(upgradeData);
    }

    @Test
    void testUpgradeKafkaWithoutVersion() throws IOException {
        UpgradeKafkaVersion upgradeKafkaVersion = UpgradeKafkaVersion.getKafkaWithVersionFromUrl(acrossUpgradeData.getFromKafkaVersionsUrl(), acrossUpgradeData.getStartingKafkaVersion());
        upgradeKafkaVersion.setVersion(null);

        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        // Setup env
        setupEnvAndUpgradeClusterOperator(acrossUpgradeData, testStorage, upgradeKafkaVersion, TestConstants.CO_NAMESPACE);

        Map<String, String> zooSnapshot = PodUtils.podSnapshot(TestConstants.CO_NAMESPACE, controllerSelector);
        Map<String, String> kafkaSnapshot = PodUtils.podSnapshot(TestConstants.CO_NAMESPACE, brokerSelector);
        Map<String, String> eoSnapshot = DeploymentUtils.depSnapshot(TestConstants.CO_NAMESPACE, KafkaResources.entityOperatorDeploymentName(clusterName));

        // Make snapshots of all Pods
        makeSnapshots();

        // Check if UTO is used before changing the CO -> used for check for KafkaTopics
        boolean wasUTOUsedBefore = StUtils.isUnidirectionalTopicOperatorUsed(TestConstants.CO_NAMESPACE, eoSelector);

        // Upgrade CO
        changeClusterOperator(acrossUpgradeData, TestConstants.CO_NAMESPACE);

        logPodImages(TestConstants.CO_NAMESPACE);

        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(TestConstants.CO_NAMESPACE, controllerSelector, 3, zooSnapshot);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(TestConstants.CO_NAMESPACE, brokerSelector, 3, kafkaSnapshot);
        DeploymentUtils.waitTillDepHasRolled(TestConstants.CO_NAMESPACE, KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoSnapshot);

        logPodImages(TestConstants.CO_NAMESPACE);
        checkAllImages(acrossUpgradeData, TestConstants.CO_NAMESPACE);

        // Verify that Pods are stable
        PodUtils.verifyThatRunningPodsAreStable(TestConstants.CO_NAMESPACE, clusterName);
        // Verify upgrade
        verifyProcedure(acrossUpgradeData, testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName(), TestConstants.CO_NAMESPACE, wasUTOUsedBefore);
        assertThat(KafkaUtils.getVersionFromKafkaPodLibs(KafkaResources.kafkaPodName(clusterName, 0)), containsString(acrossUpgradeData.getProcedures().getVersion()));
    }

    @Test
    void testUpgradeAcrossVersionsWithUnsupportedKafkaVersion() throws IOException {
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
        logPodImages(TestConstants.CO_NAMESPACE);
        //  Upgrade kafka
        changeKafkaAndLogFormatVersion(acrossUpgradeData);
        logPodImages(TestConstants.CO_NAMESPACE);
        checkAllImages(acrossUpgradeData, TestConstants.CO_NAMESPACE);
        // Verify that Pods are stable
        PodUtils.verifyThatRunningPodsAreStable(TestConstants.CO_NAMESPACE, clusterName);
        // Verify upgrade
        verifyProcedure(acrossUpgradeData, testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName(), TestConstants.CO_NAMESPACE, wasUTOUsedBefore);
    }

    @Test
    void testUpgradeAcrossVersionsWithNoKafkaVersion() throws IOException {
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
        //  Upgrade kafka
        changeKafkaAndLogFormatVersion(acrossUpgradeData);
        logPodImages(TestConstants.CO_NAMESPACE);
        checkAllImages(acrossUpgradeData, TestConstants.CO_NAMESPACE);
        // Verify that Pods are stable
        PodUtils.verifyThatRunningPodsAreStable(TestConstants.CO_NAMESPACE, clusterName);
        // Verify upgrade
        verifyProcedure(acrossUpgradeData, testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName(), TestConstants.CO_NAMESPACE, wasUTOUsedBefore);
    }

    @Test
    void testUpgradeOfKafkaConnectAndKafkaConnector() throws IOException {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext(), TestConstants.CO_NAMESPACE);
        final UpgradeKafkaVersion upgradeKafkaVersion = new UpgradeKafkaVersion(acrossUpgradeData.getDefaultKafka());

        doKafkaConnectAndKafkaConnectorUpgradeOrDowngradeProcedure(acrossUpgradeData, testStorage, upgradeKafkaVersion);
    }

    private void performUpgrade(BundleVersionModificationData upgradeData) throws IOException {
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
        changeKafkaAndLogFormatVersion(upgradeData);
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

    protected void afterEachMayOverride() {
        cleanUpKafkaTopics();
        ResourceManager.getInstance().deleteResources();
        NamespaceManager.getInstance().deleteNamespaceWithWait(CO_NAMESPACE);
    }
}
