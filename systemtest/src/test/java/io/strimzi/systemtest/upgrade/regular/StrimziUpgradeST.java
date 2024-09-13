/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade.regular;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.KindIPv6NotSupported;
import io.strimzi.systemtest.annotations.MicroShiftNotSupported;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.upgrade.AbstractUpgradeST;
import io.strimzi.systemtest.upgrade.BundleVersionModificationData;
import io.strimzi.systemtest.upgrade.UpgradeKafkaVersion;
import io.strimzi.systemtest.upgrade.VersionModificationDataLoader;
import io.strimzi.systemtest.upgrade.VersionModificationDataLoader.ModificationType;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Map;

import static io.strimzi.systemtest.Environment.TEST_SUITE_NAMESPACE;
import static io.strimzi.systemtest.TestConstants.CO_NAMESPACE;
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
public class StrimziUpgradeST extends AbstractUpgradeST {

    private static final Logger LOGGER = LogManager.getLogger(StrimziUpgradeST.class);
    private final BundleVersionModificationData acrossUpgradeData = new VersionModificationDataLoader(ModificationType.BUNDLE_UPGRADE).buildDataForUpgradeAcrossVersions();

    @MicroShiftNotSupported("Due to lack of Kafka Connect build feature")
    @KindIPv6NotSupported("Our current CI setup doesn't allow pushing into internal registries that is needed in this test")
    @ParameterizedTest(name = "from: {0} (using FG <{2}>) to: {1} (using FG <{3}>)")
    @MethodSource("io.strimzi.systemtest.upgrade.VersionModificationDataLoader#loadYamlUpgradeData")
    void testUpgradeOfKafkaKafkaConnectAndKafkaConnector(String fromVersion, String toVersion, String fgBefore, String fgAfter, BundleVersionModificationData upgradeData) throws IOException {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        UpgradeKafkaVersion upgradeKafkaVersion = new UpgradeKafkaVersion(upgradeData.getOldestKafka());
        // setting log message version to null, similarly to the examples, which are not configuring LMFV
        upgradeKafkaVersion.setLogMessageVersion(null);

        assumeTrue(StUtils.isAllowOnCurrentEnvironment(upgradeData.getEnvFlakyVariable()));
        assumeTrue(StUtils.isAllowedOnCurrentK8sVersion(upgradeData.getEnvMaxK8sVersion()));

        LOGGER.debug("Running upgrade test from version {} to {} (FG: {} -> {})",
            fromVersion, toVersion, fgBefore, fgAfter);
        doKafkaConnectAndKafkaConnectorUpgradeOrDowngradeProcedure(CO_NAMESPACE, testStorage, upgradeData, upgradeKafkaVersion);
    }

    @IsolatedTest
    void testUpgradeKafkaWithoutVersion() throws IOException {
        UpgradeKafkaVersion upgradeKafkaVersion = UpgradeKafkaVersion.getKafkaWithVersionFromUrl(acrossUpgradeData.getFromKafkaVersionsUrl(), acrossUpgradeData.getStartingKafkaVersion());
        upgradeKafkaVersion.setVersion(null);

        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        // Setup env
        setupEnvAndUpgradeClusterOperator(CO_NAMESPACE, testStorage, acrossUpgradeData, upgradeKafkaVersion);

        final Map<String, String> zooSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), controllerSelector);
        final Map<String, String> kafkaSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), brokerSelector);
        final Map<String, String> eoSnapshot = DeploymentUtils.depSnapshot(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(clusterName));

        // Make snapshots of all Pods
        makeComponentsSnapshots(testStorage.getNamespaceName());

        // Check if UTO is used before changing the CO -> used for check for KafkaTopics
        boolean wasUTOUsedBefore = StUtils.isUnidirectionalTopicOperatorUsed(testStorage.getNamespaceName(), eoSelector);

        // Upgrade CO
        changeClusterOperator(CO_NAMESPACE, testStorage.getNamespaceName(),  acrossUpgradeData);

        logPodImages(CO_NAMESPACE);

        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), controllerSelector, 3, zooSnapshot);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), brokerSelector, 3, kafkaSnapshot);
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoSnapshot);

        logPodImages(CO_NAMESPACE);
        checkAllComponentsImages(testStorage.getNamespaceName(), acrossUpgradeData);

        // Verify that Pods are stable
        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), clusterName);
        // Verify upgrade
        verifyProcedure(testStorage.getNamespaceName(), acrossUpgradeData, testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName(), wasUTOUsedBefore);
        assertThat(KafkaUtils.getVersionFromKafkaPodLibs(testStorage.getNamespaceName(), KafkaResources.kafkaPodName(clusterName, 0)), containsString(acrossUpgradeData.getProcedures().getVersion()));
    }

    @IsolatedTest
    void testUpgradeAcrossVersionsWithUnsupportedKafkaVersion() throws IOException {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        UpgradeKafkaVersion upgradeKafkaVersion = UpgradeKafkaVersion.getKafkaWithVersionFromUrl(acrossUpgradeData.getFromKafkaVersionsUrl(), acrossUpgradeData.getStartingKafkaVersion());

        // Setup env
        setupEnvAndUpgradeClusterOperator(CO_NAMESPACE, testStorage, acrossUpgradeData, upgradeKafkaVersion);

        // Make snapshots of all Pods
        makeComponentsSnapshots(TEST_SUITE_NAMESPACE);

        // Check if UTO is used before changing the CO -> used for check for KafkaTopics
        boolean wasUTOUsedBefore = StUtils.isUnidirectionalTopicOperatorUsed(TEST_SUITE_NAMESPACE, eoSelector);

        // Upgrade CO
        changeClusterOperator(CO_NAMESPACE, TEST_SUITE_NAMESPACE, acrossUpgradeData);
        logPodImages(CO_NAMESPACE);
        //  Upgrade kafka
        changeKafkaVersion(TEST_SUITE_NAMESPACE, acrossUpgradeData);
        logPodImages(TEST_SUITE_NAMESPACE);
        checkAllComponentsImages(TEST_SUITE_NAMESPACE, acrossUpgradeData);
        // Verify that Pods are stable
        PodUtils.verifyThatRunningPodsAreStable(TEST_SUITE_NAMESPACE, clusterName);
        // Verify upgrade
        verifyProcedure(TEST_SUITE_NAMESPACE, acrossUpgradeData, testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName(), wasUTOUsedBefore);
    }

    @IsolatedTest
    void testUpgradeAcrossVersionsWithNoKafkaVersion() throws IOException {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        // Setup env
        setupEnvAndUpgradeClusterOperator(CO_NAMESPACE, testStorage, acrossUpgradeData, null);

        // Check if UTO is used before changing the CO -> used for check for KafkaTopics
        boolean wasUTOUsedBefore = StUtils.isUnidirectionalTopicOperatorUsed(TEST_SUITE_NAMESPACE, eoSelector);

        // Upgrade CO
        changeClusterOperator(CO_NAMESPACE, TEST_SUITE_NAMESPACE, acrossUpgradeData);
        // Wait till first upgrade finished
        controllerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(TEST_SUITE_NAMESPACE, controllerSelector, 3, controllerPods);
        brokerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(TEST_SUITE_NAMESPACE, brokerSelector, 3, brokerPods);
        eoPods = DeploymentUtils.waitTillDepHasRolled(TEST_SUITE_NAMESPACE, KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoPods);

        LOGGER.info("Rolling to new images has finished!");
        logPodImages(CO_NAMESPACE);
        //  Upgrade kafka
        changeKafkaVersion(testStorage.getNamespaceName(), acrossUpgradeData);
        logPodImages(CO_NAMESPACE);
        checkAllComponentsImages(TEST_SUITE_NAMESPACE, acrossUpgradeData);
        // Verify that Pods are stable
        PodUtils.verifyThatRunningPodsAreStable(TEST_SUITE_NAMESPACE, clusterName);
        // Verify upgrade
        verifyProcedure(TEST_SUITE_NAMESPACE, acrossUpgradeData, testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName(), wasUTOUsedBefore);
    }

    @BeforeEach
    void setupEnvironment() {
        setUpStrimziUpgradeTestNamespaces();
    }

    @AfterEach
    void afterEach() {
        cleanUpStrimziUpgradeTestNamespaces();
    }
}
