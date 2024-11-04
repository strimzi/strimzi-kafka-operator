/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade.kraft;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.KindIPv6NotSupported;
import io.strimzi.systemtest.annotations.MicroShiftNotSupported;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.upgrade.BundleVersionModificationData;
import io.strimzi.systemtest.upgrade.UpgradeKafkaVersion;
import io.strimzi.systemtest.upgrade.VersionModificationDataLoader;
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

import static io.strimzi.systemtest.TestConstants.CO_NAMESPACE;
import static io.strimzi.systemtest.TestTags.KRAFT_UPGRADE;
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

    @MicroShiftNotSupported("Due to lack of Kafka Connect build feature")
    @KindIPv6NotSupported("Our current CI setup doesn't allow pushing into internal registries that is needed in this test")
    @ParameterizedTest(name = "from: {0} (using FG <{2}>) to: {1} (using FG <{3}>)")
    @MethodSource("io.strimzi.systemtest.upgrade.VersionModificationDataLoader#loadYamlUpgradeDataForKRaft")
    void testUpgradeOfKafkaKafkaConnectAndKafkaConnector(String fromVersion, String toVersion, String fgBefore, String fgAfter, BundleVersionModificationData upgradeData) throws IOException {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        UpgradeKafkaVersion upgradeKafkaVersion = new UpgradeKafkaVersion(upgradeData.getOldestKafka());
        // setting metadata version to null, similarly to the examples, which are not configuring metadataVersion
        upgradeKafkaVersion.setMetadataVersion(null);

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

        Map<String, String> controllerSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), controllerSelector);
        Map<String, String> brokerSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), brokerSelector);
        Map<String, String> eoSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), eoSelector);

        // Make snapshots of all Pods
        makeComponentsSnapshots(testStorage.getNamespaceName());

        // Check if UTO is used before changing the CO -> used for check for KafkaTopics
        boolean wasUTOUsedBefore = StUtils.isUnidirectionalTopicOperatorUsed(testStorage.getNamespaceName(), eoSelector);

        // Upgrade CO
        changeClusterOperator(CO_NAMESPACE, testStorage.getNamespaceName(), acrossUpgradeData);
        logClusterOperatorPodImage(CO_NAMESPACE);
        logComponentsPodImages(testStorage.getNamespaceName());

        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), controllerSelector, 3, controllerSnapshot);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), brokerSelector, 3, brokerSnapshot);
        DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoSnapshot);
        checkAllComponentsImages(testStorage.getNamespaceName(), acrossUpgradeData);

        // Verify that Pods are stable
        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), clusterName);
        // Verify upgrade
        verifyProcedure(testStorage.getNamespaceName(), acrossUpgradeData, testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName(), wasUTOUsedBefore);

        String controllerPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(clusterName, CONTROLLER_NODE_NAME)).get(0).getMetadata().getName();
        String brokerPodName = kubeClient().listPodsByPrefixInName(testStorage.getNamespaceName(), KafkaResource.getStrimziPodSetName(clusterName, BROKER_NODE_NAME)).get(0).getMetadata().getName();

        assertThat(KafkaUtils.getVersionFromKafkaPodLibs(testStorage.getNamespaceName(), controllerPodName), containsString(acrossUpgradeData.getProcedures().getVersion()));
        assertThat(KafkaUtils.getVersionFromKafkaPodLibs(testStorage.getNamespaceName(), brokerPodName), containsString(acrossUpgradeData.getProcedures().getVersion()));
    }

    @IsolatedTest
    void testUpgradeAcrossVersionsWithUnsupportedKafkaVersion() throws IOException {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        UpgradeKafkaVersion upgradeKafkaVersion = UpgradeKafkaVersion.getKafkaWithVersionFromUrl(acrossUpgradeData.getFromKafkaVersionsUrl(), acrossUpgradeData.getStartingKafkaVersion());

        // Setup env
        setupEnvAndUpgradeClusterOperator(CO_NAMESPACE, testStorage, acrossUpgradeData, upgradeKafkaVersion);

        // Make snapshots of all Pods
        makeComponentsSnapshots(testStorage.getNamespaceName());

        // Check if UTO is used before changing the CO -> used for check for KafkaTopics
        boolean wasUTOUsedBefore = StUtils.isUnidirectionalTopicOperatorUsed(testStorage.getNamespaceName(), eoSelector);

        // Upgrade CO
        changeClusterOperator(CO_NAMESPACE, testStorage.getNamespaceName(), acrossUpgradeData);

        waitForKafkaClusterRollingUpdate(testStorage.getNamespaceName());

        logPodImages(CO_NAMESPACE, coSelector);

        // Upgrade kafka
        changeKafkaVersion(testStorage.getNamespaceName(), acrossUpgradeData, true);

        logPodImages(CO_NAMESPACE, coSelector);

        checkAllComponentsImages(testStorage.getNamespaceName(), acrossUpgradeData);

        // Verify that Pods are stable
        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), clusterName);

        // Verify upgrade
        verifyProcedure(testStorage.getNamespaceName(), acrossUpgradeData, testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName(), wasUTOUsedBefore);
    }

    @IsolatedTest
    void testUpgradeAcrossVersionsWithNoKafkaVersion() throws IOException {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());

        // Setup env
        setupEnvAndUpgradeClusterOperator(CO_NAMESPACE, testStorage, acrossUpgradeData, null);

        // Check if UTO is used before changing the CO -> used for check for KafkaTopics
        boolean wasUTOUsedBefore = StUtils.isUnidirectionalTopicOperatorUsed(testStorage.getNamespaceName(), eoSelector);

        // Upgrade CO
        changeClusterOperator(CO_NAMESPACE, testStorage.getNamespaceName(), acrossUpgradeData);

        // Wait till first upgrade finished
        controllerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), controllerSelector, 3, controllerPods);
        brokerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(testStorage.getNamespaceName(), brokerSelector, 3, brokerPods);
        eoPods = DeploymentUtils.waitTillDepHasRolled(testStorage.getNamespaceName(), KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoPods);

        LOGGER.info("Rolling to new images has finished!");
        logPodImages(CO_NAMESPACE, coSelector);

        // Upgrade kafka
        changeKafkaVersion(testStorage.getNamespaceName(), acrossUpgradeData);
        logComponentsPodImages(testStorage.getNamespaceName());
        checkAllComponentsImages(testStorage.getNamespaceName(), acrossUpgradeData);

        // Verify that Pods are stable
        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), clusterName);

        // Verify upgrade
        verifyProcedure(testStorage.getNamespaceName(), acrossUpgradeData, testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName(), wasUTOUsedBefore);
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
