/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.KindIPv6NotSupported;
import io.strimzi.systemtest.annotations.MicroShiftNotSupported;
import io.strimzi.systemtest.annotations.RequiresSharedNamespace;
import io.strimzi.systemtest.resources.crd.KafkaComponents;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;

import static io.strimzi.systemtest.TestConstants.CO_NAMESPACE;
import static io.strimzi.systemtest.TestTags.KRAFT_UPGRADE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Class for testing upgrade process of Strimzi with its components when running in KRaft mode
 *      -> KRaft to KRaft upgrades
 * Metadata for the following tests are collected from systemtest/src/test/resources/upgrade/BundleUpgrade.yaml
 */
@Tag(KRAFT_UPGRADE)
@RequiresSharedNamespace("Resources are created in the test-suite-namespace, also needed for the ParameterizedTest")
public class KRaftStrimziUpgradeST extends AbstractKRaftUpgradeST {

    private static final Logger LOGGER = LogManager.getLogger(KRaftStrimziUpgradeST.class);
    private final VersionModificationDataLoader upgradeDataLoader = new VersionModificationDataLoader(VersionModificationDataLoader.ModificationType.BUNDLE_UPGRADE);
    private final BundleVersionModificationData acrossUpgradeData = upgradeDataLoader.buildDataForUpgradeAcrossVersionsForKRaft();
    private final BundleVersionModificationData unsupportedKafkaUpgradeData = upgradeDataLoader.buildDataForUpgradeWithUnsupportedKafkaVersion();

    @MicroShiftNotSupported("Due to lack of Kafka Connect build feature")
    @KindIPv6NotSupported("Our current CI setup doesn't allow pushing into internal registries that is needed in this test")
    @ParameterizedTest(name = "from: {0} (using FG <{2}>) to: {1} (using FG <{3}>)")
    @MethodSource("io.strimzi.systemtest.upgrade.VersionModificationDataLoader#loadYamlUpgradeDataForKRaft")
    void testUpgradeOfKafkaKafkaConnectAndKafkaConnector(String fromVersion, String toVersion, String fgBefore, String fgAfter, BundleVersionModificationData upgradeData) throws IOException {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        UpgradeKafkaVersion upgradeKafkaVersion = new UpgradeKafkaVersion(upgradeData.getDeployKafkaVersion());
        // setting metadata version to null, similarly to the examples, which are not configuring metadataVersion
        upgradeKafkaVersion.setMetadataVersion(null);

        assumeTrue(StUtils.isAllowOnCurrentEnvironment(upgradeData.getEnvFlakyVariable()));
        assumeTrue(StUtils.isAllowedOnCurrentK8sVersion(upgradeData.getEnvMaxK8sVersion()));

        LOGGER.debug("Running upgrade test from version {} to {} (FG: {} -> {})",
            fromVersion, toVersion, fgBefore, fgAfter);
        doKafkaConnectAndKafkaConnectorUpgradeOrDowngradeProcedure(CO_NAMESPACE, testStorage, upgradeData, upgradeKafkaVersion);
    }

    /**
     * Upgrades the Cluster Operator above a Kafka cluster that has no `version` configured, but keeps the
     * `metadataVersion`. Nothing in the test changes the Kafka version, so the new operator is the only thing that
     * decides which Kafka version the cluster ends up on - its default one.
     */
    @IsolatedTest
    void testUpgradeWithoutKafkaVersion() throws IOException {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        UpgradeKafkaVersion upgradeKafkaVersion = UpgradeKafkaVersion.getKafkaWithVersionFromUrl(acrossUpgradeData.getFromKafkaVersionsUrl(), acrossUpgradeData.getDeployKafkaVersion());
        upgradeKafkaVersion.setVersion(null);

        doUpgradeWithoutKafkaVersionProcedure(testStorage, upgradeKafkaVersion);
    }

    /**
     * Same as {@link #testUpgradeWithoutKafkaVersion()}, but the Kafka cluster is deployed with neither `version`
     * nor `metadataVersion` configured.
     */
    @IsolatedTest
    void testUpgradeWithoutKafkaAndMetadataVersions() throws IOException {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        doUpgradeWithoutKafkaVersionProcedure(testStorage, null);
    }

    /**
     * Covers users upgrading from an old Strimzi running a Kafka version that the new Strimzi no longer supports.
     * The Kafka cluster cannot be reconciled by the new operator until its version is raised to a supported one, so
     * unlike the other upgrade tests this one does change the Kafka version in the Kafka CR.
     */
    @IsolatedTest
    void testUpgradeAcrossVersionsWithUnsupportedKafkaVersion() throws IOException {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final UpgradeKafkaVersion unsupportedKafkaVersion = UpgradeKafkaVersion.getKafkaWithVersionFromUrl(
            unsupportedKafkaUpgradeData.getFromKafkaVersionsUrl(), unsupportedKafkaUpgradeData.getDeployKafkaVersion());

        // Setup env
        setupEnvAndUpgradeClusterOperator(CO_NAMESPACE, testStorage, unsupportedKafkaUpgradeData, unsupportedKafkaVersion);

        // Upgrade CO. The Kafka cluster keeps running the unsupported version, so the new operator cannot roll it yet
        changeClusterOperator(CO_NAMESPACE, testStorage.getNamespaceName(), unsupportedKafkaUpgradeData);
        logClusterOperatorPodImage(CO_NAMESPACE);

        // Upgrade Kafka to a supported version - this is what unblocks the reconciliation and rolls the whole cluster
        final String targetKafkaVersion = unsupportedKafkaUpgradeData.getProcedures().getVersion();
        changeKafkaVersion(testStorage.getNamespaceName(), unsupportedKafkaUpgradeData);
        waitForReadinessOfKafkaCluster(testStorage.getNamespaceName());

        logComponentsPodImages(testStorage.getNamespaceName());
        checkAllComponentsImages(testStorage.getNamespaceName(), unsupportedKafkaUpgradeData, targetKafkaVersion);

        // Verify that Pods are stable
        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), CLUSTER_NAME);

        // Verify upgrade
        verifyProcedure(testStorage.getNamespaceName(), unsupportedKafkaUpgradeData, testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName());

        assertKafkaPodsUseVersion(testStorage.getNamespaceName(), targetKafkaVersion);
    }

    /**
     * Deploys a Kafka cluster without the `version` configured, upgrades only the Cluster Operator and verifies that
     * the cluster ends up on the Kafka version defaulted by the new operator.
     *
     * @param testStorage           Test-related configuration and storage
     * @param upgradeKafkaVersion   Kafka version details used for the initial deployment. Passing {@code null}
     *                              removes the `metadataVersion` from the Kafka CR as well.
     */
    private void doUpgradeWithoutKafkaVersionProcedure(TestStorage testStorage, UpgradeKafkaVersion upgradeKafkaVersion) throws IOException {
        // Setup env
        setupEnvAndUpgradeClusterOperator(CO_NAMESPACE, testStorage, acrossUpgradeData, upgradeKafkaVersion);

        // Upgrade CO
        changeClusterOperator(CO_NAMESPACE, testStorage.getNamespaceName(), acrossUpgradeData);
        logClusterOperatorPodImage(CO_NAMESPACE);

        waitForKafkaClusterRollingUpdate(testStorage.getNamespaceName());

        // No Kafka version is configured in the Kafka CR, so the new operator defaults it to its own default version
        final String expectedKafkaVersion = TestKafkaVersion.getDefaultSupportedKafkaVersion();

        logComponentsPodImages(testStorage.getNamespaceName());
        checkAllComponentsImages(testStorage.getNamespaceName(), acrossUpgradeData, expectedKafkaVersion);

        // Verify that Pods are stable
        PodUtils.verifyThatRunningPodsAreStable(testStorage.getNamespaceName(), CLUSTER_NAME);

        // Verify upgrade
        verifyProcedure(testStorage.getNamespaceName(), acrossUpgradeData, testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName());

        assertKafkaPodsUseVersion(testStorage.getNamespaceName(), expectedKafkaVersion);
    }

    /**
     * Asserts that the Kafka libraries present in the controller and broker Pods belong to the expected Kafka version.
     *
     * @param namespaceName     Namespace where the Kafka cluster is deployed
     * @param kafkaVersion      Kafka version the Pods are expected to run
     */
    private void assertKafkaPodsUseVersion(String namespaceName, String kafkaVersion) {
        final String controllerPodName = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(namespaceName, KafkaComponents.getPodSetName(CLUSTER_NAME, CONTROLLER_NODE_NAME)).get(0).getMetadata().getName();
        final String brokerPodName = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(namespaceName, KafkaComponents.getPodSetName(CLUSTER_NAME, BROKER_NODE_NAME)).get(0).getMetadata().getName();

        final String mavenVersion = TestKafkaVersion.getSpecificVersion(kafkaVersion).mavenVersion();

        assertThat(KafkaUtils.getVersionFromKafkaPodLibs(namespaceName, controllerPodName), containsString(mavenVersion));
        assertThat(KafkaUtils.getVersionFromKafkaPodLibs(namespaceName, brokerPodName), containsString(mavenVersion));
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
