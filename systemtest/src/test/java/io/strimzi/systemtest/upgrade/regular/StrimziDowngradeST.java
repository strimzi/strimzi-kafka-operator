/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade.regular;

import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.annotations.KindIPv6NotSupported;
import io.strimzi.systemtest.annotations.MicroShiftNotSupported;
import io.strimzi.systemtest.resources.NamespaceManager;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.upgrade.AbstractUpgradeST;
import io.strimzi.systemtest.upgrade.BundleVersionModificationData;
import io.strimzi.systemtest.upgrade.UpgradeKafkaVersion;
import io.strimzi.systemtest.upgrade.VersionModificationDataLoader;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.List;

import static io.strimzi.systemtest.Environment.TEST_SUITE_NAMESPACE;
import static io.strimzi.systemtest.TestConstants.CO_NAMESPACE;
import static io.strimzi.systemtest.TestConstants.UPGRADE;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * This test class contains tests for Strimzi downgrade from version X to version X - 1.
 * Metadata for downgrade procedure are available in resource file StrimziDowngrade.json
 * Kafka upgrade is done as part of those tests as well, but the tests for Kafka upgrade/downgrade are in {@link KafkaUpgradeDowngradeST}.
 */
@Tag(UPGRADE)
@KRaftNotSupported("Strimzi and Kafka downgrade is not supported with KRaft mode")
public class StrimziDowngradeST extends AbstractUpgradeST {

    private static final Logger LOGGER = LogManager.getLogger(StrimziDowngradeST.class);
    private final List<BundleVersionModificationData> bundleDowngradeMetadata = new VersionModificationDataLoader(VersionModificationDataLoader.ModificationType.BUNDLE_DOWNGRADE).getBundleUpgradeOrDowngradeDataList();

    @ParameterizedTest(name = "testDowngradeStrimziVersion-{0}-{1}")
    @MethodSource("io.strimzi.systemtest.upgrade.VersionModificationDataLoader#loadYamlDowngradeData")
    void testDowngradeStrimziVersion(String from, String to, BundleVersionModificationData parameters) throws Exception {
        assumeTrue(StUtils.isAllowOnCurrentEnvironment(parameters.getEnvFlakyVariable()));
        assumeTrue(StUtils.isAllowedOnCurrentK8sVersion(parameters.getEnvMaxK8sVersion()));

        LOGGER.debug("Running downgrade test from version {} to {}", from, to);
        performDowngrade(CO_NAMESPACE, TEST_SUITE_NAMESPACE, parameters);
    }

    @MicroShiftNotSupported("Due to lack of Kafka Connect build feature")
    @KindIPv6NotSupported("Our current CI setup doesn't allow pushing into internal registries that is needed in this test")
    @IsolatedTest
    void testDowngradeOfKafkaConnectAndKafkaConnector() throws IOException {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        final BundleVersionModificationData bundleDowngradeDataWithFeatureGates = bundleDowngradeMetadata.stream()
                .filter(bundleMetadata -> bundleMetadata.getFeatureGatesBefore() != null && !bundleMetadata.getFeatureGatesBefore().isEmpty() ||
                        bundleMetadata.getFeatureGatesAfter() != null && !bundleMetadata.getFeatureGatesAfter().isEmpty()).toList().get(0);
        UpgradeKafkaVersion upgradeKafkaVersion = new UpgradeKafkaVersion(bundleDowngradeDataWithFeatureGates.getDeployKafkaVersion());

        doKafkaConnectAndKafkaConnectorUpgradeOrDowngradeProcedure(CO_NAMESPACE, TEST_SUITE_NAMESPACE, bundleDowngradeDataWithFeatureGates, testStorage, upgradeKafkaVersion);
    }

    private void performDowngrade(String clusterOperatorNamespaceName, String componentsNamespaceName, BundleVersionModificationData downgradeData) throws IOException {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        UpgradeKafkaVersion testUpgradeKafkaVersion = UpgradeKafkaVersion.getKafkaWithVersionFromUrl(downgradeData.getFromKafkaVersionsUrl(), downgradeData.getDeployKafkaVersion());

        // Setup env
        // We support downgrade only when you didn't upgrade to new inter.broker.protocol.version and log.message.format.version
        // https://strimzi.io/docs/operators/latest/full/deploying.html#con-target-downgrade-version-str
        setupEnvAndUpgradeClusterOperator(clusterOperatorNamespaceName, componentsNamespaceName, downgradeData, testStorage, testUpgradeKafkaVersion);

        logClusterOperatorPodImage(clusterOperatorNamespaceName);
        logComponentsPodImages(componentsNamespaceName);

        // Check if UTO is used before changing the CO -> used for check for KafkaTopics
        boolean wasUTOUsedBefore = StUtils.isUnidirectionalTopicOperatorUsed(componentsNamespaceName, eoSelector);

        // Downgrade CO
        changeClusterOperator(clusterOperatorNamespaceName, componentsNamespaceName, downgradeData);
        // Wait for Kafka cluster rolling update
        waitForKafkaClusterRollingUpdate(componentsNamespaceName);
        logComponentsPodImages(componentsNamespaceName);
        // Downgrade kafka
        changeKafkaAndLogFormatVersion(componentsNamespaceName, downgradeData);
        // Verify that pods are stable
        PodUtils.verifyThatRunningPodsAreStable(componentsNamespaceName, clusterName);
        checkAllComponentsImages(componentsNamespaceName, downgradeData);
        // Verify upgrade
        verifyProcedure(componentsNamespaceName, downgradeData, testStorage.getContinuousProducerName(), testStorage.getContinuousConsumerName(), wasUTOUsedBefore);
    }

    @BeforeEach
    void setupEnvironment() {
        NamespaceManager.getInstance().createNamespaceAndPrepare(CO_NAMESPACE);
    }

    @AfterEach
    void afterEach() {
        cleanUpKafkaTopics();
        deleteInstalledYamls(CO_NAMESPACE, TEST_SUITE_NAMESPACE, coDir);
        NamespaceManager.getInstance().deleteNamespaceWithWait(CO_NAMESPACE);
        if (!TEST_SUITE_NAMESPACE.equals(CO_NAMESPACE)) {
            NamespaceManager.getInstance().deleteNamespaceWithWait(TEST_SUITE_NAMESPACE);
        }

    }
}
