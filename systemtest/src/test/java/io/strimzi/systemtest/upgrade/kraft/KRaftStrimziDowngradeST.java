/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade.kraft;

import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.NamespaceManager;
import io.strimzi.systemtest.storage.TestStorage;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;

import static io.strimzi.systemtest.TestConstants.CO_NAMESPACE;
import static io.strimzi.systemtest.TestConstants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestConstants.KRAFT_UPGRADE;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Class for testing downgrade process of Strimzi with its components when running in KRaft mode
 *      -> KRaft to KRaft downgrades
 * Metadata for the following tests are collected from systemtest/src/test/resources/upgrade/BundleDowngrade.yaml
 */
@Tag(KRAFT_UPGRADE)
public class KRaftStrimziDowngradeST extends AbstractKRaftUpgradeST {
    private static final Logger LOGGER = LogManager.getLogger(KRaftStrimziDowngradeST.class);
    private final BundleVersionModificationData bundleDowngradeVersionData = new VersionModificationDataLoader(VersionModificationDataLoader.ModificationType.BUNDLE_DOWNGRADE).buildDataForDowngradeUsingFirstScenarioForKRaft();

    @ParameterizedTest(name = "testDowngradeStrimziVersion-{0}-{1}")
    @MethodSource("io.strimzi.systemtest.upgrade.VersionModificationDataLoader#loadYamlDowngradeDataForKRaft")
    @Tag(INTERNAL_CLIENTS_USED)
    void testDowngradeStrimziVersion(String from, String to, BundleVersionModificationData parameters, ExtensionContext extensionContext) throws Exception {
        assumeTrue(StUtils.isAllowOnCurrentEnvironment(parameters.getEnvFlakyVariable()));
        assumeTrue(StUtils.isAllowedOnCurrentK8sVersion(parameters.getEnvMaxK8sVersion()));

        LOGGER.debug("Running downgrade test from version {} to {}", from, to);
        performDowngrade(parameters, extensionContext);
    }

    @Test
    void testDowngradeOfKafkaConnectAndKafkaConnector(final ExtensionContext extensionContext) throws IOException {
        final TestStorage testStorage = new TestStorage(extensionContext, TestConstants.CO_NAMESPACE);
        UpgradeKafkaVersion upgradeKafkaVersion = new UpgradeKafkaVersion(bundleDowngradeVersionData.getDeployKafkaVersion());

        doKafkaConnectAndKafkaConnectorUpgradeOrDowngradeProcedure(extensionContext, bundleDowngradeVersionData, testStorage, upgradeKafkaVersion);
    }

    @SuppressWarnings("MethodLength")
    private void performDowngrade(BundleVersionModificationData downgradeData, ExtensionContext extensionContext) throws IOException {
        TestStorage testStorage = new TestStorage(extensionContext);

        String lowerMetadataVersion = downgradeData.getProcedures().getMetadataVersion();
        UpgradeKafkaVersion testUpgradeKafkaVersion = new UpgradeKafkaVersion(downgradeData.getDeployKafkaVersion(), lowerMetadataVersion);

        // Setup env
        // We support downgrade only when you didn't upgrade to new inter.broker.protocol.version and log.message.format.version
        // https://strimzi.io/docs/operators/latest/full/deploying.html#con-target-downgrade-version-str

        setupEnvAndUpgradeClusterOperator(extensionContext, downgradeData, testStorage, testUpgradeKafkaVersion, TestConstants.CO_NAMESPACE);

        logPodImages(TestConstants.CO_NAMESPACE);

        boolean wasUTOUsedBefore = StUtils.isUnidirectionalTopicOperatorUsed(TestConstants.CO_NAMESPACE, eoSelector);

        // Downgrade CO
        changeClusterOperator(downgradeData, TestConstants.CO_NAMESPACE, extensionContext);

        // Wait for Kafka cluster rolling update
        waitForKafkaClusterRollingUpdate();

        logPodImages(TestConstants.CO_NAMESPACE);

        // Downgrade kafka
        changeKafkaAndMetadataVersion(downgradeData, extensionContext);

        // Verify that pods are stable
        PodUtils.verifyThatRunningPodsAreStable(TestConstants.CO_NAMESPACE, clusterName);

        checkAllImages(downgradeData, TestConstants.CO_NAMESPACE);

        // Verify upgrade
        verifyProcedure(downgradeData, testStorage.getProducerName(), testStorage.getConsumerName(), TestConstants.CO_NAMESPACE, wasUTOUsedBefore);
    }

    @BeforeEach
    void setupEnvironment() {
        NamespaceManager.getInstance().createNamespaceAndPrepare(CO_NAMESPACE);
    }

    @AfterEach
    void afterEach() {
        cleanUpKafkaTopics();
        deleteInstalledYamls(coDir, TestConstants.CO_NAMESPACE);
        NamespaceManager.getInstance().deleteNamespaceWithWait(CO_NAMESPACE);
    }
}
