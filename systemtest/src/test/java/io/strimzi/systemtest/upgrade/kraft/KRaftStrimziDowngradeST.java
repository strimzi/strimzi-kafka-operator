/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade.kraft;

import io.strimzi.systemtest.annotations.KindIPv6NotSupported;
import io.strimzi.systemtest.annotations.MicroShiftNotSupported;
import io.strimzi.systemtest.resources.NamespaceManager;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.upgrade.BundleVersionModificationData;
import io.strimzi.systemtest.upgrade.UpgradeKafkaVersion;
import io.strimzi.systemtest.utils.StUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;

import static io.strimzi.systemtest.Environment.TEST_SUITE_NAMESPACE;
import static io.strimzi.systemtest.TestConstants.CO_NAMESPACE;
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

    @MicroShiftNotSupported("Due to lack of Kafka Connect build feature")
    @KindIPv6NotSupported("Our current CI setup doesn't allow pushing into internal registries that is needed in this test")
    @ParameterizedTest(name = "from: {0} (using FG <{2}>) to: {1} (using FG <{3}>)")
    @MethodSource("io.strimzi.systemtest.upgrade.VersionModificationDataLoader#loadYamlDowngradeDataForKRaft")
    void testDowngradeOfKafkaKafkaConnectAndKafkaConnector(String from, String to, String fgBefore, String fgAfter, BundleVersionModificationData downgradeData) throws IOException {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        UpgradeKafkaVersion downgradeKafkaVersion = UpgradeKafkaVersion.getKafkaWithVersionFromUrl(downgradeData.getFromKafkaVersionsUrl(), downgradeData.getDeployKafkaVersion());
        // setting log message version to null, similarly to the examples, which are not configuring LMFV
        downgradeKafkaVersion.setLogMessageVersion(null);

        assumeTrue(StUtils.isAllowOnCurrentEnvironment(downgradeData.getEnvFlakyVariable()));
        assumeTrue(StUtils.isAllowedOnCurrentK8sVersion(downgradeData.getEnvMaxK8sVersion()));

        LOGGER.debug("Running downgrade test from version {} to {} (FG: {} -> {})", from, to, fgBefore, fgAfter);

        doKafkaConnectAndKafkaConnectorUpgradeOrDowngradeProcedure(CO_NAMESPACE, testStorage, downgradeData, downgradeKafkaVersion);
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
