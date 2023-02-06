/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Map;

import static io.strimzi.systemtest.Constants.UPGRADE;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * This test class contains tests for Strimzi upgrade from version X to version X + 1.
 * Metadata for upgrade procedure are available in resource file StrimziUpgrade.json
 * Kafka upgrade is done as part of those tests as well, but the tests for Kafka upgrade/downgrade are in {@link KafkaUpgradeDowngradeIsolatedST}.
 */
@Tag(UPGRADE)
@IsolatedSuite
@KRaftNotSupported("Strimzi and Kafka downgrade is not supported with KRaft mode")
public class StrimziUpgradeIsolatedST extends AbstractUpgradeST {

    private static final Logger LOGGER = LogManager.getLogger(StrimziUpgradeIsolatedST.class);

    @ParameterizedTest(name = "from: {0} (using FG <{2}>) to: {1} (using FG <{3}>) ")
    @MethodSource("io.strimzi.systemtest.upgrade.UpgradeDowngradeData#loadYamlUpgradeData")
    @Tag(INTERNAL_CLIENTS_USED)
    void testUpgradeStrimziVersion(String fromVersion, String toVersion, String fgBefore, String fgAfter, UpgradeDowngradeData upgradeData, ExtensionContext extensionContext) throws Exception {
        assumeTrue(StUtils.isAllowOnCurrentEnvironment(upgradeData.getEnvFlakyVariable()));
        assumeTrue(StUtils.isAllowedOnCurrentK8sVersion(upgradeData.getEnvMaxK8sVersion()));

        LOGGER.debug("Running upgrade test from version {} to {} (FG: {} -> {})",
                fromVersion, toVersion, fgBefore, fgAfter);
        performUpgrade(upgradeData, extensionContext);
    }

    @Test
    void testUpgradeKafkaWithoutVersion(ExtensionContext extensionContext) throws IOException {
        UpgradeDowngradeDatalist upgradeDatalist = new UpgradeDowngradeDatalist();
        UpgradeDowngradeData acrossUpgradeData = upgradeDatalist.buildDataForUpgradeAcrossVersions();

        UpgradeKafkaVersion upgradeKafkaVersion = UpgradeKafkaVersion.getKafkaWithVersionFromUrl(acrossUpgradeData.getFromKafkaVersionsUrl(), acrossUpgradeData.getStartingKafkaVersion());
        upgradeKafkaVersion.setVersion(null);

        TestStorage testStorage = new TestStorage(extensionContext);

        // Setup env
        setupEnvAndUpgradeClusterOperator(extensionContext, acrossUpgradeData, testStorage, upgradeKafkaVersion, clusterOperator.getDeploymentNamespace());

        Map<String, String> zooSnapshot = PodUtils.podSnapshot(clusterOperator.getDeploymentNamespace(), zkSelector);
        Map<String, String> kafkaSnapshot = PodUtils.podSnapshot(clusterOperator.getDeploymentNamespace(), zkSelector);
        Map<String, String> eoSnapshot = DeploymentUtils.depSnapshot(clusterOperator.getDeploymentNamespace(), KafkaResources.entityOperatorDeploymentName(clusterName));

        // Make snapshots of all pods
        makeSnapshots();

        // Upgrade CO
        changeClusterOperator(acrossUpgradeData, clusterOperator.getDeploymentNamespace(), extensionContext);

        logPodImages(clusterName);

        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(clusterOperator.getDeploymentNamespace(), zkSelector, 3, zooSnapshot);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(clusterOperator.getDeploymentNamespace(), zkSelector, 3, kafkaSnapshot);
        DeploymentUtils.waitTillDepHasRolled(clusterOperator.getDeploymentNamespace(), KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoSnapshot);

        logPodImages(clusterName);
        checkAllImages(acrossUpgradeData, clusterOperator.getDeploymentNamespace());

        // Verify that pods are stable
        PodUtils.verifyThatRunningPodsAreStable(clusterOperator.getDeploymentNamespace(), clusterName);
        // Verify upgrade
        verifyProcedure(acrossUpgradeData, testStorage.getProducerName(), testStorage.getConsumerName(), clusterOperator.getDeploymentNamespace());
        assertThat(KafkaUtils.getVersionFromKafkaPodLibs(KafkaResources.kafkaPodName(clusterName, 0)), containsString(acrossUpgradeData.getProcedures().getVersion()));
        // Check errors in CO log
        assertNoCoErrorsLogged(clusterOperator.getDeploymentNamespace(), 0);
    }

    @Test
    void testUpgradeAcrossVersionsWithUnsupportedKafkaVersion(ExtensionContext extensionContext) throws IOException {
        UpgradeDowngradeDatalist upgradeDatalist = new UpgradeDowngradeDatalist();
        UpgradeDowngradeData acrossUpgradeData = upgradeDatalist.buildDataForUpgradeAcrossVersions();

        TestStorage testStorage = new TestStorage(extensionContext);
        UpgradeKafkaVersion upgradeKafkaVersion = UpgradeKafkaVersion.getKafkaWithVersionFromUrl(acrossUpgradeData.getFromKafkaVersionsUrl(), acrossUpgradeData.getStartingKafkaVersion());

        // Setup env
        setupEnvAndUpgradeClusterOperator(extensionContext, acrossUpgradeData, testStorage, upgradeKafkaVersion, clusterOperator.getDeploymentNamespace());

        // Make snapshots of all pods
        makeSnapshots();

        // Upgrade CO
        changeClusterOperator(acrossUpgradeData, clusterOperator.getDeploymentNamespace(), extensionContext);
        logPodImages(clusterName);
        //  Upgrade kafka
        changeKafkaAndLogFormatVersion(acrossUpgradeData, extensionContext);
        logPodImages(clusterName);
        checkAllImages(acrossUpgradeData, clusterOperator.getDeploymentNamespace());
        // Verify that pods are stable
        PodUtils.verifyThatRunningPodsAreStable(clusterOperator.getDeploymentNamespace(), clusterName);
        // Verify upgrade
        verifyProcedure(acrossUpgradeData, testStorage.getProducerName(), testStorage.getConsumerName(), clusterOperator.getDeploymentNamespace());
        // Check errors in CO log
        assertNoCoErrorsLogged(clusterOperator.getDeploymentNamespace(), 0);
    }

    @Test
    void testUpgradeAcrossVersionsWithNoKafkaVersion(ExtensionContext extensionContext) throws IOException {
        UpgradeDowngradeDatalist upgradeDatalist = new UpgradeDowngradeDatalist();
        UpgradeDowngradeData acrossUpgradeData = upgradeDatalist.buildDataForUpgradeAcrossVersions();
        TestStorage testStorage = new TestStorage(extensionContext);

        // Setup env
        setupEnvAndUpgradeClusterOperator(extensionContext, acrossUpgradeData, testStorage, null, clusterOperator.getDeploymentNamespace());

        // Upgrade CO
        changeClusterOperator(acrossUpgradeData, clusterOperator.getDeploymentNamespace(), extensionContext);
        // Wait till first upgrade finished
        zkPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(clusterOperator.getDeploymentNamespace(), zkSelector, 3, zkPods);
        kafkaPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(clusterOperator.getDeploymentNamespace(), kafkaSelector, 3, kafkaPods);
        eoPods = DeploymentUtils.waitTillDepHasRolled(clusterOperator.getDeploymentNamespace(), KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoPods);

        LOGGER.info("Rolling to new images has finished!");
        logPodImages(clusterName);
        //  Upgrade kafka
        changeKafkaAndLogFormatVersion(acrossUpgradeData, extensionContext);
        logPodImages(clusterName);
        checkAllImages(acrossUpgradeData, clusterOperator.getDeploymentNamespace());
        // Verify that pods are stable
        PodUtils.verifyThatRunningPodsAreStable(clusterOperator.getDeploymentNamespace(), clusterName);
        // Verify upgrade
        verifyProcedure(acrossUpgradeData, testStorage.getProducerName(), testStorage.getConsumerName(), clusterOperator.getDeploymentNamespace());

        // Check errors in CO log
        assertNoCoErrorsLogged(clusterOperator.getDeploymentNamespace(), 0);
    }

    private void performUpgrade(UpgradeDowngradeData upgradeData, ExtensionContext extensionContext) throws IOException {
        TestStorage testStorage = new TestStorage(extensionContext);
        // leave empty, so the original Kafka version from appropriate Strimzi's yaml will be used
        UpgradeKafkaVersion upgradeKafkaVersion = new UpgradeKafkaVersion();

        // Setup env
        setupEnvAndUpgradeClusterOperator(extensionContext, upgradeData, testStorage, upgradeKafkaVersion, clusterOperator.getDeploymentNamespace());

        // Upgrade CO to HEAD
        logPodImages(clusterName);
        changeClusterOperator(upgradeData, clusterOperator.getDeploymentNamespace(), extensionContext);

        if (TestKafkaVersion.supportedVersionsContainsVersion(upgradeData.getDefaultKafkaVersionPerStrimzi())) {
            waitForKafkaClusterRollingUpdate();
        }

        logPodImages(clusterName);
        // Upgrade kafka
        changeKafkaAndLogFormatVersion(upgradeData, extensionContext);
        logPodImages(clusterName);
        checkAllImages(upgradeData, clusterOperator.getDeploymentNamespace());

        // Verify that pods are stable
        PodUtils.verifyThatRunningPodsAreStable(clusterOperator.getDeploymentNamespace(), clusterName);
        // Verify upgrade
        verifyProcedure(upgradeData, testStorage.getProducerName(), testStorage.getConsumerName(), clusterOperator.getDeploymentNamespace());

        // Check errors in CO log
        assertNoCoErrorsLogged(clusterOperator.getDeploymentNamespace(), 0);
    }

    @BeforeEach
    void setupEnvironment() {
        cluster.createNamespace(clusterOperator.getDeploymentNamespace());
    }

    protected void afterEachMayOverride(ExtensionContext extensionContext) throws Exception {
        deleteInstalledYamls(coDir, clusterOperator.getDeploymentNamespace());

        // delete all topics created in test
        cmdKubeClient(clusterOperator.getDeploymentNamespace()).deleteAllByResource(KafkaTopic.RESOURCE_KIND);
        KafkaTopicUtils.waitForTopicWithPrefixDeletion(clusterOperator.getDeploymentNamespace(), topicName);

        ResourceManager.getInstance().deleteResources(extensionContext);
        cluster.deleteNamespaces();
    }

    @AfterAll
    void tearDown() {
        clusterOperator.unInstall();
    }
}
