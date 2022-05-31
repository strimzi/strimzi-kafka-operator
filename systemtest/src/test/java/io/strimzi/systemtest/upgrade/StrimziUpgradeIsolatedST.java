/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.UpgradeDowngradeDatalist;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.UpgradeDowngradeData;
import io.strimzi.test.TestUtils;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.Constants.UPGRADE;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.INFRA_NAMESPACE;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
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
public class StrimziUpgradeIsolatedST extends AbstractUpgradeST {

    private static final Logger LOGGER = LogManager.getLogger(StrimziUpgradeIsolatedST.class);

    // TODO: make testUpgradeKafkaWithoutVersion to run upgrade with config from StrimziUpgradeST.json
    // main idea of the test and usage of latestReleasedVersion: upgrade CO from version X, kafka Y, to CO version Z and kafka Y + 1 at the end
    private final String strimziReleaseWithOlderKafkaVersion = "0.23.0";
    private final String strimziReleaseWithOlderKafka = String.format("https://github.com/strimzi/strimzi-kafka-operator/releases/download/%s/strimzi-%s.zip",
            strimziReleaseWithOlderKafkaVersion, strimziReleaseWithOlderKafkaVersion);

    @ParameterizedTest(name = "from: {0} (using FG <{2}>) to: {1} (using FG <{3}>) ")
    @MethodSource("loadYamlUpgradeData")
    @Tag(INTERNAL_CLIENTS_USED)
    void testUpgradeStrimziVersion(String fromVersion, String toVersion, String fgBefore, String fgAfter, UpgradeDowngradeData testParameters, ExtensionContext extensionContext) throws Exception {
        assumeTrue(StUtils.isAllowOnCurrentEnvironment(testParameters.getEnvFlakyVariable()));
        assumeTrue(StUtils.isAllowedOnCurrentK8sVersion(testParameters.getEnvMaxK8sVersion()));

        LOGGER.debug("Running upgrade test from version {} to {} (FG: {} -> {})",
                fromVersion, toVersion, fgBefore, fgAfter);
        performUpgrade(testParameters, extensionContext);
    }

    @Test
    void testUpgradeKafkaWithoutVersion(ExtensionContext extensionContext) throws IOException {
        File dir = FileUtils.downloadAndUnzip(strimziReleaseWithOlderKafka);
        File startKafkaPersistent = new File(dir, "strimzi-" + strimziReleaseWithOlderKafkaVersion + "/examples/kafka/kafka-persistent.yaml");
        File startKafkaVersionsYaml = FileUtils.downloadYaml("https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/" + strimziReleaseWithOlderKafkaVersion + "/kafka-versions.yaml");
        File latestKafkaVersionsYaml = new File(TestUtils.USER_PATH + "//../kafka-versions.yaml");

        coDir = new File(dir, "strimzi-" + strimziReleaseWithOlderKafkaVersion + "/install/cluster-operator/");

        String latestKafkaVersion = getValueForLastKafkaVersionInFile(latestKafkaVersionsYaml, "version");
        String startKafkaVersion = getValueForLastKafkaVersionInFile(startKafkaVersionsYaml, "version");
        String startLogMessageFormat = getValueForLastKafkaVersionInFile(startKafkaVersionsYaml, "format");
        String startInterBrokerProtocol = getValueForLastKafkaVersionInFile(startKafkaVersionsYaml, "protocol");

        // Modify + apply installation files
        copyModifyApply(coDir, clusterOperator.getDeploymentNamespace(), extensionContext, "");
        // Apply Kafka Persistent without version
        LOGGER.info("Deploy Kafka from: {}", startKafkaPersistent.getPath());
        // Change kafka version of it's empty (null is for remove the version)
        cmdKubeClient().applyContent(KafkaUtils.changeOrRemoveKafkaConfiguration(startKafkaPersistent, null, startLogMessageFormat, startInterBrokerProtocol));
        // Wait for readiness
        waitForReadinessOfKafkaCluster();

        assertThat(KafkaUtils.getVersionFromKafkaPodLibs(KafkaResources.kafkaPodName(clusterName, 0)), containsString(startKafkaVersion));

        Map<String, String> operatorSnapshot = DeploymentUtils.depSnapshot(ResourceManager.getCoDeploymentName());
        Map<String, String> zooSnapshot = PodUtils.podSnapshot(clusterOperator.getDeploymentNamespace(), zkSelector);
        Map<String, String> kafkaSnapshot = PodUtils.podSnapshot(clusterOperator.getDeploymentNamespace(), zkSelector);
        Map<String, String> eoSnapshot = DeploymentUtils.depSnapshot(KafkaResources.entityOperatorDeploymentName(clusterName));

        // Update CRDs, CRB, etc.
        kubeClient().getClient().apps().deployments().inNamespace(clusterOperator.getDeploymentNamespace()).withName(ResourceManager.getCoDeploymentName()).delete();

        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(INFRA_NAMESPACE)
            .createInstallation()
            .runBundleInstallation();

        DeploymentUtils.waitTillDepHasRolled(ResourceManager.getCoDeploymentName(), 1, operatorSnapshot);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(clusterOperator.getDeploymentNamespace(), zkSelector, 3, zooSnapshot);
        RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(clusterOperator.getDeploymentNamespace(), zkSelector, 3, kafkaSnapshot);
        DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoSnapshot);

        assertThat(KafkaUtils.getVersionFromKafkaPodLibs(KafkaResources.kafkaPodName(clusterName, 0)), containsString(latestKafkaVersion));
    }

    @Test
    void testUpgradeAcrossVersionsWithUnsupportedKafkaVersion(ExtensionContext extensionContext) throws IOException {
        UpgradeDowngradeDatalist upgradeDatalist = new UpgradeDowngradeDatalist();
        UpgradeDowngradeData acrossUpgradeData = upgradeDatalist.buildDataForUpgradeAcrossVersions();

        String continuousTopicName = "continuous-topic";
        String producerName = "hello-world-producer";
        String consumerName = "hello-world-consumer";
        String continuousConsumerGroup = "continuous-consumer-group";

        // Setup env
        setupEnvAndUpgradeClusterOperator(extensionContext, acrossUpgradeData, producerName, consumerName, continuousTopicName, continuousConsumerGroup, acrossUpgradeData.getDeployKafkaVersion(), clusterOperator.getDeploymentNamespace());
        convertCRDs(acrossUpgradeData.getUrlToConversionTool(), acrossUpgradeData.getToConversionTool(), clusterOperator.getDeploymentNamespace());
        // Make snapshots of all pods
        makeSnapshots();

        // Upgrade CO
        changeClusterOperator(acrossUpgradeData, clusterOperator.getDeploymentNamespace(), extensionContext);
        logPodImages(clusterName);
        //  Upgrade kafka
        changeKafkaAndLogFormatVersion(acrossUpgradeData, extensionContext);
        logPodImages(clusterName);
        checkAllImages(acrossUpgradeData);
        // Verify that pods are stable
        PodUtils.verifyThatRunningPodsAreStable(clusterName);
        // Verify upgrade
        verifyProcedure(acrossUpgradeData, producerName, consumerName, clusterOperator.getDeploymentNamespace());
        // Check errors in CO log
        assertNoCoErrorsLogged(0);
    }

    @Test
    void testUpgradeAcrossVersionsWithNoKafkaVersion(ExtensionContext extensionContext) throws IOException {
        UpgradeDowngradeDatalist upgradeDatalist = new UpgradeDowngradeDatalist();
        UpgradeDowngradeData acrossUpgradeData = upgradeDatalist.buildDataForUpgradeAcrossVersions();

        String continuousTopicName = "continuous-topic";
        String producerName = "hello-world-producer";
        String consumerName = "hello-world-consumer";
        String continuousConsumerGroup = "continuous-consumer-group";

        // Setup env
        setupEnvAndUpgradeClusterOperator(extensionContext, acrossUpgradeData, producerName, consumerName, continuousTopicName, continuousConsumerGroup, null, clusterOperator.getDeploymentNamespace());
        convertCRDs(acrossUpgradeData.getUrlToConversionTool(), acrossUpgradeData.getToConversionTool(), clusterOperator.getDeploymentNamespace());

        // Upgrade CO
        changeClusterOperator(acrossUpgradeData, clusterOperator.getDeploymentNamespace(), extensionContext);
        // Wait till first upgrade finished
        zkPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(clusterOperator.getDeploymentNamespace(), zkSelector, 3, zkPods);
        kafkaPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(clusterOperator.getDeploymentNamespace(), kafkaSelector, 3, kafkaPods);
        eoPods = DeploymentUtils.waitTillDepHasRolled(KafkaResources.entityOperatorDeploymentName(clusterName), 1, eoPods);

        LOGGER.info("Rolling to new images has finished!");
        logPodImages(clusterName);
        //  Upgrade kafka
        changeKafkaAndLogFormatVersion(acrossUpgradeData, extensionContext);
        logPodImages(clusterName);
        checkAllImages(acrossUpgradeData);
        // Verify that pods are stable
        PodUtils.verifyThatRunningPodsAreStable(clusterName);
        // Verify upgrade
        verifyProcedure(acrossUpgradeData, producerName, consumerName, clusterOperator.getDeploymentNamespace());

        // Check errors in CO log
        assertNoCoErrorsLogged(0);
    }

    private UpgradeDowngradeData getDataForStartUpgrade(List<UpgradeDowngradeData> upgradeYaml) throws IOException {
        Collections.reverse(upgradeYaml);

        UpgradeDowngradeData startingVersion = null;

        for (UpgradeDowngradeData item : upgradeYaml) {
            item.setDefaultKafka(item.getDefaultKafkaVersionPerStrimzi());
            startingVersion = item;
            break;
        }
        return startingVersion;
    }

    String getValueForLastKafkaVersionInFile(File kafkaVersions, String field) throws IOException {
        YAMLMapper mapper = new YAMLMapper();
        JsonNode node = mapper.readTree(kafkaVersions);
        ObjectNode kafkaVersionNode = (ObjectNode) node.get(node.size() - 1);

        return kafkaVersionNode.get(field).asText();
    }

    private void performUpgrade(UpgradeDowngradeData testParameters, ExtensionContext extensionContext) throws IOException {
        String continuousTopicName = "continuous-topic";
        String producerName = "hello-world-producer";
        String consumerName = "hello-world-consumer";
        String continuousConsumerGroup = "continuous-consumer-group";

        // Setup env
        setupEnvAndUpgradeClusterOperator(extensionContext, testParameters, producerName, consumerName, continuousTopicName, continuousConsumerGroup, "", clusterOperator.getDeploymentNamespace());

        logPodImages(clusterName);

        // Upgrade CRDs and upgrade CO to 0.24
        if (testParameters.getConversionTool() != null) {
            convertCRDs(testParameters.getUrlToConversionTool(), testParameters.getToConversionTool(), clusterOperator.getDeploymentNamespace());
        }

        // Upgrade CO to HEAD
        logPodImages(clusterName);
        changeClusterOperator(testParameters, clusterOperator.getDeploymentNamespace(), extensionContext);

        if (TestKafkaVersion.supportedVersionsContainsVersion(testParameters.getDefaultKafkaVersionPerStrimzi())) {
            waitForKafkaClusterRollingUpdate();
        }

        logPodImages(clusterName);
        // Upgrade kafka
        changeKafkaAndLogFormatVersion(testParameters, extensionContext);
        logPodImages(clusterName);
        checkAllImages(testParameters);

        // Verify that pods are stable
        PodUtils.verifyThatRunningPodsAreStable(clusterName);
        // Verify upgrade
        verifyProcedure(testParameters, producerName, consumerName, clusterOperator.getDeploymentNamespace());

        // Check errors in CO log
        assertNoCoErrorsLogged(0);
    }

    @BeforeEach
    void setupEnvironment() {
        cluster.createNamespace(clusterOperator.getDeploymentNamespace());
    }

    @AfterEach
    protected void tearDownEnvironmentAfterEach() {
        deleteInstalledYamls(coDir, clusterOperator.getDeploymentNamespace());
        cluster.deleteNamespaces();
    }

    @AfterAll
    void tearDown() {
        clusterOperator.unInstall();
    }
}
