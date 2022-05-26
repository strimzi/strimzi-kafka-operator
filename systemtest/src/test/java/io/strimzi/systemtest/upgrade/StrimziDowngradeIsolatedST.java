/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.UpgradeDowngradeData;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;

import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.UPGRADE;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * This test class contains tests for Strimzi downgrade from version X to version X - 1.
 * Metadata for downgrade procedure are available in resource file StrimziDowngrade.json
 * Kafka upgrade is done as part of those tests as well, but the tests for Kafka upgrade/downgrade are in {@link KafkaUpgradeDowngradeIsolatedST}.
 */
@Tag(UPGRADE)
@IsolatedSuite
public class StrimziDowngradeIsolatedST extends AbstractUpgradeST {

    private static final Logger LOGGER = LogManager.getLogger(StrimziDowngradeIsolatedST.class);

    @ParameterizedTest(name = "testDowngradeStrimziVersion-{0}-{1}")
    @MethodSource("loadYamlDowngradeData")
    @Tag(INTERNAL_CLIENTS_USED)
    void testDowngradeStrimziVersion(String from, String to, UpgradeDowngradeData parameters, ExtensionContext extensionContext) throws Exception {
        assumeTrue(StUtils.isAllowOnCurrentEnvironment(parameters.getEnvironmentInfo().get("flakyEnvVariable")));
        assumeTrue(StUtils.isAllowedOnCurrentK8sVersion(parameters.getEnvironmentInfo().get("maxK8sVersion")));

        LOGGER.debug("Running downgrade test from version {} to {}", from, to);
        performDowngrade(parameters, extensionContext);
    }

    @SuppressWarnings("MethodLength")
    private void performDowngrade(UpgradeDowngradeData testParameters, ExtensionContext extensionContext) throws IOException {
        String continuousTopicName = "continuous-topic";
        String producerName = "hello-world-producer";
        String consumerName = "hello-world-consumer";
        String continuousConsumerGroup = "continuous-consumer-group";

        // Setup env
        // We support downgrade only when you didn't upgrade to new inter.broker.protocol.version and log.message.format.version
        // https://strimzi.io/docs/operators/latest/full/deploying.html#con-target-downgrade-version-str
        setupEnvAndUpgradeClusterOperator(extensionContext, testParameters, producerName, consumerName, continuousTopicName, continuousConsumerGroup, testParameters.getDeployKafkaVersion(), clusterOperator.getDeploymentNamespace());
        logPodImages(clusterName);
        // Downgrade CO
        changeClusterOperator(testParameters, clusterOperator.getDeploymentNamespace(), extensionContext);
        // Wait for Kafka cluster rolling update
        waitForKafkaClusterRollingUpdate();
        logPodImages(clusterName);
        // Verify that pods are stable
        PodUtils.verifyThatRunningPodsAreStable(clusterName);
        checkAllImages(testParameters);
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
    void afterEach() {
        deleteInstalledYamls(coDir, clusterOperator.getDeploymentNamespace());
        cluster.deleteNamespaces();
    }

    @AfterAll
    void tearDown() {
        clusterOperator.unInstall();
    }
}
