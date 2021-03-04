/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.vertx.core.json.JsonObject;
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
 * Kafka upgrade is done as part of those tests as well, but the tests for Kafka upgrade/downgrade are in {@link KafkaUpgradeDowngradeST}.
 */
@Tag(UPGRADE)
public class StrimziDowngradeST extends AbstractUpgradeST {

    private static final Logger LOGGER = LogManager.getLogger(StrimziDowngradeST.class);

    public static final String NAMESPACE = "strimzi-downgrade-test";

    @ParameterizedTest(name = "testDowngradeStrimziVersion-{0}-{1}")
    @MethodSource("loadJsonDowngradeData")
    @Tag(INTERNAL_CLIENTS_USED)
    void testDowngradeStrimziVersion(String from, String to, JsonObject parameters, ExtensionContext extensionContext) throws Exception {

        assumeTrue(StUtils.isAllowOnCurrentEnvironment(parameters.getJsonObject("environmentInfo").getString("flakyEnvVariable")));
        assumeTrue(StUtils.isAllowedOnCurrentK8sVersion(parameters.getJsonObject("environmentInfo").getString("maxK8sVersion")));

        LOGGER.debug("Running downgrade test from version {} to {}", from, to);
        performDowngrade(parameters, extensionContext);
    }

    @SuppressWarnings("MethodLength")
    private void performDowngrade(JsonObject testParameters, ExtensionContext extensionContext) throws IOException {
        String continuousTopicName = "continuous-topic";
        String producerName = "hello-world-producer";
        String consumerName = "hello-world-consumer";
        String continuousConsumerGroup = "continuous-consumer-group";

        // Setup env
        setupEnvAndUpgradeClusterOperator(extensionContext, testParameters, producerName, consumerName, continuousTopicName, continuousConsumerGroup, "", NAMESPACE);
        logPodImages(clusterName);
        //  Upgrade kafka
        changeKafkaAndLogFormatVersion(testParameters.getJsonObject("proceduresBeforeOperatorDowngrade"), testParameters, clusterName, extensionContext);
        // Downgrade CO
        changeClusterOperator(testParameters, NAMESPACE);
        // Wait for Kafka cluster rolling update
        waitForKafkaClusterRollingUpdate();
        logPodImages(clusterName);
        // Verify that pods are stable
        PodUtils.verifyThatRunningPodsAreStable(clusterName);
        checkAllImages(testParameters.getJsonObject("imagesAfterOperatorDowngrade"));
        // Verify upgrade
        verifyProcedure(testParameters, producerName, consumerName, NAMESPACE);
        // Check errors in CO log
        assertNoCoErrorsLogged(0);
    }

    @BeforeEach
    void setupEnvironment() {
        cluster.createNamespace(NAMESPACE);
    }

    @AfterEach
    void afterEach() {
        deleteInstalledYamls(coDir, NAMESPACE);
        cluster.deleteNamespaces();
    }

    // There is no value of having teardown logic for class resources due to the fact that
    // CO was deployed by method StrimziUpgradeST.copyModifyApply() and removed by method StrimziUpgradeST.deleteInstalledYamls()
    @AfterAll
    protected void tearDownEnvironmentAfterAll() { }
}
