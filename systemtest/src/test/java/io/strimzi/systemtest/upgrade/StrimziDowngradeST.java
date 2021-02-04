/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import io.strimzi.systemtest.logs.TestExecutionWatcher;
import io.strimzi.systemtest.utils.StUtils;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;

import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.UPGRADE;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag(UPGRADE)
public class StrimziDowngradeST extends AbstractUpgradeST {

    private static final Logger LOGGER = LogManager.getLogger(StrimziDowngradeST.class);

    public static final String NAMESPACE = "strimzi-downgrade-test";

    @ParameterizedTest(name = "testDowngradeStrimziVersion-{0}-{1}")
    @MethodSource("loadJsonDowngradeData")
    @Tag(INTERNAL_CLIENTS_USED)
    void testDowngradeStrimziVersion(String from, String to, JsonObject parameters) throws Exception {

        assumeTrue(StUtils.isAllowOnCurrentEnvironment(parameters.getJsonObject("environmentInfo").getString("flakyEnvVariable")));
        assumeTrue(StUtils.isAllowedOnCurrentK8sVersion(parameters.getJsonObject("environmentInfo").getString("maxK8sVersion")));

        LOGGER.debug("Running downgrade test from version {} to {}", from, to);

        try {
            performDowngrade(parameters, MESSAGE_COUNT, MESSAGE_COUNT);
            // Tidy up
        } catch (Exception e) {
            e.printStackTrace();
            TestExecutionWatcher.collectLogs(testClass, testName);
            try {
                if (kafkaYaml != null) {
                    cmdKubeClient().delete(kafkaYaml);
                }
            } catch (Exception ex) {
                LOGGER.warn("Failed to delete resources: {}", kafkaYaml.getName());
            }
            try {
                if (coDir != null) {
                    cmdKubeClient().delete(coDir);
                }
            } catch (Exception ex) {
                LOGGER.warn("Failed to delete resources: {}", coDir.getName());
            }

            throw e;
        } finally {
            deleteInstalledYamls(coDir, NAMESPACE);
        }
    }

    @SuppressWarnings("MethodLength")
    private void performDowngrade(JsonObject testParameters, int produceMessagesCount, int consumeMessagesCount) throws IOException {
        String continuousTopicName = "continuous-topic";
        String producerName = "hello-world-producer";
        String consumerName = "hello-world-consumer";
        String continuousConsumerGroup = "continuous-consumer-group";

        // Setup env
        setupEnvAndUpgradeClusterOperator(testParameters, produceMessagesCount, consumeMessagesCount, producerName, consumerName, continuousTopicName, continuousConsumerGroup, "", NAMESPACE);
        logPodImages(clusterName);
        //  Upgrade kafka
        changeKafkaAndLogFormatVersion(testParameters.getJsonObject("proceduresBeforeOperatorDowngrade"), clusterName, NAMESPACE);
        // Downgrade CO
        changeClusterOperator(testParameters, NAMESPACE);
        // Wait for Kafka cluster rolling update
        waitForKafkaClusterRollingUpdate();
        logPodImages(clusterName);
        checkAllImages(testParameters.getJsonObject("imagesAfterOperatorDowngrade"));
        // Verify upgrade
        verifyProcedure(testParameters, produceMessagesCount, consumeMessagesCount, producerName, consumerName, NAMESPACE);
        // Check errors in CO log
        assertNoCoErrorsLogged(0);
    }

    @BeforeEach
    void setupEnvironment() {
        cluster.createNamespace(NAMESPACE);
    }

    @Override
    protected void tearDownEnvironmentAfterEach() {
        cluster.deleteNamespaces();
    }

    // There is no value of having teardown logic for class resources due to the fact that
    // CO was deployed by method StrimziUpgradeST.copyModifyApply() and removed by method StrimziUpgradeST.deleteInstalledYamls()
    @Override
    protected void tearDownEnvironmentAfterAll() {
    }
}
