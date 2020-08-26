/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka.dynamicconfiguration;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.kafka.config.model.ConfigModel;
import io.strimzi.kafka.config.model.Type;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static io.strimzi.systemtest.Constants.DYNAMIC_CONFIGURATION;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

/**
 * DynamicConfigurationSharedST is responsible for verify that if we change dynamic Kafka configuration it will not
 * trigger rolling update
 * Shared -> for each test case we same configuration of Kafka resource
 */
@Tag(REGRESSION)
@Tag(DYNAMIC_CONFIGURATION)
public class DynamicConfigurationSharedST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(DynamicConfigurationSharedST.class);
    private static final String NAMESPACE = "kafka-configuration-shared-cluster-test";

    @TestFactory
    Iterator<DynamicTest> testDynConfiguration() {

        List<DynamicTest> dynamicTests = new ArrayList<>(40);

        Map<String, Object> testCases = generateTestCases(TestKafkaVersion.getKafkaVersionsInMap().get(Environment.ST_KAFKA_VERSION).version());

        testCases.forEach((key, value) -> dynamicTests.add(DynamicTest.dynamicTest("Test " + key + "->" + value, () -> {
            // exercise phase
            KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, key, value);

            // verify phase
            assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, key, value), is(true));
            assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), key, value), is(true));
        })));
        return dynamicTests.iterator();
    }

    /**
     * Method, which dynamically generate test cases based on Kafka version
     * @param kafkaVersion specific kafka version
     * @return String generated test cases
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity"})
    private static Map<String, Object> generateTestCases(String kafkaVersion) {

        Map<String, ConfigModel> dynamicProperties = KafkaUtils.getDynamicConfigurationProperties(kafkaVersion);
        Map<String, Object> testCases = new HashMap<>();

        dynamicProperties.forEach((key, value) -> {

            Type type = value.getType();
            Object stochasticChosenValue;

            switch (type) {
                case STRING:
                    switch (key) {
                        case "compression.type":
                            List<String> compressionTypes = Arrays.asList("snappy", "gzip", "lz4", "zstd");

                            stochasticChosenValue = compressionTypes.get(ThreadLocalRandom.current().nextInt(0, compressionTypes.size() - 1));
                            break;
                        case "log.message.timestamp.type":
                            stochasticChosenValue = "LogAppendTime";
                            break;
                        default:
                            stochasticChosenValue = " ";
                    }
                    testCases.put(key, stochasticChosenValue);
                    break;
                case INT:
                case LONG:
                    if (key.equals("num.recovery.threads.per.data.dir") || key.equals("log.cleaner.threads") ||
                        key.equals("num.network.threads") || key.equals("min.insync.replicas") ||
                        key.equals("num.replica.fetchers") || key.equals("num.partitions")) {
                        stochasticChosenValue = ThreadLocalRandom.current().nextInt(2, 3);
                    } else if (key.equals("log.cleaner.io.buffer.load.factor") ||
                        key.equals("log.retention.ms") || key.equals("max.connections") ||
                        key.equals("max.connections.per.ip") || key.equals("background.threads")) {
                        stochasticChosenValue = ThreadLocalRandom.current().nextInt(1, 20);
                    } else {
                        stochasticChosenValue = ThreadLocalRandom.current().nextInt(100, 50_000);
                    }
                    testCases.put(key, stochasticChosenValue);
                    break;
                case DOUBLE:
                    if (key.equals("log.cleaner.min.cleanable.dirty.ratio") ||
                        key.equals("log.cleaner.min.cleanable.ratio")) {
                        stochasticChosenValue = ThreadLocalRandom.current().nextDouble(0, 1);
                    } else {
                        stochasticChosenValue = ThreadLocalRandom.current().nextDouble(1, 20);
                    }
                    testCases.put(key, stochasticChosenValue);
                    break;
                case BOOLEAN:
                    if (key.equals("unclean.leader.election.enable") || key.equals("log.preallocate")) {
                        stochasticChosenValue = true;
                    } else {
                        stochasticChosenValue = ThreadLocalRandom.current().nextInt(2) == 0 ? true : false;
                    }
                    testCases.put(key, stochasticChosenValue);
                    break;
                case LIST:
                    // metric.reporters has default empty '""'
                    // log.cleanup.policy = [delete, compact] -> default delete

                    switch (key) {
                        case "log.cleanup.policy":
                            stochasticChosenValue = "delete";
                            break;
                        default:
                            stochasticChosenValue = " ";
                    }
                    testCases.put(key, stochasticChosenValue);
            }

            // skipping these configuration, which doesn't work appear in the kafka pod (TODO: investigate why!)
            testCases.remove("log.message.downconversion.enable");
            testCases.remove("log.roll.ms");
            testCases.remove("num.recovery.threads.per.data.dir");
            testCases.remove("log.cleanup.policy");
            testCases.remove("num.io.threads");
            testCases.remove("log.cleaner.dedupe.buffer.size");
            testCases.remove("max.connections");
            testCases.remove("background.threads");
            testCases.remove("num.partitions");
            testCases.remove("unclean.leader.election.enable");

            // skipping these configuration exceptions
            testCases.remove("ssl.enabled.protocols");
            testCases.remove("ssl.protocol");
            testCases.remove("ssl.cipher.suites");
            testCases.remove("zookeeper.connection.timeout.ms");
            testCases.remove("zookeeper.connect");
        });

        return testCases;
    }

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);

        LOGGER.info("Deploying shared Kafka across all test cases!");
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3).done();
    }
}
