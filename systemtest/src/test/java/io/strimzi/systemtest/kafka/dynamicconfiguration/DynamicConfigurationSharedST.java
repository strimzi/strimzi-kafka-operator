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
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.extension.ExtensionContext;

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
    private final String dynamicConfigurationSharedClusterName = "dynamic-configuration-shared-cluster-name";

    @TestFactory
    Iterator<DynamicTest> testDynConfiguration() {

        List<DynamicTest> dynamicTests = new ArrayList<>(40);

        Map<String, Object> testCases = generateTestCases(TestKafkaVersion.getKafkaVersionsInMap().get(Environment.ST_KAFKA_VERSION).version());

        testCases.forEach((key, value) -> dynamicTests.add(DynamicTest.dynamicTest("Test " + key + "->" + value, () -> {
            // exercise phase
            KafkaUtils.updateConfigurationWithStabilityWait(dynamicConfigurationSharedClusterName, key, value);

            // verify phase
            assertThat(KafkaUtils.verifyCrDynamicConfiguration(dynamicConfigurationSharedClusterName, key, value), is(true));
            assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(dynamicConfigurationSharedClusterName), key, value), is(true));
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
                        case "ssl.protocol":
                            stochasticChosenValue = "TLSv1.1";
                            break;
                        default:
                            stochasticChosenValue = " ";
                    }
                    testCases.put(key, stochasticChosenValue);
                    break;
                case INT:
                case LONG:
                    switch (key) {
                        case "num.recovery.threads.per.data.dir":
                        case "log.cleaner.threads":
                        case "num.network.threads":
                        case "min.insync.replicas":
                        case "num.replica.fetchers":
                        case "num.partitions":
                            stochasticChosenValue = ThreadLocalRandom.current().nextInt(2, 3);
                            break;
                        case "log.cleaner.io.buffer.load.factor":
                        case "log.retention.ms":
                        case "max.connections":
                        case "max.connections.per.ip":
                        case "background.threads":
                            stochasticChosenValue = ThreadLocalRandom.current().nextInt(4, 20);
                            break;
                        default:
                            stochasticChosenValue = ThreadLocalRandom.current().nextInt(100, 50_000);
                    }
                    testCases.put(key, stochasticChosenValue);
                    break;
                case DOUBLE:
                    switch (key) {
                        case "log.cleaner.min.cleanable.dirty.ratio":
                        case "log.cleaner.min.cleanable.ratio":
                            stochasticChosenValue = ThreadLocalRandom.current().nextDouble(0, 1);
                            break;
                        default:
                            stochasticChosenValue = ThreadLocalRandom.current().nextDouble(1, 20);
                    }
                    testCases.put(key, stochasticChosenValue);
                    break;
                case BOOLEAN:
                    switch (key) {
                        case "unclean.leader.election.enable":
                        case "log.preallocate":
                            stochasticChosenValue = true;
                            break;
                        case "log.message.downconversion.enable":
                            stochasticChosenValue = false;
                            break;
                        default:
                            stochasticChosenValue = ThreadLocalRandom.current().nextInt(2) == 0 ? true : false;
                    }
                    testCases.put(key, stochasticChosenValue);
                    break;
                case LIST:
                    // metric.reporters has default empty '""'
                    // log.cleanup.policy = [delete, compact] -> default delete
                    switch (key) {
                        case "log.cleanup.policy":
                            stochasticChosenValue = "compact";
                            break;
                        case "ssl.enabled.protocols":
                            stochasticChosenValue = "TLSv1.1";
                            break;
                        default:
                            stochasticChosenValue = " ";
                    }
                    testCases.put(key, stochasticChosenValue);
            }

            // skipping these configuration, which doesn't work appear in the kafka pod (TODO: investigate why!)
            testCases.remove("num.recovery.threads.per.data.dir");
            testCases.remove("num.io.threads");
            testCases.remove("log.cleaner.dedupe.buffer.size");
            testCases.remove("num.partitions");

            // skipping these configuration exceptions
            testCases.remove("ssl.cipher.suites");
            testCases.remove("zookeeper.connection.timeout.ms");
            testCases.remove("zookeeper.connect");
        });

        return testCases;
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        installClusterOperator(extensionContext, NAMESPACE);

        LOGGER.info("Deploying shared Kafka across all test cases!");
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(dynamicConfigurationSharedClusterName, 3).build());
    }
}
