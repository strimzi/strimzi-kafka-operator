/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.dynamicconfiguration;

import io.strimzi.api.kafka.model.KafkaResources;
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
import java.util.Iterator;
import java.util.LinkedHashMap;
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

        String generatedTestCases = generateTestCases(TestKafkaVersion.getKafkaVersionsInMap().get(Environment.ST_KAFKA_VERSION).version());
        String[] testCases = generatedTestCases.split("\n");

        for (String testCaseLine : testCases) {
            String[] testCase = testCaseLine.split(",");
            dynamicTests.add(DynamicTest.dynamicTest("Test " + testCase[0] + "->" + testCase[1], () -> {
                // exercise phase
                KafkaUtils.updateConfigurationWithStabilityWait(CLUSTER_NAME, testCase[0], testCase[1]);

                // verify phase
                assertThat(KafkaUtils.verifyCrDynamicConfiguration(CLUSTER_NAME, testCase[0], testCase[1]), is(true));
                assertThat(KafkaUtils.verifyPodDynamicConfiguration(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), testCase[0], testCase[1]), is(true));
            }));
        }

        return dynamicTests.iterator();
    }

    /**
     * Method, which dynamically generate test cases based on Kafka version
     * @param kafkaVersion specific kafka version
     * @return String generated test cases
     */
    private static String generateTestCases(String kafkaVersion) {

        StringBuilder testCases = new StringBuilder();

        Map<String, Object> dynamicProperties = KafkaUtils.getDynamicConfigurationProperties(kafkaVersion);

        dynamicProperties.forEach((key, value) -> {
            testCases.append(key);
            testCases.append(", ");

            String type = ((LinkedHashMap<String, String>) value).get("type");
            Object stochasticChosenValue;

            switch (type) {
                case "STRING":
                    if (key.equals("compression.type")) {
                        List<String> compressionTypes = Arrays.asList("snappy", "gzip", "lz4", "zstd");

                        stochasticChosenValue = compressionTypes.get(ThreadLocalRandom.current().nextInt(0, compressionTypes.size() - 1));
                        testCases.append(stochasticChosenValue);
                    } else {
                        testCases.append(" ");
                    }
                    break;
                case "INT":
                case "LONG":
                    if (key.equals("background.threads") || key.equals("log.cleaner.io.buffer.load.factor") ||
                        key.equals("log.retention.ms") || key.equals("max.connections") ||
                        key.equals("max.connections.per.ip")) {
                        stochasticChosenValue = ThreadLocalRandom.current().nextInt(1, 20);
                    } else {
                        stochasticChosenValue = ThreadLocalRandom.current().nextInt(100, 50_000);
                    }
                    testCases.append(stochasticChosenValue);
                    break;
                case "DOUBLE":
                    stochasticChosenValue = ThreadLocalRandom.current().nextDouble(1, 20);
                    testCases.append(stochasticChosenValue);
                    break;
                case "BOOLEAN":
                    stochasticChosenValue = ThreadLocalRandom.current().nextInt(2) == 0 ? true : false;
                    testCases.append(stochasticChosenValue);
                    break;
                case "LIST":
                    // metric.reporters has default empty '""'
                    // log.cleanup.policy = [delete, compact] -> default delete

                    if (key.equals("log.cleanup.policy")) {
                        stochasticChosenValue = "[delete]";
                    } else {
                        stochasticChosenValue = " ";
                    }

                    testCases.append(stochasticChosenValue);
            }
            testCases.append(",");
            testCases.append("\n");
        });

        return testCases.toString();
    }

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);

        LOGGER.info("Deploying shared Kafka across all test cases!");
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1).done();
    }
}
