/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.test.ClusterOperator;
import io.strimzi.test.Namespace;
import io.strimzi.test.StrimziExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static io.strimzi.test.k8s.BaseKubeClient.STATEFUL_SET;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(StrimziExtension.class)
@Namespace(LogLevelST.NAMESPACE)
@ClusterOperator
class LogLevelST extends AbstractNewST {
    static final String NAMESPACE = "log-level-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(LogLevelST.class);
    private static final String TESTED_LOGGER = "kafka.root.logger.level";
    private static final String POD_NAME = kafkaClusterName(CLUSTER_NAME) + "-0";

    @Test
    void testLogLevelInfo() {
        String logLevel = "INFO";
        LOGGER.info("Running testLogLevelInfo in namespace {}", NAMESPACE);
        createKafkaPods(logLevel);
        assertTrue(checkKafkaLogLevel(logLevel), "Kafka's log level is set properly");
    }

    @Test
    @Tag("release")
    void testLogLevelError() {
        String logLevel = "ERROR";
        LOGGER.info("Running testLogLevelInfo in namespace {}", NAMESPACE);
        createKafkaPods(logLevel);
        assertTrue(checkKafkaLogLevel(logLevel), "Kafka's log level is set properly");
    }

    @Test
    @Tag("release")
    void testLogLevelWarn() {
        String logLevel = "WARN";
        LOGGER.info("Running testLogLevelInfo in namespace {}", NAMESPACE);
        createKafkaPods(logLevel);
        assertTrue(checkKafkaLogLevel(logLevel), "Kafka's log level is set properly");
    }

    @Test
    @Tag("release")
    void testLogLevelTrace() {
        String logLevel = "TRACE";
        LOGGER.info("Running testLogLevelInfo in namespace {}", NAMESPACE);
        createKafkaPods(logLevel);
        assertTrue(checkKafkaLogLevel(logLevel), "Kafka's log level is set properly");
    }

    @Test
    @Tag("release")
    void testLogLevelDebug() {
        String logLevel = "DEBUG";
        LOGGER.info("Running testLogLevelInfo in namespace {}", NAMESPACE);
        createKafkaPods(logLevel);
        assertTrue(checkKafkaLogLevel(logLevel), "Kafka's log level is set properly");
    }

    @Test
    @Tag("release")
    void testLogLevelFatal() {
        String logLevel = "FATAL";
        LOGGER.info("Running testLogLevelInfo in namespace {}", NAMESPACE);
        createKafkaPods(logLevel);
        assertTrue(checkKafkaLogLevel(logLevel), "Kafka's log level is set properly");
    }

    @Test
    @Tag("release")
    void testLogLevelOff() {
        String logLevel = "OFF";
        LOGGER.info("Running testLogLevelInfo in namespace {}", NAMESPACE);
        createKafkaPods(logLevel);
        assertTrue(checkKafkaLogLevel(logLevel), "Kafka's log level is set properly");
    }

    private boolean checkKafkaLogLevel(String logLevel) {
        LOGGER.info("Check log level setting. Expected: {}", logLevel);
        String kafkaPodLog = kubeClient.logs(POD_NAME, "kafka");
        boolean result = kafkaPodLog.contains("level=" + logLevel);

        if (result) {
            kafkaPodLog = kubeClient.searchInLog(STATEFUL_SET, kafkaClusterName(CLUSTER_NAME), 600, "ERROR");
            result = kafkaPodLog.isEmpty();
        }

        return result;
    }

    private void createKafkaPods(String logLevel) {
        LOGGER.info("Create kafka in {} for testing logger: {}={}", CLUSTER_NAME, TESTED_LOGGER, logLevel);

        resources().kafka(resources().defaultKafka(CLUSTER_NAME, 3)
                .editSpec()
                .editKafka().
                        addToConfig(TESTED_LOGGER, logLevel)
                .endKafka()
                .endSpec()
                .build()).done();
    }
}
