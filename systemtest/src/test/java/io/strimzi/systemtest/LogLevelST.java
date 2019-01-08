/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.systemtest.timemeasuring.Operation;
import io.strimzi.systemtest.timemeasuring.TimeMeasuringSystem;
import io.strimzi.test.Annotations.ClusterOperator;
import io.strimzi.test.Annotations.Namespace;
import io.strimzi.test.Extensions.StrimziExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.test.Extensions.StrimziExtension.REGRESSION;
import static io.strimzi.test.k8s.BaseKubeClient.STATEFUL_SET;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(StrimziExtension.class)
@Namespace(LogLevelST.NAMESPACE)
@ClusterOperator
@Tag(REGRESSION)
class LogLevelST extends AbstractST {
    static final String NAMESPACE = "log-level-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(LogLevelST.class);
    private static final String KAFKA_MAP = String.format("%s-%s", CLUSTER_NAME, "kafka-config");
    private static final String ZOOKEEPER_MAP = String.format("%s-%s", CLUSTER_NAME, "zookeeper-config");
    private static final String TO_MAP = String.format("%s-%s", CLUSTER_NAME, "entity-topic-operator-config");
    private static final String UO_MAP = String.format("%s-%s", CLUSTER_NAME, "entity-user-operator-config");
    private static final String CONNECT_MAP = String.format("%s-%s", CLUSTER_NAME, "connect-config");
    private static final String MM_MAP = String.format("%s-%s", CLUSTER_NAME, "mirror-maker-config");

    private static final String INFO = "INFO";
    private static final String ERROR = "ERROR";
    private static final String WARN = "WARN";
    private static final String TRACE = "TRACE";
    private static final String DEBUG = "DEBUG";
    private static final String FATAL = "FATAL";
    private static final String OFF = "OFF";

    private static final Map<String, String> KAFKA_LOGGERS = new HashMap<String, String>() {
        {
            put("kafka.root.logger.level", INFO);
            put("log4j.logger.org.I0Itec.zkclient.ZkClient", ERROR);
            put("log4j.logger.org.apache.zookeeper", WARN);
            put("log4j.logger.kafka", TRACE);
            put("log4j.logger.org.apache.kafka", DEBUG);
            put("log4j.logger.kafka.request.logger", FATAL);
            put("log4j.logger.kafka.network.Processor", OFF);
            put("log4j.logger.kafka.server.KafkaApis", INFO);
            put("log4j.logger.kafka.network.RequestChannel$", ERROR);
            put("log4j.logger.kafka.controller", WARN);
            put("log4j.logger.kafka.log.LogCleaner", TRACE);
            put("log4j.logger.state.change.logger", DEBUG);
            put("log4j.logger.kafka.authorizer.logger", FATAL);
        }
    };

    private static final Map<String, String> ZOOKEEPER_LOGGERS = new HashMap<String, String>() {
        {
            put("zookeeper.root.logger", OFF);
        }
    };

    private static final Map<String, String> CONNECT_LOGGERS = new HashMap<String, String>() {
        {
            put("connect.root.logger.level", INFO);
            put("log4j.logger.org.I0Itec.zkclient", ERROR);
            put("log4j.logger.org.reflections", WARN);
        }
    };

    private static final Map<String, String> OPERATORS_LOGGERS = new HashMap<String, String>() {
        {
            put("rootLogger.level", DEBUG);
        }
    };

    private static final Map<String, String> MIRROR_MAKER_LOGGERS = new HashMap<String, String>() {
        {
            put("mirrormaker.root.logger", TRACE);
        }
    };

    private static Resources classResources;


    @Test
    void testKafkaLoggers() {
        int duration = TimeMeasuringSystem.getCurrentDuration(testClass, testClass, operationID);
        assertTrue(checkLogLevel(KAFKA_LOGGERS, duration, KAFKA_MAP), "Kafka's log level is set properly");
    }

    @Test
    void testZookeeperLoggers() {
        int duration = TimeMeasuringSystem.getCurrentDuration(testClass, testClass, operationID);
        assertTrue(checkLogLevel(ZOOKEEPER_LOGGERS, duration, ZOOKEEPER_MAP), "Zookeeper's log level is set properly");
    }

    @Test
    void testTOLoggers() {
        int duration = TimeMeasuringSystem.getCurrentDuration(testClass, testClass, operationID);
        assertTrue(checkLogLevel(OPERATORS_LOGGERS, duration, TO_MAP), "Topic operator's log level is set properly");
    }

    @Test
    void testUOLoggers() {
        int duration = TimeMeasuringSystem.getCurrentDuration(testClass, testClass, operationID);
        assertTrue(checkLogLevel(OPERATORS_LOGGERS, duration, UO_MAP), "User operator's log level is set properly");
    }

    @Test
    void testKafkaConnectLoggers() {
        int duration = TimeMeasuringSystem.getCurrentDuration(testClass, testClass, operationID);
        assertTrue(checkLogLevel(CONNECT_LOGGERS, duration, CONNECT_MAP), "Kafka connect's log level is set properly");
    }

    @Test
    void testMirrorMakerLoggers() {
        int duration = TimeMeasuringSystem.getCurrentDuration(testClass, testClass, operationID);
        assertTrue(checkLogLevel(MIRROR_MAKER_LOGGERS, duration, MM_MAP), "Mirror maker's log level is set properly");
    }

    private boolean checkLogLevel(Map<String, String> loggers, int since, String configMapName) {
        boolean result = false;
        for (Map.Entry<String, String> entry : loggers.entrySet()) {
            LOGGER.info("Check log level setting since {} seconds. Logger: {} Expected: {}", since, entry.getKey(), entry.getValue());
            String configMap = kubeClient.get("configMap", configMapName);
            String loggerConfig = String.format("%s=%s", entry.getKey(), entry.getValue());
            result = configMap.contains(loggerConfig);

            if (result) {
                String log = kubeClient.searchInLog(STATEFUL_SET, kafkaClusterName(CLUSTER_NAME), since, ERROR);
                result = log.isEmpty();
            }
        }

        return result;
    }

    @BeforeAll
    static void createClassResources(TestInfo testInfo) {
        LOGGER.info("Create resources for the tests");
        applyRoleBindings(NAMESPACE, NAMESPACE);
        // 050-Deployment
        testClassResources.clusterOperator(NAMESPACE).done();

        testClass = testInfo.getTestClass().get().getSimpleName();
        operationID = startDeploymentMeasuring();
        String targetKafka = CLUSTER_NAME + "-target";

        testClassResources.kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .withNewInlineLoggingLogging()
                        .withLoggers(KAFKA_LOGGERS)
                    .endInlineLoggingLogging()
                .endKafka()
                .editZookeeper()
                    .withNewInlineLoggingLogging()
                        .withLoggers(ZOOKEEPER_LOGGERS)
                    .endInlineLoggingLogging()
                .endZookeeper()
                .editEntityOperator()
                    .withNewUserOperator()
                        .withNewInlineLoggingLogging()
                            .withLoggers(OPERATORS_LOGGERS)
                        .endInlineLoggingLogging()
                    .endUserOperator()
                    .withNewTopicOperator()
                        .withNewInlineLoggingLogging()
                            .withLoggers(OPERATORS_LOGGERS)
                        .endInlineLoggingLogging()
                    .endTopicOperator()
                .endEntityOperator()
            .endSpec()
            .done();

        testClassResources.kafkaEphemeral(targetKafka, 3).done();

        testClassResources.kafkaConnect(CLUSTER_NAME, 1)
            .editSpec()
                .withNewInlineLoggingLogging()
                    .withLoggers(CONNECT_LOGGERS)
                .endInlineLoggingLogging()
            .endSpec().done();

        testClassResources.kafkaMirrorMaker(CLUSTER_NAME, CLUSTER_NAME, targetKafka, "my-group", 1, false)
            .editSpec()
                .withNewInlineLoggingLogging()
                  .withLoggers(MIRROR_MAKER_LOGGERS)
                .endInlineLoggingLogging()
            .endSpec()
            .done();
    }


    @AfterAll
    static void deleteClassResources() {
        TimeMeasuringSystem.stopOperation(operationID);
    }

    private static Resources classResources() {
        return classResources;
    }

    private static String startDeploymentMeasuring() {
        TimeMeasuringSystem.setTestName(testClass, testClass);
        return TimeMeasuringSystem.startOperation(Operation.CLASS_EXECUTION);
    }
}
