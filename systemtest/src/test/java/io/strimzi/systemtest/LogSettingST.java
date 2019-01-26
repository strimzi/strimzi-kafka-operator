/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.strimzi.api.kafka.model.EntityOperatorJvmOptions;
import io.strimzi.systemtest.timemeasuring.Operation;
import io.strimzi.systemtest.timemeasuring.TimeMeasuringSystem;
import io.strimzi.test.annotations.ClusterOperator;
import io.strimzi.test.annotations.Namespace;
import io.strimzi.test.extensions.StrimziExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.test.extensions.StrimziExtension.REGRESSION;
import static io.strimzi.test.k8s.BaseKubeClient.STATEFUL_SET;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(StrimziExtension.class)
@Namespace(LogSettingST.NAMESPACE)
@ClusterOperator
@Tag(REGRESSION)
class LogSettingST extends AbstractST {
    static final String NAMESPACE = "log-level-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(LogSettingST.class);
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

    private static final String CG_LOGGING_NAME = "cg-logging";

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

    @Test
    void testKafkaLoggers() {
        int duration = TimeMeasuringSystem.getCurrentDuration(testClass, testClass, operationID);
        assertTrue(checkLoggersLevel(KAFKA_LOGGERS, duration, KAFKA_MAP), "Kafka's log level is set properly");
    }

    @Test
    void testZookeeperLoggers() {
        int duration = TimeMeasuringSystem.getCurrentDuration(testClass, testClass, operationID);
        assertTrue(checkLoggersLevel(ZOOKEEPER_LOGGERS, duration, ZOOKEEPER_MAP), "Zookeeper's log level is set properly");
    }

    @Test
    void testTOLoggers() {
        int duration = TimeMeasuringSystem.getCurrentDuration(testClass, testClass, operationID);
        assertTrue(checkLoggersLevel(OPERATORS_LOGGERS, duration, TO_MAP), "Topic operator's log level is set properly");
    }

    @Test
    void testUOLoggers() {
        int duration = TimeMeasuringSystem.getCurrentDuration(testClass, testClass, operationID);
        assertTrue(checkLoggersLevel(OPERATORS_LOGGERS, duration, UO_MAP), "User operator's log level is set properly");
    }

    @Test
    void testKafkaConnectLoggers() {
        int duration = TimeMeasuringSystem.getCurrentDuration(testClass, testClass, operationID);
        assertTrue(checkLoggersLevel(CONNECT_LOGGERS, duration, CONNECT_MAP), "Kafka connect's log level is set properly");
    }

    @Test
    void testMirrorMakerLoggers() {
        int duration = TimeMeasuringSystem.getCurrentDuration(testClass, testClass, operationID);
        assertTrue(checkLoggersLevel(MIRROR_MAKER_LOGGERS, duration, MM_MAP), "Mirror maker's log level is set properly");
    }

    @Test
    void testCgLoggingEnabled() {
        assertTrue(checkGcLoggingStatefulSets(kafkaClusterName(CLUSTER_NAME)), "Kafka CG logging is enabled");
        assertTrue(checkGcLoggingStatefulSets(zookeeperClusterName(CLUSTER_NAME)), "Zookeeper CG logging is enabled");

        assertTrue(checkGcLoggingDeployments(entityOperatorDeploymentName(CLUSTER_NAME)), "TO CG logging is enabled");
        assertTrue(checkGcLoggingDeployments(entityOperatorDeploymentName(CLUSTER_NAME)), "UO CG logging is enabled");

        assertTrue(checkGcLoggingDeployments(kafkaConnectName(CLUSTER_NAME)), "Connect CG logging is enabled");
        assertTrue(checkGcLoggingDeployments(kafkaMirrorMakerName(CLUSTER_NAME)), "Mirror-maker CG logging is enabled");
    }

    @Test
    void testCgLoggingDisabled() {
        assertFalse(checkGcLoggingStatefulSets(kafkaClusterName(CG_LOGGING_NAME)), "Kafka CG logging is disabled");
        assertFalse(checkGcLoggingStatefulSets(zookeeperClusterName(CG_LOGGING_NAME)), "Zookeeper CG logging is disabled");

        assertFalse(checkGcLoggingDeployments(entityOperatorDeploymentName(CG_LOGGING_NAME)), "TO CG logging is disabled");
        assertFalse(checkGcLoggingDeployments(entityOperatorDeploymentName(CG_LOGGING_NAME)), "UO CG logging is disabled");

        assertFalse(checkGcLoggingDeployments(kafkaConnectName(CG_LOGGING_NAME)), "Connect CG logging is disabled");
        assertFalse(checkGcLoggingDeployments(kafkaMirrorMakerName(CG_LOGGING_NAME)), "Mirror-maker CG logging is disabled");
    }


    private boolean checkLoggersLevel(Map<String, String> loggers, int since, String configMapName) {
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


    private Boolean checkGcLoggingDeployments(String deploymentName) {
        List<EnvVar> envVars = client.apps().deployments().withName(deploymentName).get().getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        return checkEnvVarValue(envVars);
    }

    private Boolean checkGcLoggingStatefulSets(String statefulSetName) {
        List<EnvVar> envVars = client.apps().statefulSets().withName(statefulSetName).get().getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        return checkEnvVarValue(envVars);
    }

    private Boolean checkEnvVarValue(List<EnvVar> envVars) {
        for (EnvVar env : envVars) {
            LOGGER.info("{}={}", env.getName(), env.getValue());
            if (env.getName().contains("GC_LOG_ENABLED")) {
                LOGGER.info(env.getValue());
                return env.getValue().contains("true");
            }
        }
        return false;
    }

    @BeforeAll
    static void createClassResources(TestInfo testInfo) {
        LOGGER.info("Create resources for the tests");
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        testClassResources.clusterOperator(NAMESPACE).done();

        testClass = testInfo.getTestClass().get().getSimpleName();
        operationID = startDeploymentMeasuring();

        testClassResources.kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .withNewInlineLogging()
                        .withLoggers(KAFKA_LOGGERS)
                    .endInlineLogging()
                .endKafka()
                .editZookeeper()
                    .withNewInlineLogging()
                        .withLoggers(ZOOKEEPER_LOGGERS)
                    .endInlineLogging()
                .endZookeeper()
                .editEntityOperator()
                    .withNewUserOperator()
                        .withNewInlineLogging()
                            .withLoggers(OPERATORS_LOGGERS)
                        .endInlineLogging()
                    .endUserOperator()
                    .withNewTopicOperator()
                        .withNewInlineLogging()
                            .withLoggers(OPERATORS_LOGGERS)
                        .endInlineLogging()
                    .endTopicOperator()
                .endEntityOperator()
            .endSpec()
            .done();

        EntityOperatorJvmOptions entityOperatorJvmOptions = new EntityOperatorJvmOptions();
        entityOperatorJvmOptions.setGcLoggingEnabled(false);

        testClassResources.kafkaEphemeral(CG_LOGGING_NAME, 3)
                .editSpec()
                    .editKafka()
                        .withNewJvmOptions()
                            .withGcLoggingEnabled(false)
                        .endJvmOptions()
                    .endKafka()
                    .editZookeeper()
                        .withNewJvmOptions()
                            .withGcLoggingEnabled(false)
                        .endJvmOptions()
                    .endZookeeper()
                    .editOrNewEntityOperator()
                        .editOrNewTopicOperator()
                            .withNewJvmOptionsLike(entityOperatorJvmOptions)
                            .endJvmOptions()
                        .endTopicOperator()
                        .editOrNewUserOperator()
                            .withNewJvmOptionsLike(entityOperatorJvmOptions)
                            .endJvmOptions()
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .done();

        testClassResources.kafkaConnect(CLUSTER_NAME, 1)
            .editSpec()
                .withNewInlineLogging()
                    .withLoggers(CONNECT_LOGGERS)
                .endInlineLogging()
            .endSpec().done();

        testClassResources.kafkaMirrorMaker(CLUSTER_NAME, CLUSTER_NAME, CG_LOGGING_NAME, "my-group", 1, false)
            .editSpec()
                .withNewInlineLogging()
                  .withLoggers(MIRROR_MAKER_LOGGERS)
                .endInlineLogging()
            .endSpec()
            .done();

        testClassResources.kafkaConnect(CG_LOGGING_NAME, 1)
                .editSpec()
                    .withNewJvmOptions()
                        .withGcLoggingEnabled(false)
                    .endJvmOptions()
                .endSpec().done();

        testClassResources.kafkaMirrorMaker(CG_LOGGING_NAME, CLUSTER_NAME, CG_LOGGING_NAME, "my-group", 1, false)
                .editSpec()
                    .withNewJvmOptions()
                       .withGcLoggingEnabled(false)
                    .endJvmOptions()
                .endSpec()
                .done();
    }

    @AfterAll
    static void deleteClassResources() {
        TimeMeasuringSystem.stopOperation(operationID);
    }

    private static String startDeploymentMeasuring() {
        TimeMeasuringSystem.setTestName(testClass, testClass);
        return TimeMeasuringSystem.startOperation(Operation.CLASS_EXECUTION);
    }
}
