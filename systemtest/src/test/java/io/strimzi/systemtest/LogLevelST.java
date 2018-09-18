package io.strimzi.systemtest;


import io.strimzi.test.ClusterOperator;
import io.strimzi.test.Namespace;
import io.strimzi.test.StrimziRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;

import static io.strimzi.test.k8s.BaseKubeClient.STATEFUL_SET;
import static junit.framework.TestCase.assertTrue;

@RunWith(StrimziRunner.class)
@Namespace(LogLevelST.NAMESPACE)
@ClusterOperator
public class LogLevelST extends AbstractST {
    static final String NAMESPACE = "log-level-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(LogLevelST.class);
    private static final String TESTED_LOGGER = "kafka.root.logger.level";
    private static final String POD_NAME = kafkaClusterName(CLUSTER_NAME) + "-0";

    @Test
    public void testLogLevelInfo() {
        LOGGER.info("Running testLogLevelInfo in namespace {}", NAMESPACE);
        String logLevel = "INFO";
        createKafkaPods(logLevel);
        assertTrue("Kafka's log level is set properly", checkKafkaLogLevel(logLevel));
    }

    @Test
    public void testLogLevelError() {
        LOGGER.info("Running testLogLevelError in namespace {}", NAMESPACE);
        String logLevel = "ERROR";

        createKafkaPods(logLevel);
        assertTrue("Kafka's log level is set properly", checkKafkaLogLevel(logLevel));
    }

    @Test
    public void testLogLevelWarn() {
        LOGGER.info("Running testLogLevelWarn in namespace {}", NAMESPACE);
        String logLevel = "WARN";

        createKafkaPods(logLevel);
        assertTrue("Kafka's log level is set properly", checkKafkaLogLevel(logLevel));
    }

    @Test
    public void testLogLevelTrace() {
        LOGGER.info("Running testLogLevelTrace in namespace {}", NAMESPACE);
        String logLevel = "TRACE";
        createKafkaPods(logLevel);
        assertTrue("Kafka's log level is set properly", checkKafkaLogLevel(logLevel));
    }

    @Test
    public void testLogLevelDebug() {
        LOGGER.info("Running testLogLevelDebug in namespace {}", NAMESPACE);
        String logLevel = "DEBUG";
        createKafkaPods(logLevel);
        assertTrue("Kafka's log level is set properly", checkKafkaLogLevel(logLevel));
    }

    @Test
    public void testLogLevelFatal() {
        LOGGER.info("Running testLogLevelFatal in namespace {}", NAMESPACE);
        String logLevel = "FATAL";
        createKafkaPods(logLevel);
        assertTrue("Kafka's log level is set properly", checkKafkaLogLevel(logLevel));
    }

    @Test
    public void testLogLevelOff() {
        LOGGER.info("Running testLogLevelOff in namespace {}", NAMESPACE);
        String logLevel = "OFF";
        createKafkaPods(logLevel);
        assertTrue("Kafka's log level is set properly", checkKafkaLogLevel(logLevel));
    }

    private boolean checkKafkaLogLevel(String logLevel) {
        LOGGER.info("Check log level setting. Expected: {}", logLevel);
        String kafkaPodLog = kubeClient.logs(POD_NAME, "kafka");
        boolean result = kafkaPodLog.contains("level=" + logLevel);

        if(result) {
            kafkaPodLog = kubeClient.searchInLog(STATEFUL_SET, kafkaClusterName(CLUSTER_NAME), 600, "ERROR");
            result = kafkaPodLog.isEmpty();
        }

        return result;
    }

    private void createKafkaPods(String logLevel) {
        LOGGER.info("Create kafka in {} for testing logger: {}={}", CLUSTER_NAME, TESTED_LOGGER, logLevel);

        resources().kafka(resources().defaultKafka(CLUSTER_NAME, 1)
                .editSpec()
                .editKafka().
                        addToConfig(TESTED_LOGGER, logLevel)
                .endKafka()
                .endSpec()
                .build()).done();
    }
}
