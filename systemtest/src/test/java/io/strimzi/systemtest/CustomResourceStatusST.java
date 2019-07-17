/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.test.TestUtils;
import io.strimzi.test.timemeasuring.Operation;
import io.strimzi.test.timemeasuring.TimeMeasuringSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
class CustomResourceStatusST extends AbstractST {
    static final String NAMESPACE = "status-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(CustomResourceStatusST.class);
    private static final String TOPIC_NAME = "status-topic";

    @Test
    void testKafkaStatus() throws Exception {
        LOGGER.info("Checking status of deployed kafka cluster");
        Condition kafkaCondition = getTestClassResources().kafka().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus().getConditions().get(0);
        logCurrentStatus(kafkaCondition);
        assertThat("Kafka cluster is in wrong state!", kafkaCondition.getType(), is("Ready"));
        LOGGER.info("Kafka cluster is in desired state: Ready");

        waitForClusterAvailability(NAMESPACE, TOPIC_NAME);

        replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec().getZookeeper().setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("100000m"))
                    .build());
        });

        LOGGER.info("Wait until cluster will be in NotReady state ...");
        waitForKafkaStatus("NotReady");

        LOGGER.info("Recovery cluster to Ready state ...");
        replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec().getZookeeper().setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("10m"))
                    .build());
        });
    }

    @AfterEach
    void checkClusterAvailability() {
        LOGGER.info("Wait until status will be in Ready state (AfterEach phase) ...");
        waitForKafkaStatus("Ready");
    }

    @BeforeAll
    void createClassResources() {
        createTestEnvironment();
    }

    private String startDeploymentMeasuring() {
        TimeMeasuringSystem.setTestName(testClass, testClass);
        return TimeMeasuringSystem.startOperation(Operation.CLASS_EXECUTION);
    }

    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        createTestEnvironment();
    }

    @Override
    protected void tearDownEnvironmentAfterAll() {
        teardownEnvForOperator();
    }

    void createTestEnvironment() {
        LOGGER.info("Create resources for the tests");
        prepareEnvForOperator(NAMESPACE);

        createTestClassResources();
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        getTestClassResources().clusterOperator(NAMESPACE, Long.toString(Constants.CO_OPERATION_TIMEOUT)).done();

        setOperationID(startDeploymentMeasuring());

        getTestClassResources().kafkaEphemeral(CLUSTER_NAME, 3, 1)
                .editSpec()
                .editKafka()
                .editListeners()
                .withNewKafkaListenerExternalNodePort()
                .withTls(false)
                .endKafkaListenerExternalNodePort()
                .endListeners()
                .endKafka()
                .endSpec()
                .done();

        getTestClassResources().topic(CLUSTER_NAME, TOPIC_NAME);
    }

    void logCurrentStatus(Condition kafkaCondition) {
        LOGGER.debug("Kafka Status: {}", kafkaCondition.getStatus());
        LOGGER.debug("Kafka Type: {}", kafkaCondition.getType());
        LOGGER.debug("Kafka Message: {}", kafkaCondition.getMessage());
    }

    void waitForKafkaStatus(String status) {
        TestUtils.waitFor("Wait until status is false", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT, () -> {
            Condition kafkaCondition = getTestClassResources().kafka().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus().getConditions().get(0);
            logCurrentStatus(kafkaCondition);
            return kafkaCondition.getType().equals(status);
        });
        LOGGER.info("Kafka cluster is in desired state: {}", status);
    }
}
