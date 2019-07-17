/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.systemtest.utils.StUtils;
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
        Condition kafkaCondition = testClassResources().kafka().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus().getConditions().get(0);
        logCurrentStatus(kafkaCondition, Kafka.RESOURCE_KIND);
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

    @Test
    void testKafkaUserStatus() {
        String userName = "status-user-test";
        testClassResources().tlsUser(CLUSTER_NAME, userName).done();
        StUtils.waitForSecretReady(userName);
        LOGGER.info("Checking status of deployed kafka user");
        Condition kafkaCondition = testClassResources().kafkaUser().inNamespace(NAMESPACE).withName(userName).get().getStatus().getConditions().get(0);
        LOGGER.info("Kafka User Status: {}", kafkaCondition.getStatus());
        LOGGER.info("Kafka User Type: {}", kafkaCondition.getType());
        LOGGER.info("Kafka User Message: {}", kafkaCondition.getMessage());
        assertThat("Kafka user is in wrong state!", kafkaCondition.getType(), is("Ready"));
        LOGGER.info("Kafka user is in desired state: Ready");
    }

    @Test
    void testKafkaUserStatusNotReady() {
        createTestMethodResources();
        // Simulate NotReady state with userName longer than 64 characters
        String userName = "sasl-use-rabcdefghijklmnopqrstuvxyzabcdefghijklmnopqrstuvxyzabcdef";
        testMethodResources().tlsUser(CLUSTER_NAME, userName).done();

        String eoPodName = kubeClient().listPods("strimzi.io/name", KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME)).get(0).getMetadata().getName();
        StUtils.waitForKafkaUserCreationError(userName, eoPodName);

        LOGGER.info("Checking status of deployed Kafka User {}", userName);
        Condition kafkaCondition = testMethodResources().kafkaUser().inNamespace(NAMESPACE).withName(userName).get().getStatus().getConditions().get(0);
        LOGGER.info("Kafka User Status: {}", kafkaCondition.getStatus());
        LOGGER.info("Kafka User Type: {}", kafkaCondition.getType());
        LOGGER.info("Kafka User Message: {}", kafkaCondition.getMessage());
        assertThat("Kafka User is in wrong state!", kafkaCondition.getType(), is("NotReady"));
        LOGGER.info("Kafka User {} is in desired state: {}", userName, kafkaCondition.getType());
        testMethodResources().deleteResources();
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
        testClassResources().clusterOperator(NAMESPACE, Long.toString(Constants.CO_OPERATION_TIMEOUT)).done();

        setOperationID(startDeploymentMeasuring());

        testClassResources().kafka(testClassResources().defaultKafka(CLUSTER_NAME, 3, 1)
                .editSpec()
                .editKafka()
                .editListeners()
                .withNewKafkaListenerExternalNodePort()
                .withTls(false)
                .endKafkaListenerExternalNodePort()
                .endListeners()
                .endKafka()
                .endSpec().build())
                .done();

        testClassResources().topic(CLUSTER_NAME, TOPIC_NAME).done();
    }

    void logCurrentStatus(Condition kafkaCondition, String resource) {
        LOGGER.debug("Kafka {} Status: {}", resource, kafkaCondition.getStatus());
        LOGGER.debug("Kafka {} Type: {}", resource, kafkaCondition.getType());
        LOGGER.debug("Kafka {} Message: {}", resource, kafkaCondition.getMessage());
    }

    void waitForKafkaStatus(String status) {
        TestUtils.waitFor("Wait until status is false", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT, () -> {
            Condition kafkaCondition = testClassResources().kafka().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus().getConditions().get(0);
            logCurrentStatus(kafkaCondition, Kafka.RESOURCE_KIND);
            return kafkaCondition.getType().equals(status);
        });
        LOGGER.info("Kafka cluster is in desired state: {}", status);
    }
}
