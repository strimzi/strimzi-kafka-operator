/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.KafkaBridgeStatus;
import io.strimzi.api.kafka.model.status.KafkaConnectS2Istatus;
import io.strimzi.api.kafka.model.status.KafkaConnectStatus;
import io.strimzi.api.kafka.model.status.KafkaMirrorMakerStatus;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import io.strimzi.api.kafka.model.status.ListenerStatus;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.timemeasuring.Operation;
import io.strimzi.test.timemeasuring.TimeMeasuringSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.strimzi.api.kafka.model.KafkaResources.externalBootstrapServiceName;
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
        waitForKafkaStatus("Ready");
        waitForClusterAvailability(NAMESPACE, TOPIC_NAME);
        assertKafkaStatus(1, "my-cluster-kafka-bootstrap.status-cluster-test.svc");

        replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec().getEntityOperator().getTopicOperator().setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("100000m"))
                    .build());
        });

        LOGGER.info("Wait until cluster will be in NotReady state ...");
        waitForKafkaStatus("NotReady");

        LOGGER.info("Recovery cluster to Ready state ...");
        replaceKafkaResource(CLUSTER_NAME, k -> {
            k.getSpec().getEntityOperator().getTopicOperator().setResources(new ResourceRequirementsBuilder()
                    .addToRequests("cpu", new Quantity("10m"))
                    .build());
        });
        waitForKafkaStatus("Ready");
        assertKafkaStatus(3, "my-cluster-kafka-bootstrap.status-cluster-test.svc");
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
        LOGGER.info("Kafka User Reason: {}", kafkaCondition.getReason());
        assertThat("Kafka User is in wrong state!", kafkaCondition.getType(), is("NotReady"));
        LOGGER.info("Kafka User {} is in desired state: {}", userName, kafkaCondition.getType());
        testMethodResources().deleteResources();
    }

    @Test
    void testKafkaMirrorMakerStatus() {
        createTestMethodResources();
        // Deploy Mirror Maker
        testMethodResources().kafkaMirrorMaker(CLUSTER_NAME, CLUSTER_NAME, CLUSTER_NAME, "my-group" + rng.nextInt(Integer.MAX_VALUE), 1, false).done();
        waitForKafkaMirrorMakerStatus("Ready");
        assertKafkaMirrorMakerStatus(1);
        // Corrupt Mirror Maker pods
        replaceMirrorMakerResource(CLUSTER_NAME, mm -> mm.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()));
        waitForKafkaMirrorMakerStatus("NotReady");
        // Restore Mirror Maker pod
        replaceMirrorMakerResource(CLUSTER_NAME, mm -> mm.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("10m"))
                .build()));
        waitForKafkaMirrorMakerStatus("Ready");
        assertKafkaMirrorMakerStatus(3);
    }

    @Test
    @Disabled("Currently, readiness check for MM is not working correctly so MM status is not set properly when MM config is corrupted by wrong bootstrap server")
    void testKafkaMirrorMakerStatusWrongBootstrap() {
        createTestMethodResources();
        testMethodResources().kafkaMirrorMaker(CLUSTER_NAME, CLUSTER_NAME, CLUSTER_NAME, "my-group" + rng.nextInt(Integer.MAX_VALUE), 1, false).done();
        waitForKafkaMirrorMakerStatus("Ready");
        assertKafkaMirrorMakerStatus(1);
        // Corrupt Mirror Maker pods
        replaceMirrorMakerResource(CLUSTER_NAME, mm -> mm.getSpec().getConsumer().setBootstrapServers("non-exists-bootstrap"));
        waitForKafkaMirrorMakerStatus("NotReady");
        // Restore Mirror Maker pods
        replaceMirrorMakerResource(CLUSTER_NAME, mm -> mm.getSpec().getConsumer().setBootstrapServers(CLUSTER_NAME));
        waitForKafkaMirrorMakerStatus("Ready");
        assertKafkaMirrorMakerStatus(3);
    }

    @Test
    void testKafkaBridgeStatus() {
        createTestMethodResources();

        testMethodResources().kafkaBridge(CLUSTER_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), 1, Constants.HTTP_BRIDGE_DEFAULT_PORT).done();
        waitForKafkaBridgeStatus("Ready");
        assertKafkaBridgeStatus(1, "http://my-cluster-bridge-bridge-service.status-cluster-test.svc:8080");

        replaceBridgeResource(CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()));
        waitForKafkaBridgeStatus("NotReady");

        replaceBridgeResource(CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("10m"))
                .build()));
        waitForKafkaBridgeStatus("Ready");
        assertKafkaBridgeStatus(3, "http://my-cluster-bridge-bridge-service.status-cluster-test.svc:8080");
    }

    @Test
    void testKafkaConnectStatus() {
        createTestMethodResources();
        testMethodResources().kafkaConnect(CLUSTER_NAME, 1).done();
        waitForKafkaConnectStatus("Ready");
        assertKafkaConnectStatus(1, "http://my-cluster-connect-api.status-cluster-test.svc:8083");

        replaceKafkaConnectResource(CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()));
        waitForKafkaConnectStatus("NotReady");

        replaceKafkaConnectResource(CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("10m"))
                .build()));
        waitForKafkaConnectStatus("Ready");
        assertKafkaConnectStatus(3, "http://my-cluster-connect-api.status-cluster-test.svc:8083");
    }

    @Test
    void testKafkaConnectS2IStatus() {
        createTestMethodResources();

        testMethodResources().kafkaConnectS2I(CLUSTER_NAME, 1, CLUSTER_NAME).done();
        waitForKafkaConnectS2IStatus("Ready");
        assertKafkaConnectS2IStatus(1, "http://my-cluster-connect-api.status-cluster-test.svc:8083", "my-cluster-connect");

        replaceConnectS2IResource(CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()));
        waitForKafkaConnectS2IStatus("NotReady");

        replaceConnectS2IResource(CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("10m"))
                .build()));
        waitForKafkaConnectS2IStatus("Ready");
        assertKafkaConnectS2IStatus(3, "http://my-cluster-connect-api.status-cluster-test.svc:8083", "my-cluster-connect");
    }

    @BeforeAll
    void createClassResources() {
        createTestEnvironment();
    }

    @AfterEach
    void deleteMethodResources() {
        deleteTestMethodResources();
    }

    private String startDeploymentMeasuring() {
        TimeMeasuringSystem.setTestName(testClass, testClass);
        return TimeMeasuringSystem.startOperation(Operation.CLASS_EXECUTION);
    }

    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        createTestEnvironment();
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
        LOGGER.info("Wait until Kafka cluster is in desired state: {}", status);
        TestUtils.waitFor("Kafka Cluster status is not in desired state: " + status, Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT, () -> {
            Condition kafkaCondition = testClassResources().kafka().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus().getConditions().get(0);
            logCurrentStatus(kafkaCondition, Kafka.RESOURCE_KIND);
            return kafkaCondition.getType().equals(status);
        });
        LOGGER.info("Kafka cluster is in desired state: {}", status);
    }

    void waitForKafkaMirrorMakerStatus(String status) {
        LOGGER.info("Wait until Kafka Mirror Maker cluster is in desired state: {}", status);
        TestUtils.waitFor("Kafka Mirror Maker status is not in desired state: " + status, Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT, () -> {
            Condition kafkaCondition = testClassResources().kafkaMirrorMaker().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus().getConditions().get(0);
            logCurrentStatus(kafkaCondition, KafkaMirrorMaker.RESOURCE_KIND);
            return kafkaCondition.getType().equals(status);
        });
        LOGGER.info("Kafka Mirror Maker cluster is in desired state: {}", status);
    }

    void waitForKafkaBridgeStatus(String status) {
        LOGGER.info("Wait until Kafka Bridge cluster is in desired state: {}", status);
        TestUtils.waitFor("Kafka Bridge status is not in desired state: " + status, Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT, () -> {
            Condition kafkaCondition = testClassResources().kafkaBridge().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus().getConditions().get(0);
            logCurrentStatus(kafkaCondition, KafkaBridge.RESOURCE_KIND);
            return kafkaCondition.getType().equals(status);
        });
        LOGGER.info("Kafka Bridge cluster is in desired state: {}", status);
    }

    void waitForKafkaConnectStatus(String status) {
        LOGGER.info("Wait until Kafka Connect cluster is in desired state: {}", status);
        TestUtils.waitFor("Kafka Connect status is not in desired state: " + status, Constants.GLOBAL_POLL_INTERVAL, Constants.CONNECT_STATUS_TIMEOUT, () -> {
            Condition kafkaCondition = testClassResources().kafkaConnect().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus().getConditions().get(0);
            logCurrentStatus(kafkaCondition, KafkaConnect.RESOURCE_KIND);
            return kafkaCondition.getType().equals(status);
        });
        LOGGER.info("Kafka Connect cluster is in desired state: {}", status);
    }

    void waitForKafkaConnectS2IStatus(String status) {
        LOGGER.info("Wait until Kafka ConnectS2I cluster is in desired state: {}", status);
        TestUtils.waitFor("Kafka ConnectS2I status is not in desired state: " + status, Constants.GLOBAL_POLL_INTERVAL, Constants.CONNECT_STATUS_TIMEOUT, () -> {
            Condition kafkaCondition = testClassResources().kafkaConnectS2I().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus().getConditions().get(0);
            logCurrentStatus(kafkaCondition, KafkaConnectS2I.RESOURCE_KIND);
            return kafkaCondition.getType().equals(status);
        });
        LOGGER.info("Kafka ConnectS2I cluster is in desired state: {}", status);
    }

    void assertKafkaStatus(long expectedObservedGeneration, String internalAddress) {
        KafkaStatus kafkaStatus = testClassResources().kafka().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus();
        assertThat("", kafkaStatus.getObservedGeneration(), is(expectedObservedGeneration));

        for (ListenerStatus listener : kafkaStatus.getListeners()) {
            switch (listener.getType()) {
                case "tls":
                    assertThat("", listener.getAddresses().get(0).getPort(), is(9093));
                    assertThat("", listener.getAddresses().get(0).getHost(), is(internalAddress));
                    break;
                case "plain":
                    assertThat("", listener.getAddresses().get(0).getPort(), is(9092));
                    assertThat("", listener.getAddresses().get(0).getHost(), is(internalAddress));
                    break;
                case "external":
                    Service extBootstrapService = kubeClient(NAMESPACE).getClient().services()
                            .inNamespace(NAMESPACE)
                            .withName(externalBootstrapServiceName(CLUSTER_NAME))
                            .get();
                    assertThat("", listener.getAddresses().get(0).getPort(), is(extBootstrapService.getSpec().getPorts().get(0).getNodePort()));
                    assertThat("", listener.getAddresses().get(0).getHost() != null);
                    break;
            }
        }
    }

    void assertKafkaMirrorMakerStatus(long expectedObservedGeneration) {
        KafkaMirrorMakerStatus kafkaMirrorMakerStatus = testMethodResources().kafkaMirrorMaker().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus();
        assertThat("", kafkaMirrorMakerStatus.getObservedGeneration(), is(expectedObservedGeneration));
    }

    void assertKafkaBridgeStatus(long expectedObservedGeneration, String bridgeAddress) {
        KafkaBridgeStatus kafkaBridgeStatus = testClassResources().kafkaBridge().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus();
        assertThat("", kafkaBridgeStatus.getObservedGeneration(), is(expectedObservedGeneration));
        assertThat("", kafkaBridgeStatus.getUrl(), is(bridgeAddress));
    }

    void assertKafkaConnectStatus(long expectedObservedGeneration, String expectedUrl) {
        KafkaConnectStatus kafkaConnectStatus = testMethodResources().kafkaConnect().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus();
        assertThat("", kafkaConnectStatus.getObservedGeneration(), is(expectedObservedGeneration));
        assertThat("", kafkaConnectStatus.getUrl(), is(expectedUrl));
    }

    void assertKafkaConnectS2IStatus(long expectedObservedGeneration, String expectedUrl, String expectedConfigName) {
        KafkaConnectS2Istatus kafkaConnectS2IStatus = testMethodResources().kafkaConnectS2I().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus();
        assertThat("", kafkaConnectS2IStatus.getObservedGeneration(), is(expectedObservedGeneration));
        assertThat("", kafkaConnectS2IStatus.getUrl(), is(expectedUrl));
        assertThat("", kafkaConnectS2IStatus.getBuildConfigName(), is(expectedConfigName));
    }
}
