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
import org.junit.jupiter.api.BeforeEach;
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
    private static final String CONNECTS2I_CLUSTER_NAME = CLUSTER_NAME + "-s2i";

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
        String bridgeUrl = "http://my-cluster-bridge-bridge-service.status-cluster-test.svc:8080";
        testMethodResources().kafkaBridge(CLUSTER_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), 1, Constants.HTTP_BRIDGE_DEFAULT_PORT).done();
        waitForKafkaBridgeStatus("Ready");
        assertKafkaBridgeStatus(1, bridgeUrl);

        replaceBridgeResource(CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()));
        waitForKafkaBridgeStatus("NotReady");

        replaceBridgeResource(CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("10m"))
                .build()));
        waitForKafkaBridgeStatus("Ready");
        assertKafkaBridgeStatus(3, bridgeUrl);
    }

    @Test
    void testKafkaConnectStatus() {
        String connectUrl = "http://my-cluster-connect-api.status-cluster-test.svc:8083";
        testMethodResources().kafkaConnect(CLUSTER_NAME, 1).done();
        waitForKafkaConnectStatus("Ready");
        assertKafkaConnectStatus(1, connectUrl);

        replaceKafkaConnectResource(CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()));
        waitForKafkaConnectStatus("NotReady");

        replaceKafkaConnectResource(CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("10m"))
                .build()));
        waitForKafkaConnectStatus("Ready");
        assertKafkaConnectStatus(3, connectUrl);
    }

    @Test
    void testKafkaConnectS2IStatus() {
        String connectS2IUrl = "http://my-cluster-s2i-connect-api.status-cluster-test.svc:8083";
        String connectS2IDeploymentConfigName = CONNECTS2I_CLUSTER_NAME + "-connect";
        testMethodResources().kafkaConnectS2I(CONNECTS2I_CLUSTER_NAME, 1, CLUSTER_NAME).done();
        waitForKafkaConnectS2IStatus("Ready");
        assertKafkaConnectS2IStatus(1, connectS2IUrl, connectS2IDeploymentConfigName);

        replaceConnectS2IResource(CONNECTS2I_CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("100000000m"))
                .build()));
        waitForKafkaConnectS2IStatus("NotReady");

        replaceConnectS2IResource(CONNECTS2I_CLUSTER_NAME, kb -> kb.getSpec().setResources(new ResourceRequirementsBuilder()
                .addToRequests("cpu", new Quantity("10m"))
                .build()));
        waitForKafkaConnectS2IStatus("Ready");
        assertKafkaConnectS2IStatus(3, connectS2IUrl, connectS2IDeploymentConfigName);
    }

    @BeforeAll
    void createClassResources() {
        LOGGER.info("Create resources for the tests");
        setOperationID(startDeploymentMeasuring());
        prepareEnvForOperator(NAMESPACE);

        createTestClassResources();
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        testClassResources().clusterOperator(NAMESPACE, Long.toString(Constants.CO_OPERATION_TIMEOUT)).done();

        deployTestSpecificResources();
    }

    @BeforeEach
    void createMethodResources() {
        createTestMethodResources();
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
        super.recreateTestEnv(coNamespace, bindingsNamespaces);
        deployTestSpecificResources();
    }

    void deployTestSpecificResources() {
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
            Condition kafkaCondition = testClassResources().kafkaConnectS2I().inNamespace(NAMESPACE).withName(CONNECTS2I_CLUSTER_NAME).get().getStatus().getConditions().get(0);
            logCurrentStatus(kafkaCondition, KafkaConnectS2I.RESOURCE_KIND);
            return kafkaCondition.getType().equals(status);
        });
        LOGGER.info("Kafka ConnectS2I cluster is in desired state: {}", status);
    }

    void assertKafkaStatus(long expectedObservedGeneration, String internalAddress) {
        KafkaStatus kafkaStatus = testClassResources().kafka().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus();
        assertThat("Kafka cluster status has incorrect Observed Generation", kafkaStatus.getObservedGeneration(), is(expectedObservedGeneration));

        for (ListenerStatus listener : kafkaStatus.getListeners()) {
            switch (listener.getType()) {
                case "tls":
                    assertThat("TLS bootstrap has incorrect port", listener.getAddresses().get(0).getPort(), is(9093));
                    assertThat("TLS bootstrap has incorrect host", listener.getAddresses().get(0).getHost(), is(internalAddress));
                    break;
                case "plain":
                    assertThat("Plain bootstrap has incorrect port", listener.getAddresses().get(0).getPort(), is(9092));
                    assertThat("Plain bootstrap has incorrect host", listener.getAddresses().get(0).getHost(), is(internalAddress));
                    break;
                case "external":
                    Service extBootstrapService = kubeClient(NAMESPACE).getClient().services()
                            .inNamespace(NAMESPACE)
                            .withName(externalBootstrapServiceName(CLUSTER_NAME))
                            .get();
                    assertThat("External bootstrap has incorrect port", listener.getAddresses().get(0).getPort(), is(extBootstrapService.getSpec().getPorts().get(0).getNodePort()));
                    assertThat("External bootstrap has incorrect host", listener.getAddresses().get(0).getHost() != null);
                    break;
            }
        }
    }

    void assertKafkaMirrorMakerStatus(long expectedObservedGeneration) {
        KafkaMirrorMakerStatus kafkaMirrorMakerStatus = testMethodResources().kafkaMirrorMaker().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus();
        assertThat("Kafka MirrorMaker cluster status has incorrect Observed Generation", kafkaMirrorMakerStatus.getObservedGeneration(), is(expectedObservedGeneration));
    }

    void assertKafkaBridgeStatus(long expectedObservedGeneration, String bridgeAddress) {
        KafkaBridgeStatus kafkaBridgeStatus = testClassResources().kafkaBridge().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus();
        assertThat("Kafka Bridge cluster status has incorrect Observed Generation", kafkaBridgeStatus.getObservedGeneration(), is(expectedObservedGeneration));
        assertThat("Kafka Bridge cluster status has incorrect URL", kafkaBridgeStatus.getUrl(), is(bridgeAddress));
    }

    void assertKafkaConnectStatus(long expectedObservedGeneration, String expectedUrl) {
        KafkaConnectStatus kafkaConnectStatus = testMethodResources().kafkaConnect().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getStatus();
        assertThat("Kafka Connect cluster status has incorrect Observed Generation", kafkaConnectStatus.getObservedGeneration(), is(expectedObservedGeneration));
        assertThat("Kafka Connect cluster status has incorrect URL", kafkaConnectStatus.getUrl(), is(expectedUrl));
    }

    void assertKafkaConnectS2IStatus(long expectedObservedGeneration, String expectedUrl, String expectedConfigName) {
        KafkaConnectS2Istatus kafkaConnectS2IStatus = testMethodResources().kafkaConnectS2I().inNamespace(NAMESPACE).withName(CONNECTS2I_CLUSTER_NAME).get().getStatus();
        assertThat("Kafka ConnectS2I cluster status has incorrect Observed Generation", kafkaConnectS2IStatus.getObservedGeneration(), is(expectedObservedGeneration));
        assertThat("Kafka ConnectS2I cluster status has incorrect URL", kafkaConnectS2IStatus.getUrl(), is(expectedUrl));
        assertThat("Kafka ConnectS2I cluster status has incorrect BuildConfigName", kafkaConnectS2IStatus.getBuildConfigName(), is(expectedConfigName));
    }
}
