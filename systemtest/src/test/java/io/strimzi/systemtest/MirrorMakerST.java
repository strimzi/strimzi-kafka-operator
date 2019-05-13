/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.PasswordSecretSource;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.KafkaListenerTls;
import io.strimzi.test.timemeasuring.Operation;
import io.strimzi.test.timemeasuring.TimeMeasuringSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.Constants.REGRESSION;

@Tag(REGRESSION)
public class MirrorMakerST extends MessagingBaseST {

    private static final Logger LOGGER = LogManager.getLogger(KafkaST.class);

    public static final String NAMESPACE = "mm-cluster-test";
    private static final String TOPIC_NAME = "test-topic";
    private final int messagesCount = 200;

    @Test
    @Tag(REGRESSION)
    void testMirrorMaker() throws Exception {
        Map<String, String> jvmOptionsXX = new HashMap<>();
        jvmOptionsXX.put("UseG1GC", "true");
        operationID = startTimeMeasuring(Operation.MM_DEPLOYMENT);
        String topicSourceName = TOPIC_NAME + "-source" + "-" + rng.nextInt(Integer.MAX_VALUE);
        String kafkaSourceName = CLUSTER_NAME + "-source";
        String kafkaTargetName = CLUSTER_NAME + "-target";

        // Deploy source kafka
        testMethodResources().kafkaEphemeral(kafkaSourceName, 1, 1).done();
        // Deploy target kafka
        testMethodResources().kafkaEphemeral(kafkaTargetName, 1, 1).done();
        // Deploy Topic
        testMethodResources().topic(kafkaSourceName, topicSourceName).done();

        testMethodResources().deployKafkaClients(CLUSTER_NAME).done();

        // Check brokers availability
        availabilityTest(messagesCount, Constants.TIMEOUT_AVAILABILITY_TEST, kafkaSourceName);
        availabilityTest(messagesCount, Constants.TIMEOUT_AVAILABILITY_TEST, kafkaTargetName);

        // Deploy Mirror Maker
        testMethodResources().kafkaMirrorMaker(CLUSTER_NAME, kafkaSourceName, kafkaTargetName, "my-group" + rng.nextInt(Integer.MAX_VALUE), 1, false).
                editSpec()
                .withResources(new ResourceRequirementsBuilder()
                        .addToLimits("memory", new Quantity("400M"))
                        .addToLimits("cpu", new Quantity("2"))
                        .addToRequests("memory", new Quantity("300M"))
                        .addToRequests("cpu", new Quantity("1"))
                        .build())
                .withNewJvmOptions()
                    .withXmx("200m")
                    .withXms("200m")
                    .withServer(true)
                    .withXx(jvmOptionsXX)
                .endJvmOptions()
                .endSpec().done();
        String podName = KUBE_CLIENT.list("Pod").stream().filter(n -> n.startsWith(kafkaMirrorMakerName(CLUSTER_NAME))).findFirst().get();
        assertResources(NAMESPACE, podName, CLUSTER_NAME.concat("-mirror-maker"),
                "400M", "2", "300M", "1");
        assertExpectedJavaOpts(podName,
                "-Xmx200m", "-Xms200m", "-server", "-XX:+UseG1GC");

        TimeMeasuringSystem.stopOperation(operationID);

        int sent = sendMessages(messagesCount, Constants.TIMEOUT_SEND_MESSAGES, kafkaSourceName, false, topicSourceName, null);
        int receivedSource = receiveMessages(messagesCount, Constants.TIMEOUT_RECV_MESSAGES, kafkaSourceName, false, topicSourceName, null);
        int receivedTarget = receiveMessages(messagesCount, Constants.TIMEOUT_RECV_MESSAGES, kafkaTargetName, false, topicSourceName, null);

        assertSentAndReceivedMessages(sent, receivedSource);
        assertSentAndReceivedMessages(sent, receivedTarget);
    }

    /**
     * Test mirroring messages by Mirror Maker over tls transport using mutual tls auth
     */
    @Test
    @Tag(REGRESSION)
    void testMirrorMakerTlsAuthenticated() throws Exception {
        operationID = startTimeMeasuring(Operation.MM_DEPLOYMENT);
        String topicSourceName = TOPIC_NAME + "-source" + "-" + rng.nextInt(Integer.MAX_VALUE);
        String kafkaSourceUserName = "my-user-source";
        String kafkaUserTargetName = "my-user-target";
        String kafkaClusterSourceName = CLUSTER_NAME + "-source";
        String kafkaClusterTargetName = CLUSTER_NAME + "-target";

        KafkaListenerAuthenticationTls auth = new KafkaListenerAuthenticationTls();
        KafkaListenerTls listenerTls = new KafkaListenerTls();
        listenerTls.setAuth(auth);

        // Deploy source kafka with tls listener and mutual tls auth
        testMethodResources().kafka(testMethodResources().defaultKafka(kafkaClusterSourceName, 1, 1)
                .editSpec()
                .editKafka()
                .withNewListeners()
                .withTls(listenerTls)
                .withNewTls()
                .endTls()
                .endListeners()
                .endKafka()
                .endSpec().build()).done();

        // Deploy target kafka with tls listener and mutual tls auth
        testMethodResources().kafka(testMethodResources().defaultKafka(kafkaClusterTargetName, 1, 1)
                .editSpec()
                .editKafka()
                .withNewListeners()
                .withTls(listenerTls)
                .withNewTls()
                .endTls()
                .endListeners()
                .endKafka()
                .endSpec().build()).done();

        // Deploy topic
        testMethodResources().topic(kafkaClusterSourceName, topicSourceName).done();

        // Create Kafka user
        KafkaUser userSource = testMethodResources().tlsUser(kafkaClusterSourceName, kafkaSourceUserName).done();
        waitTillSecretExists(kafkaSourceUserName);

        KafkaUser userTarget = testMethodResources().tlsUser(kafkaClusterTargetName, kafkaUserTargetName).done();
        waitTillSecretExists(kafkaUserTargetName);

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecretSource = new CertSecretSource();
        certSecretSource.setCertificate("ca.crt");
        certSecretSource.setSecretName(clusterCaCertSecretName(kafkaClusterSourceName));

        // Initialize CertSecretSource with certificate and secret names for producer
        CertSecretSource certSecretTarget = new CertSecretSource();
        certSecretTarget.setCertificate("ca.crt");
        certSecretTarget.setSecretName(clusterCaCertSecretName(kafkaClusterTargetName));

        testMethodResources().deployKafkaClients(true, CLUSTER_NAME, userSource, userTarget).done();

        // Check brokers availability
        availabilityTest(messagesCount, Constants.TIMEOUT_AVAILABILITY_TEST, kafkaClusterSourceName, true, "my-topic-test-1", userSource);
        availabilityTest(messagesCount, Constants.TIMEOUT_AVAILABILITY_TEST, kafkaClusterTargetName, true, "my-topic-test-2", userTarget);

        // Deploy Mirror Maker with tls listener and mutual tls auth
        testMethodResources().kafkaMirrorMaker(CLUSTER_NAME, kafkaClusterSourceName, kafkaClusterTargetName, "my-group" + rng.nextInt(Integer.MAX_VALUE), 1, true)
                .editSpec()
                .editConsumer()
                .withNewTls()
                .withTrustedCertificates(certSecretSource)
                .endTls()
                .endConsumer()
                .editProducer()
                .withNewTls()
                .withTrustedCertificates(certSecretTarget)
                .endTls()
                .endProducer()
                .endSpec()
                .done();

        TimeMeasuringSystem.stopOperation(operationID);

        int sent = sendMessages(messagesCount, Constants.TIMEOUT_SEND_MESSAGES, kafkaClusterSourceName, true, topicSourceName, userSource);
        int receivedSource = receiveMessages(messagesCount, Constants.TIMEOUT_RECV_MESSAGES, kafkaClusterSourceName, true, topicSourceName, userSource);
        int receivedTarget = receiveMessages(messagesCount, Constants.TIMEOUT_RECV_MESSAGES, kafkaClusterTargetName, true, topicSourceName, userTarget);

        assertSentAndReceivedMessages(sent, receivedSource);
        assertSentAndReceivedMessages(sent, receivedTarget);
    }

    /**
     * Test mirroring messages by Mirror Maker over tls transport using scram-sha auth
     */
    @Test
    @Tag(REGRESSION)
    void testMirrorMakerTlsScramSha() throws Exception {
        operationID = startTimeMeasuring(Operation.MM_DEPLOYMENT);
        String kafkaUserSource = "my-user-source";
        String kafkaUserTarget = "my-user-target";
        String kafkaSourceName = CLUSTER_NAME + "-source";
        String kafkaTargetName = CLUSTER_NAME + "-target";
        String topicName = TOPIC_NAME + "-" + rng.nextInt(Integer.MAX_VALUE);

        // Deploy source kafka with tls listener and SCRAM-SHA authentication
        testMethodResources().kafka(testMethodResources().defaultKafka(kafkaSourceName, 1, 1)
                .editSpec()
                .editKafka()
                .withNewListeners()
                .withNewTls().withAuth(new KafkaListenerAuthenticationScramSha512()).endTls()
                .endListeners()
                .endKafka()
                .endSpec().build()).done();

        // Deploy target kafka with tls listener and SCRAM-SHA authentication
        testMethodResources().kafka(testMethodResources().defaultKafka(kafkaTargetName, 1, 1)
                .editSpec()
                .editKafka()
                .withNewListeners()
                .withNewTls().withAuth(new KafkaListenerAuthenticationScramSha512()).endTls()
                .endListeners()
                .endKafka()
                .endSpec().build()).done();

        // Create Kafka user for source cluster
        KafkaUser userSource = testMethodResources().scramShaUser(kafkaSourceName, kafkaUserSource).done();
        waitTillSecretExists(kafkaUserSource);

        // Create Kafka user for target cluster
        KafkaUser userTarget = testMethodResources().scramShaUser(kafkaTargetName, kafkaUserTarget).done();
        waitTillSecretExists(kafkaUserTarget);

        // Initialize PasswordSecretSource to set this as PasswordSecret in Mirror Maker spec
        PasswordSecretSource passwordSecretSource = new PasswordSecretSource();
        passwordSecretSource.setSecretName(kafkaUserSource);
        passwordSecretSource.setPassword("password");

        // Initialize PasswordSecretSource to set this as PasswordSecret in Mirror Maker spec
        PasswordSecretSource passwordSecretTarget = new PasswordSecretSource();
        passwordSecretTarget.setSecretName(kafkaUserTarget);
        passwordSecretTarget.setPassword("password");

        // Initialize CertSecretSource with certificate and secret names for consumer
        CertSecretSource certSecretSource = new CertSecretSource();
        certSecretSource.setCertificate("ca.crt");
        certSecretSource.setSecretName(clusterCaCertSecretName(kafkaSourceName));

        // Initialize CertSecretSource with certificate and secret names for producer
        CertSecretSource certSecretTarget = new CertSecretSource();
        certSecretTarget.setCertificate("ca.crt");
        certSecretTarget.setSecretName(clusterCaCertSecretName(kafkaTargetName));

        // Deploy client
        testMethodResources().deployKafkaClients(true, CLUSTER_NAME, userSource, userTarget).done();

        // Check brokers availability
        availabilityTest(messagesCount, Constants.TIMEOUT_AVAILABILITY_TEST, kafkaSourceName, true, "my-topic-test-1", userSource);
        availabilityTest(messagesCount, Constants.TIMEOUT_AVAILABILITY_TEST, kafkaTargetName, true, "my-topic-test-2", userTarget);

        // Deploy Mirror Maker with TLS and ScramSha512
        testMethodResources().kafkaMirrorMaker(CLUSTER_NAME, kafkaSourceName, kafkaTargetName, "my-group" + rng.nextInt(Integer.MAX_VALUE), 1, true)
                .editSpec()
                .editConsumer()
                .withNewKafkaMirrorMakerAuthenticationScramSha512()
                .withUsername(kafkaUserSource)
                .withPasswordSecret(passwordSecretSource)
                .endKafkaMirrorMakerAuthenticationScramSha512()
                .withNewTls()
                .withTrustedCertificates(certSecretSource)
                .endTls()
                .endConsumer()
                .editProducer()
                .withNewKafkaMirrorMakerAuthenticationScramSha512()
                .withUsername(kafkaUserTarget)
                .withPasswordSecret(passwordSecretTarget)
                .endKafkaMirrorMakerAuthenticationScramSha512()
                .withNewTls()
                .withTrustedCertificates(certSecretTarget)
                .endTls()
                .endProducer()
                .endSpec().done();

        // Deploy topic
        testMethodResources().topic(kafkaSourceName, topicName).done();

        TimeMeasuringSystem.stopOperation(operationID);

        int sent = sendMessages(messagesCount, Constants.TIMEOUT_SEND_MESSAGES, kafkaSourceName, true, topicName, userSource);
        int receivedSource = receiveMessages(messagesCount, Constants.TIMEOUT_RECV_MESSAGES, kafkaSourceName, true, topicName, userSource);
        int receivedTarget = receiveMessages(messagesCount, Constants.TIMEOUT_RECV_MESSAGES, kafkaTargetName, true, topicName, userTarget);

        assertSentAndReceivedMessages(sent, receivedSource);
        assertSentAndReceivedMessages(sent, receivedTarget);
    }

    @BeforeEach
    void createTestResources() throws Exception {
        createTestMethodResources();
        testMethodResources.createServiceResource(Constants.KAFKA_CLIENTS, Environment.INGRESS_DEFAULT_PORT, NAMESPACE).done();
        testMethodResources.createIngress(Constants.KAFKA_CLIENTS, Environment.INGRESS_DEFAULT_PORT, CONFIG.getMasterUrl(), NAMESPACE).done();
    }

    @AfterEach
    void deleteTestResources() throws Exception {
        deleteTestMethodResources();
        waitForDeletion(Constants.TIMEOUT_TEARDOWN);
    }

    @BeforeAll
    void setupEnvironment() {
        LOGGER.info("Creating resources before the test class");
        prepareEnvForOperator(NAMESPACE);

        createTestClassResources();
        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        testClassResources.clusterOperator(NAMESPACE).done();
    }

    @AfterAll
    void teardownEnvironment() {
        testClassResources.deleteResources();
        teardownEnvForOperator();
    }

}
