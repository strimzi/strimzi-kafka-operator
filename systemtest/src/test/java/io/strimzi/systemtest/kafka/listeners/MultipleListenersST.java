/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka.listeners;

import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.kafkaclients.externalClients.BasicExternalKafkaClient;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.LOADBALANCER_SUPPORTED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;

@Tag(REGRESSION)
public class MultipleListenersST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MultipleListenersST.class);
    public static final String NAMESPACE = "multi-listener";

    // only 4 type of listeners
    private Map<KafkaListenerType, List<GenericKafkaListener>> testCases = new HashMap<>(4);

    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Test
    void testMultipleNodePorts() {
        runListenersTest(testCases.get(KafkaListenerType.NODEPORT));
    }

    @Tag(INTERNAL_CLIENTS_USED)
    @Test
    void testMultipleInternal() {
        runListenersTest(testCases.get(KafkaListenerType.INTERNAL));
    }

    @Tag(NODEPORT_SUPPORTED)
    @Tag(ACCEPTANCE)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @Test
    void testCombinationOfInternalAndExternalListeners() {
        List<GenericKafkaListener> multipleDifferentListeners = new ArrayList<>();

        List<GenericKafkaListener> internalListeners = testCases.get(KafkaListenerType.INTERNAL);
        multipleDifferentListeners.addAll(internalListeners);

        if (!Environment.isNamespaceRbacScope()) {
            List<GenericKafkaListener> nodeportListeners = testCases.get(KafkaListenerType.NODEPORT);
            multipleDifferentListeners.addAll(nodeportListeners);
        }

        // run INTERNAL + NODEPORT listeners
        runListenersTest(multipleDifferentListeners);
    }

    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Test
    void testMultipleLoadBalancers() {
        runListenersTest(testCases.get(KafkaListenerType.LOADBALANCER));
    }

    @OpenShiftOnly
    @Tag(EXTERNAL_CLIENTS_USED)
    @Test
    void testMultipleRoutes() {
        runListenersTest(testCases.get(KafkaListenerType.ROUTE));
    }

    @OpenShiftOnly
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @Test
    void testMixtureOfExternalListeners() {
        List<GenericKafkaListener> multipleDifferentListeners = new ArrayList<>();

        List<GenericKafkaListener> routeListeners = testCases.get(KafkaListenerType.ROUTE);
        List<GenericKafkaListener> nodeportListeners = testCases.get(KafkaListenerType.NODEPORT);

        multipleDifferentListeners.addAll(routeListeners);
        multipleDifferentListeners.addAll(nodeportListeners);

        // run ROUTE + NODEPORT listeners
        runListenersTest(multipleDifferentListeners);
    }

    @OpenShiftOnly
    @Tag(NODEPORT_SUPPORTED)
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @Test
    void testCombinationOfEveryKindOfListener() {
        List<GenericKafkaListener> multipleDifferentListeners = new ArrayList<>();

        List<GenericKafkaListener> internalListeners = testCases.get(KafkaListenerType.INTERNAL);
        List<GenericKafkaListener> nodeportListeners = testCases.get(KafkaListenerType.NODEPORT);
        List<GenericKafkaListener> routeListeners = testCases.get(KafkaListenerType.ROUTE);
        List<GenericKafkaListener> loadbalancersListeners = testCases.get(KafkaListenerType.LOADBALANCER);

        multipleDifferentListeners.addAll(internalListeners);
        multipleDifferentListeners.addAll(nodeportListeners);
        multipleDifferentListeners.addAll(routeListeners);
        multipleDifferentListeners.addAll(loadbalancersListeners);

        // run INTERNAL + NODEPORT + ROUTE + LOADBALANCER listeners
        runListenersTest(multipleDifferentListeners);
    }

    private void runListenersTest(List<GenericKafkaListener> listeners) {

        LOGGER.info("This is listeners {}, which will verified.", listeners);

        // exercise phase
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .withGenericKafkaListeners(listeners)
                    .endListeners()
                .endKafka()
            .endSpec()
            .done();

        String kafkaUsername = KafkaUserUtils.generateRandomNameOfKafkaUser();
        KafkaUser kafkaUserInstance = KafkaUserResource.tlsUser(CLUSTER_NAME, kafkaUsername).done();

        for (GenericKafkaListener listener : listeners) {

            String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
            KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();

            boolean isTlsEnabled = listener.isTls();

            if (listener.getType() != KafkaListenerType.INTERNAL) {
                if (isTlsEnabled) {
                    BasicExternalKafkaClient externalTlsKafkaClient = new BasicExternalKafkaClient.Builder()
                        .withTopicName(topicName)
                        .withNamespaceName(NAMESPACE)
                        .withClusterName(CLUSTER_NAME)
                        .withMessageCount(MESSAGE_COUNT)
                        .withKafkaUsername(kafkaUsername)
                        .withListenerName(listener.getName())
                        .withSecurityProtocol(SecurityProtocol.SSL)
                        .withListenerName(listener.getName())
                        .build();

                    LOGGER.info("Verifying {} listener", Constants.TLS_LISTENER_DEFAULT_NAME);

                    // verify phase
                    externalTlsKafkaClient.verifyProducedAndConsumedMessages(
                        externalTlsKafkaClient.sendMessagesTls(),
                        externalTlsKafkaClient.receiveMessagesTls()
                    );
                } else {
                    BasicExternalKafkaClient externalPlainKafkaClient = new BasicExternalKafkaClient.Builder()
                        .withTopicName(topicName)
                        .withNamespaceName(NAMESPACE)
                        .withClusterName(CLUSTER_NAME)
                        .withMessageCount(MESSAGE_COUNT)
                        .withSecurityProtocol(SecurityProtocol.PLAINTEXT)
                        .withListenerName(listener.getName())
                        .build();

                    LOGGER.info("Verifying {} listener", Constants.PLAIN_LISTENER_DEFAULT_NAME);

                    // verify phase
                    externalPlainKafkaClient.verifyProducedAndConsumedMessages(
                        externalPlainKafkaClient.sendMessagesPlain(),
                        externalPlainKafkaClient.receiveMessagesPlain()
                    );
                }
            } else {
                // using internal clients
                if (isTlsEnabled) {
                    KafkaClientsResource.deployKafkaClients(true, KAFKA_CLIENTS_NAME + "-tls",
                        listener.getName(), kafkaUserInstance).done();

                    final String kafkaClientsTlsPodName =
                        ResourceManager.kubeClient().listPodsByPrefixInName(KAFKA_CLIENTS_NAME + "-tls").get(0).getMetadata().getName();

                    InternalKafkaClient internalTlsKafkaClient = new InternalKafkaClient.Builder()
                        .withUsingPodName(kafkaClientsTlsPodName)
                        .withListenerName(listener.getName())
                        .withTopicName(topicName)
                        .withNamespaceName(NAMESPACE)
                        .withClusterName(CLUSTER_NAME)
                        .withKafkaUsername(kafkaUsername)
                        .withMessageCount(MESSAGE_COUNT)
                        .build();

                    LOGGER.info("Checking produced and consumed messages to pod:{}", kafkaClientsTlsPodName);

                    // verify phase
                    internalTlsKafkaClient.checkProducedAndConsumedMessages(
                        internalTlsKafkaClient.sendMessagesTls(),
                        internalTlsKafkaClient.receiveMessagesTls()
                    );
                } else {
                    KafkaClientsResource.deployKafkaClients(false, KAFKA_CLIENTS_NAME + "-plain").done();

                    final String kafkaClientsPlainPodName =
                        ResourceManager.kubeClient().listPodsByPrefixInName(KAFKA_CLIENTS_NAME + "-plain").get(0).getMetadata().getName();

                    InternalKafkaClient internalPlainKafkaClient = new InternalKafkaClient.Builder()
                        .withUsingPodName(kafkaClientsPlainPodName)
                        .withListenerName(listener.getName())
                        .withTopicName(topicName)
                        .withNamespaceName(NAMESPACE)
                        .withClusterName(CLUSTER_NAME)
                        .withMessageCount(MESSAGE_COUNT)
                        .build();

                    LOGGER.info("Checking produced and consumed messages to pod:{}", kafkaClientsPlainPodName);

                    // verify phase
                    internalPlainKafkaClient.checkProducedAndConsumedMessages(
                        internalPlainKafkaClient.sendMessagesPlain(),
                        internalPlainKafkaClient.receiveMessagesPlain()
                    );
                }
            }
        }
    }

    /**
     * Generates stochastic count of GenericKafkaListener for each type. Every type of listener has it's own count and
     * port generation interval.
     * @return HashMap which holds all generated listeners
     */
    private Map<KafkaListenerType, List<GenericKafkaListener>> generateTestCases() {

        LOGGER.info("Starting to generate test cases for multiple listeners");

        int stochasticCount;

        for (KafkaListenerType kafkaListenerType : KafkaListenerType.values()) {

            LOGGER.info("Generating {} listener", kafkaListenerType.name());

            List<GenericKafkaListener> testCaseListeners = new ArrayList<>(5);

            switch (kafkaListenerType) {
                case NODEPORT:
                    stochasticCount = ThreadLocalRandom.current().nextInt(2, 5);

                    for (int j = 0; j < stochasticCount; j++) {

                        boolean stochasticCommunication = ThreadLocalRandom.current().nextInt(2) == 0;

                        testCaseListeners.add(new GenericKafkaListenerBuilder()
                            .withName(KafkaListenerType.NODEPORT.toValue() + j)
                            .withPort(10900 + j)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(stochasticCommunication)
                            .build());
                    }
                    break;
                case LOADBALANCER:
                    stochasticCount = ThreadLocalRandom.current().nextInt(2, 3);

                    for (int j = 0; j < stochasticCount; j++) {

                        boolean stochasticCommunication = ThreadLocalRandom.current().nextInt(2) == 0;

                        testCaseListeners.add(new GenericKafkaListenerBuilder()
                            .withName(KafkaListenerType.LOADBALANCER.toValue().substring(0, 5) + j)
                            .withPort(11900 + j)
                            .withType(KafkaListenerType.LOADBALANCER)
                            .withTls(stochasticCommunication)
                            .build());
                    }
                    break;
                case ROUTE:
                    // TODO: bug with unique ports per listener which should be fixed now in Kafka 2.7.0
                    testCaseListeners.add(new GenericKafkaListenerBuilder()
                        .withName(KafkaListenerType.ROUTE.toValue())
                        .withPort(12091)
                        .withType(KafkaListenerType.ROUTE)
                        // Route or Ingress type listener and requires enabled TLS encryption
                        .withTls(true)
                        .build());
                    break;
                case INTERNAL:
                    stochasticCount = ThreadLocalRandom.current().nextInt(2, 4);

                    for (int j = 0; j < stochasticCount; j++) {

                        boolean stochasticCommunication = ThreadLocalRandom.current().nextInt(2) == 0;

                        testCaseListeners.add(new GenericKafkaListenerBuilder()
                            .withName(KafkaListenerType.INTERNAL.toValue() + j)
                            .withPort(13900 + j)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(stochasticCommunication)
                            .build());
                    }
            }
            LOGGER.info("Generating listeners with type {} -> {}", kafkaListenerType.name(), testCaseListeners.toArray());
            testCases.put(kafkaListenerType, testCaseListeners);
        }

        LOGGER.info("Finished with generation of test cases for multiple listeners");

        return testCases;
    }

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);

        generateTestCases();
    }
}
