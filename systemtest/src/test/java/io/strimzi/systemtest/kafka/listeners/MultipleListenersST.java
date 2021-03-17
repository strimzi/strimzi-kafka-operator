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
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.kafkaclients.externalClients.BasicExternalKafkaClient;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.templates.crd.KafkaClientsTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

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
    private Object lock = new Object();

    // only 4 type of listeners
    private Map<KafkaListenerType, List<GenericKafkaListener>> testCases = new HashMap<>(4);

    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testMultipleNodePorts(ExtensionContext extensionContext) throws Exception {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        runListenersTest(extensionContext, testCases.get(KafkaListenerType.NODEPORT), clusterName);
    }

    @Tag(INTERNAL_CLIENTS_USED)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testMultipleInternal(ExtensionContext extensionContext) throws Exception {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        runListenersTest(extensionContext, testCases.get(KafkaListenerType.INTERNAL), clusterName);
    }

    @Tag(NODEPORT_SUPPORTED)
    @Tag(ACCEPTANCE)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testCombinationOfInternalAndExternalListeners(ExtensionContext extensionContext) throws Exception {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        List<GenericKafkaListener> multipleDifferentListeners = new ArrayList<>();

        List<GenericKafkaListener> internalListeners = testCases.get(KafkaListenerType.INTERNAL);
        multipleDifferentListeners.addAll(internalListeners);

        List<GenericKafkaListener> nodeportListeners = testCases.get(KafkaListenerType.NODEPORT);
        multipleDifferentListeners.addAll(nodeportListeners);

        // run INTERNAL + NODEPORT listeners
        runListenersTest(extensionContext, multipleDifferentListeners, clusterName);
    }

    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testMultipleLoadBalancers(ExtensionContext extensionContext) throws Exception {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        runListenersTest(extensionContext, testCases.get(KafkaListenerType.LOADBALANCER), clusterName);
    }

    @OpenShiftOnly
    @Tag(EXTERNAL_CLIENTS_USED)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testMultipleRoutes(ExtensionContext extensionContext) throws Exception {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        runListenersTest(extensionContext, testCases.get(KafkaListenerType.ROUTE), clusterName);
    }

    @OpenShiftOnly
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testMixtureOfExternalListeners(ExtensionContext extensionContext) throws Exception {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        List<GenericKafkaListener> multipleDifferentListeners = new ArrayList<>();

        List<GenericKafkaListener> routeListeners = testCases.get(KafkaListenerType.ROUTE);
        List<GenericKafkaListener> nodeportListeners = testCases.get(KafkaListenerType.NODEPORT);

        multipleDifferentListeners.addAll(routeListeners);
        multipleDifferentListeners.addAll(nodeportListeners);

        // run ROUTE + NODEPORT listeners
        runListenersTest(extensionContext, multipleDifferentListeners, clusterName);
    }

    @OpenShiftOnly
    @Tag(NODEPORT_SUPPORTED)
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testCombinationOfEveryKindOfListener(ExtensionContext extensionContext) throws Exception {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

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
        runListenersTest(extensionContext, multipleDifferentListeners, clusterName);
    }

    private void runListenersTest(ExtensionContext extensionContext, List<GenericKafkaListener> listeners, String clusterName) throws Exception {

        LOGGER.info("This is listeners {}, which will verified.", listeners);

        // exercise phase
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editSpec()
                .editKafka()
                    .withNewListeners()
                        .withGenericKafkaListeners(listeners)
                    .endListeners()
                .endKafka()
            .endSpec()
            .build());

        // only on thread can access to verification phase (here is a lot of variables which can be modified in run-time (data-race))
        synchronized (lock) {
            String kafkaUsername = KafkaUserUtils.generateRandomNameOfKafkaUser();
            KafkaUser kafkaUserInstance = KafkaUserTemplates.tlsUser(clusterName, kafkaUsername).build();

            resourceManager.createResource(extensionContext, kafkaUserInstance);

            for (GenericKafkaListener listener : listeners) {

                String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
                resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());

                boolean isTlsEnabled = listener.isTls();

                if (listener.getType() != KafkaListenerType.INTERNAL) {
                    if (isTlsEnabled) {
                        BasicExternalKafkaClient externalTlsKafkaClient = new BasicExternalKafkaClient.Builder()
                            .withTopicName(topicName)
                            .withNamespaceName(NAMESPACE)
                            .withClusterName(clusterName)
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
                            .withClusterName(clusterName)
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
                    String kafkaClientsName = mapWithKafkaClientNames.get(extensionContext.getDisplayName());
                    // using internal clients
                    if (isTlsEnabled) {
                        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(true, kafkaClientsName + "-tls",
                            listener.getName(), kafkaUserInstance).build());

                        final String kafkaClientsTlsPodName =
                            ResourceManager.kubeClient().listPodsByPrefixInName(kafkaClientsName + "-tls").get(0).getMetadata().getName();

                        InternalKafkaClient internalTlsKafkaClient = new InternalKafkaClient.Builder()
                            .withUsingPodName(kafkaClientsTlsPodName)
                            .withListenerName(listener.getName())
                            .withTopicName(topicName)
                            .withNamespaceName(NAMESPACE)
                            .withClusterName(clusterName)
                            .withKafkaUsername(kafkaUsername)
                            .withMessageCount(MESSAGE_COUNT)
                            .build();

                        LOGGER.info("Checking produced and consumed messages to pod:{}", kafkaClientsTlsPodName);

                        // verify phase
                        ClientUtils.waitUntilProducerAndConsumerSuccessfullySendAndReceiveMessages(extensionContext, internalTlsKafkaClient);
                    } else {
                        resourceManager.createResource(extensionContext, KafkaClientsTemplates.kafkaClients(false, kafkaClientsName + "-plain").build());
                        final String kafkaClientsPlainPodName =
                            ResourceManager.kubeClient().listPodsByPrefixInName(kafkaClientsName + "-plain").get(0).getMetadata().getName();

                        InternalKafkaClient internalPlainKafkaClient = new InternalKafkaClient.Builder()
                            .withUsingPodName(kafkaClientsPlainPodName)
                            .withListenerName(listener.getName())
                            .withTopicName(topicName)
                            .withNamespaceName(NAMESPACE)
                            .withClusterName(clusterName)
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
    void setup(ExtensionContext extensionContext) {
        installClusterOperator(extensionContext, NAMESPACE);
        generateTestCases();
    }
}
