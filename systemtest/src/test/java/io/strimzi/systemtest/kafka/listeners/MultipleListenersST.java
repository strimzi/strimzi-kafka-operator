/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka.listeners;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.OpenShiftOnly;
import io.strimzi.systemtest.kafkaclients.externalClients.ExternalKafkaClient;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static io.strimzi.systemtest.TestConstants.ACCEPTANCE;
import static io.strimzi.systemtest.TestConstants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestConstants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestConstants.LOADBALANCER_SUPPORTED;
import static io.strimzi.systemtest.TestConstants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(REGRESSION)
public class MultipleListenersST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MultipleListenersST.class);
    private Object lock = new Object();

    // only 4 type of listeners
    private Map<KafkaListenerType, List<GenericKafkaListener>> testCases = new HashMap<>(4);

    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testMultipleNodePorts(ExtensionContext extensionContext) throws Exception {
        final TestStorage testStorage = storageMap.get(extensionContext);
        String clusterName = testStorage.getClusterName();

        runListenersTest(extensionContext, testCases.get(KafkaListenerType.NODEPORT), clusterName);
    }

    @Tag(INTERNAL_CLIENTS_USED)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testMultipleInternal(ExtensionContext extensionContext) throws Exception {
        final TestStorage testStorage = storageMap.get(extensionContext);
        String clusterName = testStorage.getClusterName();

        runListenersTest(extensionContext, testCases.get(KafkaListenerType.INTERNAL), clusterName);
    }

    @Tag(NODEPORT_SUPPORTED)
    @Tag(ACCEPTANCE)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testCombinationOfInternalAndExternalListeners(ExtensionContext extensionContext) throws Exception {
        // Nodeport needs cluster wide rights to work properly which is not possible with STRIMZI_RBAC_SCOPE=NAMESPACE
        assumeFalse(Environment.isNamespaceRbacScope());
        final TestStorage testStorage = storageMap.get(extensionContext);
        String clusterName = testStorage.getClusterName();

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
        final TestStorage testStorage = storageMap.get(extensionContext);
        String clusterName = testStorage.getClusterName();

        runListenersTest(extensionContext, testCases.get(KafkaListenerType.LOADBALANCER), clusterName);
    }

    @OpenShiftOnly
    @Tag(EXTERNAL_CLIENTS_USED)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testMultipleRoutes(ExtensionContext extensionContext) throws Exception {
        final TestStorage testStorage = storageMap.get(extensionContext);
        final String clusterName = testStorage.getClusterName();

        runListenersTest(extensionContext, testCases.get(KafkaListenerType.ROUTE), clusterName);
    }

    @OpenShiftOnly
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(INTERNAL_CLIENTS_USED)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    void testMixtureOfExternalListeners(ExtensionContext extensionContext) throws Exception {
        final TestStorage testStorage = storageMap.get(extensionContext);
        final String clusterName = testStorage.getClusterName();

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
        final TestStorage testStorage = storageMap.get(extensionContext);
        final String clusterName = testStorage.getClusterName();

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

    private void runListenersTest(ExtensionContext extensionContext, List<GenericKafkaListener> listeners, String clusterName) {
        final TestStorage testStorage = storageMap.get(extensionContext);
        LOGGER.info("These are listeners to be verified: {}", listeners);

        // exercise phase
        resourceManager.createResourceWithWait(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .editSpec()
                .editKafka()
                    .withListeners(listeners)
                .endKafka()
            .endSpec()
            .build());

        // only on thread can access to verification phase (here is a lot of variables which can be modified in run-time (data-race))
        synchronized (lock) {
            String kafkaUsername = KafkaUserUtils.generateRandomNameOfKafkaUser();
            KafkaUser kafkaUserInstance = KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, clusterName, kafkaUsername).build();

            resourceManager.createResourceWithWait(extensionContext, kafkaUserInstance);

            for (GenericKafkaListener listener : listeners) {
                final String producerName = "producer-" + new Random().nextInt(Integer.MAX_VALUE);
                final String consumerName = "consumer-" + new Random().nextInt(Integer.MAX_VALUE);

                String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
                resourceManager.createResourceWithWait(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName, Environment.TEST_SUITE_NAMESPACE).build());

                boolean isTlsEnabled = listener.isTls();

                if (listener.getType() != KafkaListenerType.INTERNAL) {
                    if (isTlsEnabled) {
                        ExternalKafkaClient externalTlsKafkaClient = new ExternalKafkaClient.Builder()
                            .withTopicName(topicName)
                            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
                            .withClusterName(clusterName)
                            .withMessageCount(testStorage.getMessageCount())
                            .withKafkaUsername(kafkaUsername)
                            .withListenerName(listener.getName())
                            .withSecurityProtocol(SecurityProtocol.SSL)
                            .withListenerName(listener.getName())
                            .build();

                        LOGGER.info("Verifying {} listener", TestConstants.TLS_LISTENER_DEFAULT_NAME);

                        // verify phase
                        externalTlsKafkaClient.verifyProducedAndConsumedMessages(
                            externalTlsKafkaClient.sendMessagesTls(),
                            externalTlsKafkaClient.receiveMessagesTls()
                        );
                    } else {
                        ExternalKafkaClient externalPlainKafkaClient = new ExternalKafkaClient.Builder()
                            .withTopicName(topicName)
                            .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
                            .withClusterName(clusterName)
                            .withMessageCount(testStorage.getMessageCount())
                            .withSecurityProtocol(SecurityProtocol.PLAINTEXT)
                            .withListenerName(listener.getName())
                            .build();

                        LOGGER.info("Verifying {} listener", TestConstants.PLAIN_LISTENER_DEFAULT_NAME);

                        // verify phase
                        externalPlainKafkaClient.verifyProducedAndConsumedMessages(
                            externalPlainKafkaClient.sendMessagesPlain(),
                            externalPlainKafkaClient.receiveMessagesPlain()
                        );
                    }
                } else {
                    // using internal clients
                    KafkaClients kafkaClients = new KafkaClientsBuilder()
                        .withTopicName(topicName)
                        .withMessageCount(testStorage.getMessageCount())
                        .withProducerName(producerName)
                        .withConsumerName(consumerName)
                        .withUsername(kafkaUsername)
                        .withNamespaceName(Environment.TEST_SUITE_NAMESPACE)
                        .withBootstrapAddress(KafkaResources.bootstrapServiceName(clusterName) + ":" + listener.getPort())
                        .build();

                    if (isTlsEnabled) {
                        // verify phase
                        resourceManager.createResourceWithWait(extensionContext,
                            kafkaClients.producerTlsStrimzi(clusterName),
                            kafkaClients.consumerTlsStrimzi(clusterName)
                        );
                    } else {
                        resourceManager.createResourceWithWait(extensionContext,
                            kafkaClients.producerStrimzi(),
                            kafkaClients.consumerStrimzi()
                        );
                    }
                    ClientUtils.waitForClientsSuccess(producerName, consumerName, Environment.TEST_SUITE_NAMESPACE, testStorage.getMessageCount());
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
                            .withNewConfiguration()
                                .withFinalizers(LB_FINALIZERS)
                            .endConfiguration()
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

    @Override
    protected void beforeAllMayOverride(ExtensionContext extensionContext) {
        // first invoke classic @BeforeAll
        super.beforeAllMayOverride(extensionContext);
        // secondly generate test cases
        generateTestCases();

        this.clusterOperator = this.clusterOperator
            .defaultInstallation(extensionContext)
            .createInstallation()
            .runInstallation();
    }
}
