/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka.listeners;

import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.testframe.resources.KubeResourceManager;
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
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.kafkaclients.externalClients.ExternalKafkaClient;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static io.strimzi.systemtest.TestTags.ACCEPTANCE;
import static io.strimzi.systemtest.TestTags.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.TestTags.LOADBALANCER_SUPPORTED;
import static io.strimzi.systemtest.TestTags.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.TestTags.ROUTE;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(REGRESSION)
@SuiteDoc(
    description = @Desc("Test to verify the functionality of using multiple NodePort listeners in a Kafka cluster within the same namespace."),
    labels = {
        @Label(value = TestDocsLabels.KAFKA)
    }
)
public class MultipleListenersST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MultipleListenersST.class);
    private Object lock = new Object();

    // only 4 type of listeners
    private Map<KafkaListenerType, List<GenericKafkaListener>> testCases = new HashMap<>(4);

    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @TestDoc(
        description = @Desc("Test verifying the functionality of using multiple NodePort listeners in a Kafka cluster within the same namespace."),
        steps = {
            @Step(value = "Execute listener tests with NodePort configuration.", expected = "Listener tests run without issues using NodePort.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testMultipleNodePorts() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        runListenersTest(testCases.get(KafkaListenerType.NODEPORT), testStorage.getClusterName());
    }

    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @TestDoc(
        description = @Desc("Test to verify the usage of more than one Kafka cluster within a single namespace."),
        steps = {
            @Step(value = "Run the Internal Kafka listeners test.", expected = "Listeners test runs successfully on the specified cluster.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testMultipleInternal() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        runListenersTest(testCases.get(KafkaListenerType.INTERNAL), testStorage.getClusterName());
    }

    @Tag(NODEPORT_SUPPORTED)
    @Tag(ACCEPTANCE)
    @Tag(EXTERNAL_CLIENTS_USED)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @TestDoc(
        description = @Desc("Test verifying the combination of Internal and Enternal Kafka listeners."),
        steps = {
            @Step(value = "Check if the environment supports cluster-wide NodePort rights.", expected = "Test is skipped if the environment is not suitable."),
            @Step(value = "Retrieve and combine Internal and NodePort listeners.", expected = "Listeners are successfully retrieved and combined."),
            @Step(value = "Run listeners test with combined listeners.", expected = "Listeners test is executed successfully.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testCombinationOfInternalAndExternalListeners() {
        // Nodeport needs cluster wide rights to work properly which is not possible with STRIMZI_RBAC_SCOPE=NAMESPACE
        assumeFalse(Environment.isNamespaceRbacScope());
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        List<GenericKafkaListener> multipleDifferentListeners = new ArrayList<>();

        List<GenericKafkaListener> internalListeners = testCases.get(KafkaListenerType.INTERNAL);
        multipleDifferentListeners.addAll(internalListeners);

        List<GenericKafkaListener> nodeportListeners = testCases.get(KafkaListenerType.NODEPORT);
        multipleDifferentListeners.addAll(nodeportListeners);

        // run INTERNAL + NODEPORT listeners
        runListenersTest(multipleDifferentListeners, testStorage.getClusterName());
    }

    @Tag(LOADBALANCER_SUPPORTED)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @Tag(EXTERNAL_CLIENTS_USED)
    @TestDoc(
        description = @Desc("Test verifying the behavior of multiple LoadBalancers in a single namespace using more than one Kafka cluster."),
        steps = {
            @Step(value = "Run listeners test with LoadBalancer type.", expected = "Listeners test executes successfully with LoadBalancers."),
            @Step(value = "Validate the results.", expected = "Results match the expected outcomes for multiple LoadBalancers.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testMultipleLoadBalancers() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        runListenersTest(testCases.get(KafkaListenerType.LOADBALANCER), testStorage.getClusterName());
    }

    @OpenShiftOnly
    @Tag(EXTERNAL_CLIENTS_USED)
    @Tag(ROUTE)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @TestDoc(
        description = @Desc("Test to verify the functionality of multiple Kafka route listeners in a single namespace."),
        steps = {
            @Step(value = "Retrieve test cases for Kafka Listener Type Route.", expected = "Test cases for Route are retrieved."),
            @Step(value = "Run listener tests using the retrieved test cases and cluster name.", expected = "Listener tests run successfully with no errors.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testMultipleRoutes() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        runListenersTest(testCases.get(KafkaListenerType.ROUTE), testStorage.getClusterName());
    }

    @OpenShiftOnly
    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @TestDoc(
        description = @Desc("Test ensuring that different types of Enternal Kafka listeners (Route and NodePort) work correctly when mixed."),
        steps = {
            @Step(value = "Retrieve route listeners.", expected = "Route listeners are retrieved from test cases."),
            @Step(value = "Retrieve NodePort listeners.", expected = "Nodeport listeners are retrieved from test cases."),
            @Step(value = "Combine route and NodePort listeners.", expected = "Multiple different listeners list is populated."),
            @Step(value = "Run listeners test.", expected = "Listeners test runs using the combined list.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testMixtureOfExternalListeners() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        List<GenericKafkaListener> multipleDifferentListeners = new ArrayList<>();

        List<GenericKafkaListener> routeListeners = testCases.get(KafkaListenerType.ROUTE);
        List<GenericKafkaListener> nodeportListeners = testCases.get(KafkaListenerType.NODEPORT);

        multipleDifferentListeners.addAll(routeListeners);
        multipleDifferentListeners.addAll(nodeportListeners);

        // run ROUTE + NODEPORT listeners
        runListenersTest(multipleDifferentListeners, testStorage.getClusterName());
    }

    @OpenShiftOnly
    @Tag(NODEPORT_SUPPORTED)
    @Tag(LOADBALANCER_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @IsolatedTest("Using more tha one Kafka cluster in one namespace")
    @TestDoc(
        description = @Desc("Verifies the combination of every kind of Kafka listener: Internal, NodePort, Route, and LOADBALANCER."),
        steps = {
            @Step(value = "Retrieve different types of Kafka listeners.", expected = "Lists of Internal, NodePort, Route, and LOADBALANCER listeners are retrieved."),
            @Step(value = "Combine all different listener lists.", expected = "A combined list of all Kafka listener types is created."),
            @Step(value = "Run listeners test with combined listener list.", expected = "Listeners test runs with all types of Kafka listeners in the combined list.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testCombinationOfEveryKindOfListener() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

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
        runListenersTest(multipleDifferentListeners, testStorage.getClusterName());
    }

    private void runListenersTest(List<GenericKafkaListener> listeners, String clusterName) {
        LOGGER.info("These are listeners to be verified: {}", listeners);
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), clusterName, 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), clusterName, 3).build()
        );
        // exercise phase
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(Environment.TEST_SUITE_NAMESPACE, clusterName, 3)
            .editSpec()
                .editKafka()
                    .withListeners(listeners)
                .endKafka()
            .endSpec()
            .build());

        // only on thread can access to verification phase (here is a lot of variables which can be modified in run-time (data-race))
        synchronized (lock) {
            String kafkaUsername = KafkaUserUtils.generateRandomNameOfKafkaUser();
            KafkaUser kafkaUserInstance = KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, kafkaUsername, clusterName).build();

            KubeResourceManager.get().createResourceWithWait(kafkaUserInstance);

            for (GenericKafkaListener listener : listeners) {
                final String producerName = "producer-" + new Random().nextInt(Integer.MAX_VALUE);
                final String consumerName = "consumer-" + new Random().nextInt(Integer.MAX_VALUE);

                String topicName = KafkaTopicUtils.generateRandomNameOfTopic();
                KubeResourceManager.get().createResourceWithWait(KafkaTopicTemplates.topic(Environment.TEST_SUITE_NAMESPACE, topicName, clusterName).build());

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
                        KubeResourceManager.get().createResourceWithWait(
                            kafkaClients.producerTlsStrimzi(clusterName),
                            kafkaClients.consumerTlsStrimzi(clusterName)
                        );
                    } else {
                        KubeResourceManager.get().createResourceWithWait(
                            kafkaClients.producerStrimzi(),
                            kafkaClients.consumerStrimzi()
                        );
                    }
                    ClientUtils.waitForClientsSuccess(Environment.TEST_SUITE_NAMESPACE, consumerName, producerName, testStorage.getMessageCount());
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
    protected void beforeAllMayOverride() {
        // first invoke classic @BeforeAll
        super.beforeAllMayOverride();
        // secondly generate test cases
        generateTestCases();

        SetupClusterOperator
            .getInstance()
            .withDefaultConfiguration()
            .install();
    }
}
