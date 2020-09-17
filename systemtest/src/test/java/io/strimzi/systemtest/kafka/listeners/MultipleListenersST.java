/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka.listeners;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.kafkaclients.AbstractKafkaClient;
import io.strimzi.systemtest.kafkaclients.clientproperties.ConsumerProperties;
import io.strimzi.systemtest.kafkaclients.clientproperties.ProducerProperties;
import io.strimzi.systemtest.kafkaclients.externalClients.BasicExternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBasicClientResource;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.JobUtils;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class MultipleListenersST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MultipleListenersST.class);
    public static final String NAMESPACE = "multiple-listeners-cluster-test";

    private ProducerProperties producerProperties;
    private ConsumerProperties consumerProperties;

    @TestFactory
    Iterator<DynamicTest> testMultipleListeners() {

        List<DynamicTest> dynamicTests = new ArrayList<>(10);
        List<List<GenericKafkaListener>> testCases = generateTestCases();

        testCases.forEach(listener -> dynamicTests.add(DynamicTest.dynamicTest("Test " + listener.get(0).getType() + " with count of " + listener.size(), () -> {
            // TODO: profiling...assume for profiles NODE_PORT, LOAD_BALANCER, ROUTE...

            // exercise phase
            KafkaResource.kafkaPersistent(CLUSTER_NAME, 3)
                .editSpec()
                .editKafka()
                .withNewListeners()
                .withGenericKafkaListeners(listener)
                .endListeners()
                .endKafka()
                .endSpec()
                .done();

            KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME).done();

            if (listener.get(0).getType() != KafkaListenerType.INTERNAL) {
                // using external clients
                producerProperties = new ProducerProperties.ProducerPropertiesBuilder()
                    .withNamespaceName(NAMESPACE)
                    .withClusterName(CLUSTER_NAME)
                    .withBootstrapServerConfig(AbstractKafkaClient.getExternalBootstrapConnect(NAMESPACE, CLUSTER_NAME))
                    .withKeySerializerConfig(StringSerializer.class)
                    .withValueSerializerConfig(StringSerializer.class)
                    .withClientIdConfig("producer-plain-" + new Random().nextInt(Integer.MAX_VALUE))
                    .withSecurityProtocol(SecurityProtocol.PLAINTEXT)
                    .withSharedProperties()
                    .build();

                consumerProperties = new ConsumerProperties.ConsumerPropertiesBuilder()
                    .withNamespaceName(NAMESPACE)
                    .withClusterName(CLUSTER_NAME)
                    .withBootstrapServerConfig(AbstractKafkaClient.getExternalBootstrapConnect(NAMESPACE, CLUSTER_NAME))
                    .withKeyDeserializerConfig(StringDeserializer.class)
                    .withValueDeserializerConfig(StringDeserializer.class)
                    .withClientIdConfig("consumer-plain-" + new Random().nextInt(Integer.MAX_VALUE))
                    .withGroupIdConfig("consumer-group-test")
                    .withAutoOffsetResetConfig(OffsetResetStrategy.EARLIEST)
                    .withSecurityProtocol(SecurityProtocol.PLAINTEXT)
                    .withSharedProperties()
                    .build();

                // verify phase

                for (int i = 0; i < listener.size() - 1; i++) {

                    KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME).done();

                    BasicExternalKafkaClient clientForExternal1 = new BasicExternalKafkaClient.Builder()
                        .withTopicName(TOPIC_NAME)
                        .withNamespaceName(NAMESPACE)
                        .withClusterName(CLUSTER_NAME)
                        .withMessageCount(MESSAGE_COUNT)
                        .withProducerProperties(producerProperties)
                        .withConsumerProperties(consumerProperties)
                        .build();

                    // verify phase
                    clientForExternal1.verifyProducedAndConsumedMessages(
                        clientForExternal1.sendMessagesPlain(),
                        clientForExternal1.receiveMessagesPlain()
                    );

                    BasicExternalKafkaClient clientForExternal2 = clientForExternal1.toBuilder(clientForExternal1)
                        .withProducerProperties(
                            producerProperties.toBuilder(producerProperties)
                                .withBootstrapServerConfig(AbstractKafkaClient.getExternalBootstrapConnect(NAMESPACE, CLUSTER_NAME, listener.get(i + 1).getName()))
                                .build())
                        .withConsumerProperties(
                            consumerProperties.toBuilder(consumerProperties)
                                .withBootstrapServerConfig(AbstractKafkaClient.getExternalBootstrapConnect(NAMESPACE, CLUSTER_NAME, listener.get(i + 1).getName()))
                                .build())
                        .build();

                    // verify phase
                    clientForExternal2.verifyProducedAndConsumedMessages(
                        clientForExternal2.sendMessagesPlain(),
                        clientForExternal2.receiveMessagesPlain()
                    );
                }
            } else {
                // using internal clients
                for (int i = 0; i < listener.size() - 1; i++) {

                    // exercise phase
                    final String producerName =  "producer-name";
                    final String consumerName  = "consumer-name";

                    // tls or plain
                    KafkaBasicClientResource kafkaBasicClientJob = listener.get(i).isTls() ? new KafkaBasicClientResource(producerName, consumerName,
                        KafkaResources.tlsBootstrapAddress(CLUSTER_NAME), TOPIC_NAME, MESSAGE_COUNT, "", ClientUtils.generateRandomConsumerGroup(), 1000) :
                        new KafkaBasicClientResource(producerName, consumerName,
                            KafkaResources.plainBootstrapAddress(CLUSTER_NAME), TOPIC_NAME, MESSAGE_COUNT, "", ClientUtils.generateRandomConsumerGroup(), 1000);

                    kafkaBasicClientJob.producerStrimzi().done();
                    kafkaBasicClientJob.consumerStrimzi().done();

                    // verify phase
                    ClientUtils.waitForClientSuccess(producerName, NAMESPACE, MESSAGE_COUNT);
                    ClientUtils.waitForClientSuccess(consumerName, NAMESPACE, MESSAGE_COUNT);


                    LOGGER.info("Deleting the Jobs");
                    // teardown (for clients)
                    JobUtils.deleteJobWithWait(NAMESPACE, producerName);
                    JobUtils.deleteJobWithWait(NAMESPACE, consumerName);
                }
            }
        })));
        return dynamicTests.iterator();
    }

    private List<List<GenericKafkaListener>> generateTestCases() {

        List<List<GenericKafkaListener>> testCases = new ArrayList<>(10);

        LOGGER.info("Starting to generate test cases for multiple listeners");

        for (int i = 0; i < 10; i++) {

            KafkaListenerType stochasticChosenListener = KafkaListenerType.values()[ThreadLocalRandom.current().nextInt(0, KafkaListenerType.values().length - 1)];
            List<GenericKafkaListener> testCase = new ArrayList<>(5);
            int stochasticCount;

            switch (stochasticChosenListener) {
                case NODEPORT:
                    stochasticCount = ThreadLocalRandom.current().nextInt(2, 5);

                    for (int j = 0; j < stochasticCount; j++) {

                        boolean stochasticCommunication = ThreadLocalRandom.current().nextInt(2) == 0;

                        testCase.add(new GenericKafkaListenerBuilder()
                            .withName(generateRandomListenerName())
                            .withPort(6090 + j)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(stochasticCommunication)
                            .build());
                    }
                    break;
                case LOADBALANCER:
                    stochasticCount = ThreadLocalRandom.current().nextInt(2, 3);

                    for (int j = 0; j < stochasticCount; j++) {

                        boolean stochasticCommunication = ThreadLocalRandom.current().nextInt(2) == 0;

                        testCase.add(new GenericKafkaListenerBuilder()
                            .withName(generateRandomListenerName())
                            .withPort(7090 + j)
                            .withType(KafkaListenerType.LOADBALANCER)
                            .withTls(stochasticCommunication)
                            .build());
                    }
                    break;
                case ROUTE:
                    stochasticCount = ThreadLocalRandom.current().nextInt(2, 3);

                    for (int j = 0; j < stochasticCount; j++) {

                        boolean stochasticCommunication = ThreadLocalRandom.current().nextInt(2) == 0;

                        testCase.add(new GenericKafkaListenerBuilder()
                            .withName(generateRandomListenerName())
                            .withPort(8090 + j)
                            .withType(KafkaListenerType.ROUTE)
                            .withTls(stochasticCommunication)
                            .build());
                    }
                    break;
                case INTERNAL:
                    stochasticCount = ThreadLocalRandom.current().nextInt(2, 4);

                    for (int j = 0; j < stochasticCount; j++) {

                        boolean stochasticCommunication = ThreadLocalRandom.current().nextInt(2) == 0;

                        testCase.add(new GenericKafkaListenerBuilder()
                            .withName(generateRandomListenerName())
                            .withPort(10090 + j)
                            .withType(KafkaListenerType.INTERNAL)
                            .withTls(stochasticCommunication)
                            .build());
                    }
                    break;
                default:
            }
            testCases.add(testCase);
        }

        LOGGER.info("Finished will generation of test cases for multiple listeners");

        return testCases;
    }

    private String generateRandomListenerName() {
        final String lexicon = "abcdefghilkfmnoprstwxyz";

        StringBuilder builder = new StringBuilder();

        while (builder.toString().length() == 0) {
            int length = new Random().nextInt(10) + 15;
            for (int i = 0; i < length; i++) {
                builder.append(lexicon.charAt(new Random().nextInt(lexicon.length())));
            }
        }
        return builder.toString();
    }

    // TODO: mixture test...
    @Test
    void  testMixtureOfExternalListeners() {

    }

    // TODO: mixture test...
    @Test
    void testCombinationOfInternalAndExternalListeners() {

    }

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);
    }
}
