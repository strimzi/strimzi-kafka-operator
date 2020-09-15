/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafka.listeners;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.arraylistener.ArrayOrObjectKafkaListeners;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.kafkaclients.externalClients.BasicExternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static io.strimzi.systemtest.Constants.EXTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;

public class MultipleListenersST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(MultipleListenersST.class);
    public static final String NAMESPACE = "multiple-listeners-cluster-test";

    @Tag(NODEPORT_SUPPORTED)
    @Tag(EXTERNAL_CLIENTS_USED)
    @Test
    void testMultipleNodePort() {

        Map<String, String> kafkaSnapshot = StatefulSetUtils.ssSnapshot(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));

        KafkaResource.replaceKafkaResource(CLUSTER_NAME, kafka -> {
            kafka.getSpec().getKafka().setListeners(new ArrayOrObjectKafkaListeners(Arrays.asList(
                    new GenericKafkaListenerBuilder()
                        .withName("external1")
                        .withPort(9094)
                        .withType(KafkaListenerType.NODEPORT)
                        .withTls(false)
                        .build(),
                    new GenericKafkaListenerBuilder()
                        .withName("external2")
                        .withPort(9095)
                        .withType(KafkaListenerType.NODEPORT)
                        .withTls(false)
                        .build(),
                    new GenericKafkaListenerBuilder()
                        .withName("external3")
                        .withPort(9096)
                        .withType(KafkaListenerType.NODEPORT)
                        .withTls(false)
                        .build(),
                    new GenericKafkaListenerBuilder()
                        .withName("external4")
                        .withPort(9097)
                        .withType(KafkaListenerType.NODEPORT)
                        .withTls(false)
                        .build()
            ), null));
        });

        kafkaSnapshot = StatefulSetUtils.waitTillSsHasRolled(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3, kafkaSnapshot);
        StatefulSetUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);

        KafkaUtils.waitForKafkaStatusUpdate(CLUSTER_NAME);

        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME).done();

        BasicExternalKafkaClient basicExternalKafkaClient = new BasicExternalKafkaClient.Builder()
            .withTopicName(TOPIC_NAME)
            .withNamespaceName(NAMESPACE)
            .withClusterName(CLUSTER_NAME)
            .withMessageCount(MESSAGE_COUNT)
            .build();

        // verify phase
        basicExternalKafkaClient.verifyProducedAndConsumedMessages(
            basicExternalKafkaClient.sendMessagesPlain(),
            basicExternalKafkaClient.receiveMessagesPlain()
        );
    }

    @Test
    void testMultipleLoadBalancer() {

    }

    @Test
    void testMultipleRoutes() {

    }

    @Test
    void  testMixtureOfExternalListeners() {

    }

    @Test
    void testMultipleInternalListeners() {

    }

    @Test
    void testCombinationOfInternalAndExternalListeners() {

    }

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        installClusterOperator(NAMESPACE);

        LOGGER.info("Setting up shared Kafka cluster.");
        KafkaResource.kafkaPersistent(CLUSTER_NAME, 3).done();
    }
}
