/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.api.kafka.model.KafkaTopic;
import io.vertx.core.Future;
import kafka.server.KafkaConfig$;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class TopicOperatorTopicDeletionDisabledIT extends TopicOperatorBaseIT {
    protected static EmbeddedKafkaCluster kafkaCluster;

    @BeforeAll
    public static void beforeAll() throws IOException {
        kafkaCluster = new EmbeddedKafkaCluster(numKafkaBrokers(), kafkaClusterConfig());
        kafkaCluster.start();

        setupKubeCluster();
    }

    @AfterAll
    public static void afterAll() {
        teardownKubeCluster();
        kafkaCluster.stop();
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        setup(kafkaCluster);
    }

    @AfterEach
    public void afterEach() throws InterruptedException, TimeoutException, ExecutionException {
        teardown(false);
    }

    protected static int numKafkaBrokers() {
        return 1;
    }

    protected static Properties kafkaClusterConfig() {
        Properties config = new Properties();
        config.setProperty(KafkaConfig$.MODULE$.DeleteTopicEnableProp(), "false");
        return config;
    }

    @Test
    public void testKafkaTopicDeletionDisabled() throws InterruptedException, ExecutionException, TimeoutException {
        // create the Topic Resource
        String topicName = "test-topic-deletion-disabled";
        // The creation method will wait for the topic to be ready in K8s
        KafkaTopic topicResource = createKafkaTopicResource(topicName);
        // Wait for the topic to be ready on the Kafka Broker
        waitForTopicInKafka(topicName, true);

        // Delete the k8 KafkaTopic and wait for that to be deleted
        deleteInKube(topicResource.getMetadata().getName());

        // trigger an immediate reconcile where, with with delete.topic.enable=false, the K8s KafkaTopic should be recreated
        Future<?> result = session.topicOperator.reconcileAllTopics("periodic");
        do {
            if (result.isComplete()) {
                break;
            }
        } while (true);

        // Wait for the KafkaTopic to be recreated
        waitForTopicInKube(topicName, true);
    }
}

