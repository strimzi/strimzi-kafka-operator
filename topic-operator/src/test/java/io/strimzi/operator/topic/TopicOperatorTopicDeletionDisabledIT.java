/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.api.kafka.model.KafkaTopic;
import io.vertx.core.Future;
import kafka.server.KafkaConfig$;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class TopicOperatorTopicDeletionDisabledIT extends TopicOperatorBaseIT {

    @Override
    protected int numKafkaBrokers() {
        return 1;
    }

    @Override
    protected Properties kafkaClusterConfig() {
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

