/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.api.kafka.model.KafkaTopic;
import io.vertx.core.Future;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import kafka.server.KafkaConfig$;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Properties;

@RunWith(VertxUnitRunner.class)
public class TopicOperatorTopicDeletionDisabledIT extends TopicOperatorBaseIT {

    @Override
    protected Properties kafkaClusterConfig() {
        Properties config = new Properties();
        config.setProperty(KafkaConfig$.MODULE$.DeleteTopicEnableProp(), "false");
        return config;
    }

    @Test
    public void testKafkaTopicDeletionDisabled(TestContext context) {
        // create the Topic Resource
        String topicName = "test-topic-deletion-disabled";
        // The creation method will wait for the topic to be ready in K8s
        KafkaTopic topicResource = createKafkaTopicResource(context, topicName);
        // Wait for the topic to be ready on the Kafka Broker
        waitForTopicInKafka(context, topicName, true);

        // Delete the k8 KafkaTopic and wait for that to be deleted
        deleteInKube(context, topicResource.getMetadata().getName());

        // trigger an immediate reconcile where, with with delete.topic.enable=false, the K8s KafkaTopic should be recreated
        Future<?> result = session.topicOperator.reconcileAllTopics("periodic");
        do {
            if (result.isComplete()) {
                break;
            }
        } while (true);

        // Wait for the KafkaTopic to be recreated
        waitForTopicInKube(context, topicName, true);
    }
}

