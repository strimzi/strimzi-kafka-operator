/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;

import static java.util.Collections.singleton;

/**
 * Partial implementation of {@link Kafka} omitting those methods which imply a partition assignment.
 * Subclasses will need to implement those method according to their own semantics.
 * For example it is anticipated that one subclass will delegate to a "cluster balancer" so that cluster-wide,
 * traffic-aware assignments can be done.
 */
public class KafkaImpl implements Kafka {

    private final static ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaImpl.class);

    protected final Admin adminClient;

    protected final Vertx vertx;

    /**
     * Constructor
     *
     * @param adminClient  Instance of the Kafka AdminClient API
     * @param vertx        The Vertx instance
     */
    public KafkaImpl(Admin adminClient, Vertx vertx) {
        this.adminClient = adminClient;
        this.vertx = vertx;
    }

    /**
     * Delete a topic via the Kafka AdminClient API, calling the given handler
     * (in a different thread) with the result.
     *
     * @param reconciliation    Reconciliation marker
     * @param topicName         Name of the Kafka Topic
     */
    @Override
    public Future<Void> deleteTopic(Reconciliation reconciliation, TopicName topicName) {
        Promise<Void> handler = Promise.promise();
        LOGGER.debugCr(reconciliation, "Deleting topic {}", topicName);
        KafkaFuture<Void> future = adminClient.deleteTopics(
                singleton(topicName.toString())).topicNameValues().get(topicName.toString());
        mapFuture(future).onComplete(ar -> {
            // Complete the result future on the context thread.
            vertx.runOnContext(ignored -> {
                handler.handle(ar);
            });
        });
        return handler.future();
    }

    /**
     * Check the existence of a topic via the Kafka AdminClient API
     *
     * @param reconciliation    Reconciliation marker
     * @param topicName         Name of the Kafka Topic
     * @return Future which completes when the topic exists.
     */
    @Override
    public Future<Boolean> topicExists(Reconciliation reconciliation, TopicName topicName) {
        // Test existence by doing a validate-only creation and checking for topic exists exception.
        // This request goes to the controller, so is less susceptible to races
        // where we happen to query a broker which hasn't processed an UPDATE_METADATA
        // request yet
        return mapFuture(adminClient.createTopics(singleton(
            new NewTopic(topicName.toString(), 1, (short) 1)),
            new CreateTopicsOptions().validateOnly(true)).all())
                .map(ignored -> false)
                .recover(
                    e -> {
                        if (e instanceof ExecutionException) {
                            e = e.getCause();
                        }
                        if (e instanceof TopicExistsException) {
                            return Future.succeededFuture(true);
                        } else {
                            return Future.failedFuture(e);
                        }
                    });
    }


    /**
     * Updates the topic configuration via the Kafka AdminClient API
     * (in a different thread) with the result.
     *
     * @param reconciliation    Reconciliation marker
     * @param topic             The Kafka Topic
     * @return Future which completes when the topic is altered successfully.
     */
    @SuppressWarnings("deprecation")
    @Override
    public Future<Void> updateTopicConfig(Reconciliation reconciliation, Topic topic) {
        Map<ConfigResource, Config> configs = TopicSerialization.toTopicConfig(topic);
        KafkaFuture<Void> future = adminClient.alterConfigs(configs).values().get(configs.keySet().iterator().next());
        return mapFuture(future);
    }

    /**
     * Completes the returned Future on the Vertx event loop
     * with the topic config obtained from the Kafka AdminClient API.
     * The Future completes with a null result a topic with the given {@code topicName} does not exist.
     */
    @Override
    public Future<TopicMetadata> topicMetadata(Reconciliation reconciliation, TopicName topicName) {
        LOGGER.debugCr(reconciliation, "Getting metadata for topic {}", topicName);
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName.toString());
        return topicExists(reconciliation, topicName).compose(exists -> {
            if (exists) {
                Future<TopicDescription> topicDescriptionFuture = mapFuture(adminClient.describeTopics(
                        singleton(topicName.toString())).topicNameValues().get(topicName.toString()));
                Future<Config> configFuture = mapFuture(adminClient.describeConfigs(
                        singleton(resource)).values().get(resource));
                return CompositeFuture.all(topicDescriptionFuture, configFuture)
                        .map(compositeFuture -> new TopicMetadata(compositeFuture.resultAt(0), compositeFuture.resultAt(1)));
            } else {
                return Future.succeededFuture(null);
            }
        });
    }

    /**
     * Check the existence of a topic via the Kafka AdminClient API
     *
     * @return Future which return a set of topic names on successful completion
     */
    @Override
    public Future<Set<String>> listTopics() {
        try {
            LOGGER.debugOp("Listing topics");
            ListTopicsOptions listOptions = new ListTopicsOptions().listInternal(true);
            return mapFuture(adminClient.listTopics(listOptions).names());
        } catch (Exception e) {
            return Future.failedFuture(e);
        }
    }

    /**
     * Increase the number of partitions of a topic via the Kafka AdminClient API, calling the given handler
     * (in a different thread) with the result.
     *
     * @param reconciliation    Reconciliation Marker
     * @param topic             The Kafka topic
     * @return Future which completes when the partition count is increased
     */
    @Override
    public Future<Void> increasePartitions(Reconciliation reconciliation, Topic topic) {
        try {
            String topicName = topic.getTopicName().toString();
            final NewPartitions newPartitions = NewPartitions.increaseTo(topic.getNumPartitions());
            LOGGER.debugCr(reconciliation, "Increasing partitions {}", newPartitions);
            final Map<String, NewPartitions> request = Collections.singletonMap(topicName, newPartitions);
            return mapFuture(adminClient.createPartitions(request).values().get(topicName));
        } catch (Exception e) {
            return Future.failedFuture(e);
        }
    }

    /**
     * Create a new topic via the Kafka AdminClient API, calling the given handler
     * (in a different thread) with the result.
     *
     * @param reconciliation   Reconciliation marker
     * @param topic            The KafkaTopic to be created
     * @return Future which completes when the topic is created
     */
    @Override
    public Future<Void> createTopic(Reconciliation reconciliation, Topic topic) {
        try {
            NewTopic newTopic = TopicSerialization.toNewTopic(topic, null);
            LOGGER.debugCr(reconciliation, "Creating topic {}", newTopic);
            KafkaFuture<Void> future = adminClient.createTopics(
                    singleton(newTopic)).values().get(newTopic.name());
            return mapFuture(future);
        } catch (Exception e) {
            return Future.failedFuture(e);
        }
    }

    private <T> Future<T> mapFuture(KafkaFuture<T> future) {
        Promise<T> handler = Promise.promise();
        try {
            future.whenComplete((result, error) -> {
                vertx.runOnContext(ignored -> {
                    if (error != null) {
                        handler.fail(error);
                    } else {
                        handler.complete(result);
                    }
                });
            });
        } catch (Exception e) {
            handler.fail(e);
        }
        return handler.future();
    }

}
