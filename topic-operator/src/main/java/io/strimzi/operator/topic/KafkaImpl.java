/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import io.strimzi.operator.common.Util;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static java.util.Collections.singleton;

/**
 * Partial implementation of {@link Kafka} omitting those methods which imply a partition assignment.
 * Subclasses will need to implement those method according to their own semantics.
 * For example it is anticipated that one subclass will delegate to a "cluster balancer" so that cluster-wide,
 * traffic-aware assignments can be done.
 */
public class KafkaImpl implements Kafka {

    private final static Logger LOGGER = LogManager.getLogger(KafkaImpl.class);

    protected final Admin adminClient;

    protected final Vertx vertx;

    public KafkaImpl(Admin adminClient, Vertx vertx) {
        this.adminClient = adminClient;
        this.vertx = vertx;
    }

    /**
     * Delete a topic via the Kafka AdminClient API, calling the given handler
     * (in a different thread) with the result.
     */
    @Override
    public Future<Void> deleteTopic(TopicName topicName) {
        Promise<Void> handler = Promise.promise();
        LOGGER.debug("Deleting topic {}", topicName);
        KafkaFuture<Void> future = adminClient.deleteTopics(
                singleton(topicName.toString())).values().get(topicName.toString());
        mapFuture(future).compose(ig ->
                Util.waitFor(vertx, "deleted sync " + topicName, "deleted", 1000, 120_000, () -> {
                    try {
                        return adminClient.describeTopics(singleton(topicName.toString())).all().get().get(topicName.toString()) == null;
                    } catch (ExecutionException e) {
                        if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                            return true;
                        } else if (e.getCause() instanceof RuntimeException) {
                            throw (RuntimeException) e.getCause();
                        } else {
                            throw new RuntimeException(e);
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }, error -> true)
        ).onComplete(ar -> {
            // Complete the result future on the context thread.
            vertx.runOnContext(ignored -> {
                handler.handle(ar);
            });
        });
        return handler.future();
    }

    @SuppressWarnings("deprecation")
    @Override
    public Future<Void> updateTopicConfig(Topic topic) {
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
    public Future<TopicMetadata> topicMetadata(TopicName topicName) {
        LOGGER.debug("Getting metadata for topic {}", topicName);
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName.toString());
        Future<TopicDescription> topicDescriptionFuture = mapFuture(adminClient.describeTopics(
                singleton(topicName.toString())).values().get(topicName.toString()));
        Future<Config> configFuture = mapFuture(adminClient.describeConfigs(
                singleton(resource)).values().get(resource));
        return CompositeFuture.all(topicDescriptionFuture, configFuture)
        .map(compositeFuture ->
            new TopicMetadata(compositeFuture.resultAt(0), compositeFuture.resultAt(1)))
            .recover(error -> {
                if (error instanceof UnknownTopicOrPartitionException) {
                    return Future.succeededFuture(null);
                } else {
                    return Future.failedFuture(error);
                }
            });
    }

    @Override
    public Future<Set<String>> listTopics() {
        try {
            LOGGER.debug("Listing topics");
            ListTopicsOptions listOptions = new ListTopicsOptions().listInternal(true);
            return mapFuture(adminClient.listTopics(listOptions).names());
        } catch (Exception e) {
            return Future.failedFuture(e);
        }
    }


    @Override
    public Future<Void> increasePartitions(Topic topic) {
        try {
            String topicName = topic.getTopicName().toString();
            final NewPartitions newPartitions = NewPartitions.increaseTo(topic.getNumPartitions());
            LOGGER.debug("Increasing partitions {}", newPartitions);
            final Map<String, NewPartitions> request = Collections.singletonMap(topicName, newPartitions);
            return mapFuture(adminClient.createPartitions(request).values().get(topicName));
        } catch (Exception e) {
            return Future.failedFuture(e);
        }
    }

    /**
     * Create a new topic via the Kafka AdminClient API, calling the given handler
     * (in a different thread) with the result.
     */
    @Override
    public Future<Void> createTopic(Topic topic) {
        try {
            NewTopic newTopic = TopicSerialization.toNewTopic(topic, null);
            LOGGER.debug("Creating topic {}", newTopic);
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
