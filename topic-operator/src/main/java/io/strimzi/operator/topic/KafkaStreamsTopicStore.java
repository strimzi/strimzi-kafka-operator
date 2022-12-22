/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.apicurio.registry.utils.kafka.ProducerActions;
import io.vertx.core.Future;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

/**
 * TopicStore based on Kafka Streams and
 * Apicurio Registry's gRPC based Kafka Streams ReadOnlyKeyValueStore
 */
public class KafkaStreamsTopicStore implements TopicStore {
    private static final Logger LOGGER = LogManager.getLogger(KafkaStreamsTopicStore.class);

    private final ReadOnlyKeyValueStore<String, Topic> topicStore;

    private final String storeTopic;
    private final ProducerActions<String, TopicCommand> producer;

    private final BiFunction<String, String, CompletionStage<Integer>> resultService;

    /**
     * Constructor
     *
     * @param topicStore   Read-only topic store containing the topics.
     * @param storeTopic   Name of the topic store
     * @param producer     Producer actions
     * @param resultService  Bifunction
     */
    public KafkaStreamsTopicStore(
            ReadOnlyKeyValueStore<String, Topic> topicStore,
            String storeTopic,
            ProducerActions<String, TopicCommand> producer,
            BiFunction<String, String, CompletionStage<Integer>> resultService) {
        this.topicStore = topicStore;
        this.storeTopic = storeTopic;
        this.producer = producer;
        this.resultService = resultService;
    }


    private static Throwable toThrowable(Integer index) {
        if (index == null) {
            return null;
        }
        switch (index) {
            case 1:
                return new TopicStore.EntityExistsException();
            case 2:
                return new TopicStore.NoSuchEntityExistsException();
            case 3:
                return new TopicStore.InvalidStateException();
            default:
                throw new IllegalStateException("Invalid index: " + index);
        }
    }

    protected static Integer toIndex(Class<? extends Throwable> ct) {
        if (ct.equals(TopicStore.EntityExistsException.class)) {
            return 1;
        }
        if (ct.equals(TopicStore.NoSuchEntityExistsException.class)) {
            return 2;
        }
        if (ct.equals(TopicStore.InvalidStateException.class)) {
            return 3;
        }
        throw new IllegalStateException("Unexpected value: " + ct);
    }

    @Override
    public Future<Topic> read(TopicName name) {
        try {
            Topic topic = topicStore.get(name.toString());
            return Future.succeededFuture(topic);
        } catch (Throwable t) {
            return Future.failedFuture(t);
        }
    }


    /**
     * Method to handle the topic commands
     *
     * @param cmd    Topic command like CREATE, UPDATE, DELETE
     * @return Future which completes when the topic command is handle successfully
     */
    protected Future<Void> handleTopicCommand(TopicCommand cmd) {
        LOGGER.debug("Handling topic command [{}]: {}", cmd.getType(), cmd.getKey());
        String key = cmd.getKey();
        CompletionStage<Throwable> result = resultService.apply(key, cmd.getUuid())
                .thenApply(KafkaStreamsTopicStore::toThrowable);
        // Kafka Streams can re-balance in-between these two calls ...
        producer.apply(new ProducerRecord<>(storeTopic, key, cmd))
                .whenComplete((r, t) -> {
                    if (t != null) {
                        LOGGER.error("Error sending topic command", t);
                    }
                });
        return Future.fromCompletionStage(result).compose(
            t -> t != null ? Future.failedFuture(t) : Future.succeededFuture()
        );
    }

    /**
     * Method to generate the topic command CREATE
     * which is later on handled by the `handleTopicCommand` method
     *
     * @param topic  The Kafka topic
     * @return Future which completes when the topic command is handled successfully
     */
    @Override
    public Future<Void> create(Topic topic) {
        TopicCommand cmd = TopicCommand.create(topic);
        return handleTopicCommand(cmd);
    }

    /**
     * Method to generate the topic command UPDATE
     * which is later on handled by the `handleTopicCommand` method
     *
     * @param topic  The Kafka topic
     * @return Future which completes when the topic command UPDATE is created and handled successfully
     */
    @Override
    public Future<Void> update(Topic topic) {
        TopicCommand cmd = TopicCommand.update(topic);
        return handleTopicCommand(cmd);
    }

    /**
     * Method to generate the topic command DELETE
     * which is later on handled by the `handleTopicCommand` method
     *
     * @param topic  The Kafka topic
     * @return Future which completes when the topic command DELETE is created and handled successfully
     */
    @Override
    public Future<Void> delete(TopicName topic) {
        TopicCommand cmd = TopicCommand.delete(topic);
        return handleTopicCommand(cmd);
    }
}
