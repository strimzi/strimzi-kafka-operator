/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.apicurio.registry.utils.kafka.ProducerActions;
import io.vertx.core.Future;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    public static Throwable toThrowable(Integer index) {
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

    public static Integer toIndex(Class<? extends Throwable> ct) {
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

    private Future<Void> handleTopicCommand(TopicCommand cmd) {
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

    @Override
    public Future<Void> create(Topic topic) {
        TopicCommand cmd = TopicCommand.create(topic);
        return handleTopicCommand(cmd);
    }

    @Override
    public Future<Void> update(Topic topic) {
        TopicCommand cmd = TopicCommand.update(topic);
        return handleTopicCommand(cmd);
    }

    @Override
    public Future<Void> delete(TopicName topic) {
        TopicCommand cmd = TopicCommand.delete(topic);
        return handleTopicCommand(cmd);
    }
}
