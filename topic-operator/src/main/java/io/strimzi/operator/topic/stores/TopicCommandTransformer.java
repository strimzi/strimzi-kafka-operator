/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.stores;

import io.strimzi.operator.topic.KafkaStreamsTopicStore;
import io.strimzi.operator.topic.Topic;
import io.strimzi.operator.topic.TopicCommand;
import io.strimzi.operator.topic.stores.exceptions.EntityExistsException;
import io.strimzi.operator.topic.stores.exceptions.NoSuchEntityExistsException;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This processor applies topic command to key-value store.
 * It then updates dispatcher with store modification result.
 * In the case of invalid store update result is not-null.
 * Dispatcher applies the result to a waiting callback CompletionStage.
 */
public class TopicCommandTransformer implements Processor<String, TopicCommand, Void, Void> {
    private final String topicStoreName;
    private final ForeachAction<? super String, ? super Integer> dispatcher;

    private KeyValueStore<String, Topic> store;

    private static final Logger LOG = LogManager.getLogger(TopicCommandTransformer.class);

    public TopicCommandTransformer(
            String topicStoreName,
            ForeachAction<? super String, ? super Integer> dispatcher
    ) {
        this.topicStoreName = topicStoreName;
        this.dispatcher = dispatcher;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        withStore((KeyValueStore<String, Topic>) context.getStateStore(topicStoreName));
    }

    public TopicCommandTransformer withStore(KeyValueStore<String, Topic> store) {
        this.store = store;
        return this;
    }

    @Override
    public void process(final Record<String, TopicCommand> record) {
        String uuid = record.value().getUuid();
        TopicCommand.Type type = record.value().getType();
        Integer result = null;

        LOG.trace("Processing UUID {}, topic {}, type: {}", uuid, record.key(), type);
        switch (type) {
            case CREATE:
                Topic previous = store.putIfAbsent(record.key(), record.value().getTopic());
                if (previous != null) {
                    result = KafkaStreamsTopicStore.toIndex(EntityExistsException.class);
                }
                break;
            case UPDATE:
                store.put(record.key(), record.value().getTopic());
                break;
            case DELETE:
                previous = store.delete(record.key());
                if (previous == null) {
                    result = KafkaStreamsTopicStore.toIndex(NoSuchEntityExistsException.class);
                }
                break;
        }
        LOG.trace("Completed, UUID {}, result: {}", uuid, result);
        dispatcher.apply(uuid, result);
    }

    @Override
    public void close() {
    }
}