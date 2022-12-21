/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

/**
 * Kafka Streams topology provider for TopicStore.
 */
public class TopicStoreTopologyProvider implements Supplier<Topology> {
    private final String storeTopic;
    private final String topicStoreName;
    private final Properties kafkaProperties;
    private final ForeachAction<? super String, ? super Integer> dispatcher;

    protected TopicStoreTopologyProvider(
            String storeTopic,
            String topicStoreName,
            Properties kafkaProperties,
            ForeachAction<? super String, ? super Integer> dispatcher
    ) {
        this.storeTopic = storeTopic;
        this.topicStoreName = topicStoreName;
        this.kafkaProperties = kafkaProperties;
        this.dispatcher = dispatcher;
    }

    /**
     * @return  Computed Topology for TopicStore
     */
    @Override
    public Topology get() {
        StreamsBuilder builder = new StreamsBuilder();

        // Simple defaults
        Map<String, String> configuration = new HashMap<>();
        configuration.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        configuration.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "0");
        configuration.put(TopicConfig.SEGMENT_BYTES_CONFIG, String.valueOf(64 * 1024 * 1024));

        // Input topic command -- store topic
        // Key is Kafka topic name -- which is also used for KeyValue store key
        KStream<String, TopicCommand> topicRequest = builder.stream(
                storeTopic,
                Consumed.with(Serdes.String(), new TopicCommandSerde())
        );

        // Data structure holds all topic information
        StoreBuilder<KeyValueStore<String /* topic */, Topic>> topicStoreBuilder =
                Stores
                        .keyValueStoreBuilder(
                                Stores.inMemoryKeyValueStore(topicStoreName),
                                Serdes.String(), new TopicSerde()
                        )
                        .withCachingEnabled()
                        .withLoggingEnabled(configuration);

        builder.addStateStore(topicStoreBuilder);

        topicRequest.process(
            () -> new TopicCommandTransformer(topicStoreName, dispatcher),
            topicStoreName
        );

        return builder.build(kafkaProperties);
    }

    /**
     * This processor applies topic command to key-value store.
     * It then updates dispatcher with store modification result.
     * In the case of invalid store update result is not-null.
     * Dispatcher applies the result to a waiting callback CompletionStage.
     */
    private static class TopicCommandTransformer implements Processor<String, TopicCommand, Void, Void> {
        private final String topicStoreName;
        private final ForeachAction<? super String, ? super Integer> dispatcher;

        private KeyValueStore<String, Topic> store;

        protected TopicCommandTransformer(
                String topicStoreName,
                ForeachAction<? super String, ? super Integer> dispatcher
        ) {
            this.topicStoreName = topicStoreName;
            this.dispatcher = dispatcher;
        }

        /** Initialise the key value store */
        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context) {
            store = (KeyValueStore<String, Topic>) context.getStateStore(topicStoreName);
        }

        /**
         * Process and applies the topic command to key-value store
         *
         * @param record Record containing the topic command
         */
        @Override
        public void process(final Record<String, TopicCommand> record) {
            String uuid = record.value().getUuid();
            TopicCommand.Type type = record.value().getType();
            Integer result = null;
            switch (type) {
                case CREATE:
                    Topic previous = store.putIfAbsent(record.key(), record.value().getTopic());
                    if (previous != null) {
                        result = KafkaStreamsTopicStore.toIndex(TopicStore.EntityExistsException.class);
                    }
                    break;
                case UPDATE:
                    store.put(record.key(), record.value().getTopic());
                    break;
                case DELETE:
                    previous = store.delete(record.key());
                    if (previous == null) {
                        result = KafkaStreamsTopicStore.toIndex(TopicStore.NoSuchEntityExistsException.class);
                    }
                    break;
            }
            dispatcher.apply(uuid, result);
        }

        /** Overriden close method */
        @Override
        public void close() {
        }
    }
}