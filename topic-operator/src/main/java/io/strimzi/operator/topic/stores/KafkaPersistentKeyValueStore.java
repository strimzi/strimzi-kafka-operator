/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.stores;

import io.strimzi.operator.topic.Topic;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class KafkaPersistentKeyValueStore implements KeyValueStore<String, Topic> {

    private final Producer<String, Topic> persistenceProducer;
    ConcurrentHashMap<String, Topic> cachedTopics;
    private final String persistenceTopic;
    private final Admin admin;

    private static final Logger LOG = LogManager.getLogger(KafkaPersistentStore.class);

    public KafkaPersistentKeyValueStore(String persistenceTopic, Admin admin, Function<Map<String, Object>, Consumer<String, Topic>> persistenceConsumerCreator, Function<Map<String, Object>, Producer<String, Topic>> persistenceProducerCreator, Map<String, Object> kafkaClientProperties) {
        this.persistenceTopic = persistenceTopic;
        this.admin = admin;
        cachedTopics = loadStore(persistenceTopic, persistenceConsumerCreator, kafkaClientProperties);
        this.persistenceProducer = persistenceProducerCreator.apply(kafkaClientProperties);
    }

    @Override
    public Topic get(String key) {
        return cachedTopics.get(key);
    }

    @Override
    public void put(String key, Topic value) {
        LOG.trace("PUT {}: {}", key, value);
        writeToStoreTopic(key, value);
        cachedTopics.put(key, value);
        LOG.trace("PUT done");
    }

    @Override
    public Topic putIfAbsent(String key, Topic value) {
        LOG.trace("PUT_IF {}: {}", key, value);
        if (cachedTopics.containsKey(key)) {
            return cachedTopics.get(key);
        } else {
            put(key, value);
            return null;
        }
    }

    @Override
    public Topic delete(String key) {
        LOG.trace("DELETE {}", key);
        Topic removedTopic = cachedTopics.remove(key);
        if (removedTopic != null) {
            writeToStoreTopic(key, null);
        }
        return removedTopic;
    }

    private void writeToStoreTopic(String key, Topic value) {
        try {
            persistenceProducer.send(new ProducerRecord<>(persistenceTopic, key, value)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }


    private ConcurrentHashMap<String, Topic> loadStore(String storeTopic, Function<Map<String, Object>, Consumer<String, Topic>> persistenceConsumerCreator, Map<String, Object> kafkaClientProperties) {
        LOG.info("Loading store topic into cache");
        Consumer<String, Topic> initialConsumer = persistenceConsumerCreator.apply(kafkaClientProperties);
        initialConsumer.subscribe(List.of(storeTopic));
        ConcurrentHashMap<String, Topic> cache = new ConcurrentHashMap<>();
        boolean caughtUp = false;
        while (!caughtUp) {
            ConsumerRecords<String, Topic> records = initialConsumer.poll(Duration.ofMillis(50));
            caughtUp = records.isEmpty();
            records.forEach(cr -> cache.put(cr.key(), cr.value()));
        }

        //Tidy up so not spamming consumer group metadata with new groups after every load
        String consumerGroup = initialConsumer.groupMetadata().groupId();
        initialConsumer.close();
        admin.deleteConsumerGroups(List.of(consumerGroup));

        return cache;
    }

    // All the following are unused but required by interface
    @Override
    public KeyValueIterator<String, Topic> range(String from, String to) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public KeyValueIterator<String, Topic> all() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public long approximateNumEntries() {
        throw new UnsupportedOperationException("Not implemented");
    }


    @Override
    public String name() {
        throw new UnsupportedOperationException("Not implemented");
    }

    // The deprecation warning is legit, so suppress the warning about the suppression of that warning because Intellij
    // is having a rough day
    @Override
    @SuppressWarnings({"deprecation", "RedundantSuppression"})
    public void init(ProcessorContext context, StateStore root) {

        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void putAll(List<KeyValue<String, Topic>> entries) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean persistent() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean isOpen() {
        throw new UnsupportedOperationException("Not implemented");
    }

}