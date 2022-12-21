/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.apicurio.registry.utils.streams.diservice.AsyncBiFunctionService;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

/**
 * SPI for the store and service creation.
 * * in-memory / local
 * * distributed
 */
interface StoreAndServiceFactory {

    class StoreContext {
        ReadOnlyKeyValueStore<String, Topic> store;
        BiFunction<String, String, CompletionStage<Integer>> service;

        public StoreContext(ReadOnlyKeyValueStore<String, Topic> store, BiFunction<String, String, CompletionStage<Integer>> service) {
            this.store = Objects.requireNonNull(store);
            this.service = Objects.requireNonNull(service);
        }

        protected ReadOnlyKeyValueStore<String, Topic> getStore() {
            return store;
        }

        protected BiFunction<String, String, CompletionStage<Integer>> getService() {
            return service;
        }
    }

    /**
     * Create store and service context/tuple.
     *
     * Lookup service is a local representation of a callback service / bi-function
     * which is used when we lookup a CRUD result.
     * It takes in key (topic name) and uuid,
     * and returns exceptions enum or null if no exception.
     *
     * The list of closeables is a handle which gathers any closeable instances during
     * this create method invocation.
     * The closeables are called when KafkaStreamsTopicStoreService stop method is invoked.
     * It invokes a close method on them, in reverse order.
     *
     * @param config the topic operator configuration
     * @param kafkaProperties the Kafka properties
     * @param streams the Kafka streams instance
     * @param lookupService the lookup service
     * @param closeables the list of closeables
     * @return store and service tuple
     */
    StoreContext create(
            Config config,
            Properties kafkaProperties,
            KafkaStreams streams,
            AsyncBiFunctionService.WithSerdes<String, String, Integer> lookupService,
            List<AutoCloseable> closeables
    );
}
