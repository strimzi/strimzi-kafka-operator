/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.apicurio.registry.utils.streams.diservice.AsyncBiFunctionService;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Properties;

/**
 * Simple local / in-memory store and service factory which is sufficient when the TO runs in a single pod
 */
class LocalStoreAndServiceFactory implements StoreAndServiceFactory {

    /**
     * Creates the local/in-memory store
     */
    @SuppressWarnings("unchecked")
    public StoreContext create(
            Config config,
            Properties kafkaProperties,
            KafkaStreams streams,
            AsyncBiFunctionService.WithSerdes<String, String, Integer> serviceImpl,
            List<AutoCloseable> closeables
    ) {
        String storeName = config.get(Config.STORE_NAME);
        ReadOnlyKeyValueStore<String, Topic> store = (ReadOnlyKeyValueStore<String, Topic>) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class[]{ReadOnlyKeyValueStore.class},
                new LazyInvocationHandler(streams, storeName)
        );
        return new StoreContext(store, serviceImpl);
    }

    // we need to lazily create the store as streams might not be ready yet
    private static class LazyInvocationHandler implements InvocationHandler {
        private final KafkaStreams streams;
        private final String storeName;

        private ReadOnlyKeyValueStore<String, Topic> store;

        protected LazyInvocationHandler(KafkaStreams streams, String storeName) {
            this.streams = streams;
            this.storeName = storeName;
        }

        /**
         * Overriden invoke method whihc
         */
        @Override
        public synchronized Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (store == null) {
                store = streams.store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
            }
            try {
                return method.invoke(store, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }
    }
}

