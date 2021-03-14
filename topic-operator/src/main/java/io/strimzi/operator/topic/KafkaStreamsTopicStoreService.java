/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.apicurio.registry.utils.kafka.AsyncProducer;
import io.apicurio.registry.utils.kafka.ProducerActions;
import io.apicurio.registry.utils.streams.diservice.AsyncBiFunctionService;
import io.apicurio.registry.utils.streams.ext.ForeachActionDispatcher;
import io.apicurio.registry.utils.streams.ext.LoggingStateRestoreListener;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Integer.parseInt;

/**
 * A service to configure and start/stop KafkaStreamsTopicStore.
 */
public class KafkaStreamsTopicStoreService {
    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsTopicStoreService.class);

    private final List<AutoCloseable> closeables = new ArrayList<>();

    /* test */ KafkaStreams streams;
    /* test */ TopicStore store;

    public CompletionStage<TopicStore> start(Config config, Properties kafkaProperties) {
        String storeTopic = config.get(Config.STORE_TOPIC);
        String storeName = config.get(Config.STORE_NAME);

        // check if entry topic has the right configuration
        Admin admin = Admin.create(kafkaProperties);
        log.info("Starting ...");
        return toCS(admin.describeCluster().nodes())
                .thenApply(nodes -> new Context(nodes.size()))
                .thenCompose(c -> toCS(admin.listTopics().names()).thenApply(c::setTopics))
                .thenCompose(c -> {
                    if (c.topics.contains(storeTopic)) {
                        return validateExistingStoreTopic(storeTopic, admin, c);
                    } else {
                        return createNewStoreTopic(storeTopic, admin, c);
                    }
                })
                .thenCompose(v -> createKafkaStreams(config, kafkaProperties, storeTopic, storeName))
                .thenApply(serviceImpl -> createKafkaTopicStore(config, kafkaProperties, storeTopic, serviceImpl))
                .whenCompleteAsync((v, t) -> {
                    // use another thread to stop, if needed
                    try {
                        if (t != null) {
                            log.warn("Failed to start.", t);
                            stop();
                        } else {
                            log.info("Started.");
                        }
                    } finally {
                        close(admin);
                    }
                });
    }

    private TopicStore createKafkaTopicStore(Config config, Properties kafkaProperties, String storeTopic, AsyncBiFunctionService.WithSerdes<String, String, Integer> serviceImpl) {
        log.info("Creating topic store ...");
        ProducerActions<String, TopicCommand> producer = new AsyncProducer<>(
                kafkaProperties,
            Serdes.String().serializer(),
            new TopicCommandSerde()
        );
        closeables.add(producer);

        StoreAndServiceFactory factory = new LocalStoreAndServiceFactory();
        StoreAndServiceFactory.StoreContext sc = factory.create(config, kafkaProperties, streams, serviceImpl, closeables);

        this.store = new KafkaStreamsTopicStore(sc.getStore(), storeTopic, producer, sc.getService());
        return this.store;
    }

    private CompletableFuture<AsyncBiFunctionService.WithSerdes<String, String, Integer>> createKafkaStreams(Config config, Properties kafkaProperties, String storeTopic, String storeName) {
        log.info("Creating Kafka Streams, store name: {}", storeName);
        long timeoutMillis = config.get(Config.STALE_RESULT_TIMEOUT_MS);
        ForeachActionDispatcher<String, Integer> dispatcher = new ForeachActionDispatcher<>();
        WaitForResultService serviceImpl = new WaitForResultService(timeoutMillis, dispatcher);
        closeables.add(serviceImpl);

        AtomicBoolean done = new AtomicBoolean(false); // no need for dup complete
        CompletableFuture<AsyncBiFunctionService.WithSerdes<String, String, Integer>> cf = new CompletableFuture<>();
        KafkaStreams.StateListener listener = (newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING && !done.getAndSet(true)) {
                cf.completeAsync(() -> serviceImpl); // complete in a different thread
            }
            if (newState == KafkaStreams.State.ERROR) {
                cf.completeExceptionally(new IllegalStateException("KafkaStreams error"));
            }
        };

        Topology topology = new TopicStoreTopologyProvider(storeTopic, storeName, kafkaProperties, dispatcher).get();
        streams = new KafkaStreams(topology, kafkaProperties);
        streams.setStateListener(listener);
        streams.setGlobalStateRestoreListener(new LoggingStateRestoreListener());
        closeables.add(streams);
        streams.start();

        return cf;
    }

    private CompletionStage<Void> createNewStoreTopic(String storeTopic, Admin admin, Context c) {
        log.info("Creating new store topic: {}", storeTopic);
        int rf = Math.min(3, c.clusterSize);
        int minISR = Math.max(rf - 1, 1);
        NewTopic newTopic = new NewTopic(storeTopic, 1, (short) rf)
            .configs(Collections.singletonMap(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(minISR)));
        return toCS(admin.createTopics(Collections.singleton(newTopic)).all());
    }

    private CompletionStage<Void> validateExistingStoreTopic(String storeTopic, Admin admin, Context c) {
        log.info("Validating existing store topic: {}", storeTopic);
        ConfigResource storeTopicConfigResource = new ConfigResource(ConfigResource.Type.TOPIC, storeTopic);
        return toCS(admin.describeTopics(Collections.singleton(storeTopic)).values().get(storeTopic))
            .thenApply(td -> c.setRf(td.partitions().stream().map(tp -> tp.replicas().size()).min(Integer::compare).orElseThrow()))
            .thenCompose(c2 -> toCS(admin.describeConfigs(Collections.singleton(storeTopicConfigResource)).values().get(storeTopicConfigResource))
                    .thenApply(cr -> c2.setMinISR(parseInt(cr.get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).value()))))
            .thenApply(c3 -> {
                if (c3.rf != Math.min(3, c3.clusterSize) || c3.minISR != c3.rf - 1) {
                    log.warn("Durability of the topic [{}] is not sufficient for production use - replicationFactor: {}, {}: {}. " +
                            "Increase the replication factor to at least 3 and configure the {} to {}.",
                            storeTopic, c3.rf, TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, c3.minISR, TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, c3.minISR);
                }
                return null;
            });
    }

    public void stop() {
        log.info("Stopping services ...");
        Collections.reverse(closeables);
        closeables.forEach(KafkaStreamsTopicStoreService::close);
    }

    private static void close(AutoCloseable service) {
        try {
            service.close();
        } catch (Exception e) {
            log.warn("Exception while closing service: {}", service, e);
        }
    }

    private static <T> CompletionStage<T> toCS(KafkaFuture<T> kf) {
        CompletableFuture<T> cf = new CompletableFuture<>();
        kf.whenComplete((v, t) -> {
            if (t != null) {
                cf.completeExceptionally(t);
            } else {
                cf.complete(v);
            }
        });
        return cf;
    }

    static class Context {
        int clusterSize;
        Set<String> topics = Collections.emptySet(); // to make spotbugs happy
        int rf;
        int minISR;

        public Context(int clusterSize) {
            this.clusterSize = clusterSize;
        }

        public Context setTopics(Set<String> topics) {
            this.topics = topics;
            return this;
        }

        public Context setRf(int rf) {
            this.rf = rf;
            return this;
        }

        public Context setMinISR(int minISR) {
            this.minISR = minISR;
            return this;
        }
    }

}
