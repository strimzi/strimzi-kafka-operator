package io.strimzi.operator.topic.stores;

import io.micrometer.core.instrument.util.NamedThreadFactory;
import io.strimzi.operator.topic.Topic;
import io.strimzi.operator.topic.TopicCommand;
import io.strimzi.operator.topic.TopicCommandSerde;
import io.strimzi.operator.topic.TopicSerde;
import io.strimzi.operator.topic.TopicStoreTopologyProvider.TopicCommandTransformer;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@SuppressWarnings("resource")
public class SelfManagedPersistentStore {

    private final Admin adminClient;
    private final String selfManagedTopicName;
    private final Map<String, Object> baseClientProperties;
    private final ForeachAction<? super String, ? super Integer> dispatcher;
    private final String inputTopic;
    private final Promise<Void> catchUpComplete;
    private final ExecutorService executor;
    AtomicBoolean shutdown = new AtomicBoolean(false);
    private SelfManagedKeyValueStore keyValueStore;
    private boolean storeTopicWasCreated;
    private List<TopicPartition> inputPartitions;

    private static final Duration pollDuration  = Duration.ofMillis(500);

    private static final Logger log = LogManager.getLogger(SelfManagedPersistentStore.class);

    public SelfManagedPersistentStore(String inputTopic, String selfManagedTopicName, Properties baseClientProperties, ForeachAction<? super String, ? super Integer> dispatcher) {
        this.adminClient = Admin.create(baseClientProperties);
        this.selfManagedTopicName = selfManagedTopicName;
        this.baseClientProperties = baseClientProperties.entrySet().stream().collect(Collectors.toUnmodifiableMap(es -> es.getKey().toString(), Map.Entry::getValue));
        this.dispatcher = dispatcher;
        this.inputTopic = inputTopic;
        catchUpComplete = Promise.promise();
        this.executor = Executors.newSingleThreadExecutor(new NamedThreadFactory("to-bla"));
    }

    private void consume() {
        log.info("Starting consume");
        Map<String, Object> inputConsumerProps = new HashMap<>(baseClientProperties);
        inputConsumerProps.put("group.id", "topic-operator-input-consumer");
        Consumer<String, TopicCommand> inputConsumer = new KafkaConsumer<>(inputConsumerProps,
                new StringDeserializer(),
                new TopicCommandSerde().deserializer());
        inputConsumer.assign(inputPartitions);

        log.info("consumerSubscribed");
        if (storeTopicWasCreated) {
            log.info("Seeking to start of topic");
            inputConsumer.seekToBeginning(inputPartitions);
            log.info("Seeking done");
        }

        TopicCommandTransformer topicCommandTransformer = new TopicCommandTransformer(inputTopic, dispatcher).withStore(keyValueStore);
        log.info("Consume loop next");
        while (!shutdown.get()) {
            log.debug("Polling...");

            inputConsumer.poll(pollDuration).forEach(cr -> {
                log.info("Input record: {}, {}, {}, {}", cr.key(), cr.value().getType(), cr.value().getKey(), cr.value().getTopic());
                topicCommandTransformer.process(new Record<>(cr.key(), cr.value(), cr.timestamp()));
            });
            inputConsumer.commitSync();

            if (storeTopicWasCreated && !catchUpComplete.future().isComplete()) {
                log.info("Checking offsets on input topic after first creation of store topic");
                List<Long> longStream = inputPartitions.stream().map(tp -> inputConsumer.currentLag(tp).orElse(-1)).collect(Collectors.toList());
                log.info("Lag: {}", longStream);
                if (longStream.stream().allMatch(lag -> lag == 0)) {
                    log.info("Catch-up complete");
                    catchUpComplete.complete();
                }
            }
        }

        inputConsumer.close();
    }

    public Future<SelfManagedKeyValueStore> start() {
        log.info("Starting SMPS");
        storeTopicWasCreated = createStoreTopicIfNeeded(adminClient, selfManagedTopicName);
        try {
            inputPartitions = adminClient.describeTopics(List.of(inputTopic)).allTopicNames().get().get(inputTopic).partitions().stream().map(tpi -> new TopicPartition(inputTopic, tpi.partition())).collect(Collectors.toList());
            log.info("Input topic partitions: {}", inputPartitions);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        log.info("Creating KVS");
        keyValueStore = new SelfManagedKeyValueStore(selfManagedTopicName, baseClientProperties);

        log.info("KVS completed");
        if (!storeTopicWasCreated) {
            log.info("Completing catchup as topic not existed");
            catchUpComplete.complete();
        } else {
            log.info("Catch up wasn't completed, waiting for consume loop to do so");
        }

        log.info("Submitting consume");
        executor.submit(this::consume);

        log.info("Submitted, returning a future");
        return catchUpComplete.future().compose(i -> {
            log.info("Catch up future complete");
            return Future.succeededFuture(keyValueStore);
        });
    }

    private boolean createStoreTopicIfNeeded(Admin adminClient, String selfManagedTopicName) {
        Map<String, String> configuration = new HashMap<>();
        configuration.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        configuration.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "0");
        configuration.put(TopicConfig.SEGMENT_BYTES_CONFIG, String.valueOf(64 * 1024 * 1024));

        try {
            if (adminClient.listTopics().names().get().contains(selfManagedTopicName)) {
                log.info("Topic {} exists, not creating", selfManagedTopicName);
                return false;
            }
            NewTopic makeTopic = new NewTopic(selfManagedTopicName, -1, (short) -1).configs(configuration);
            adminClient.createTopics(List.of(makeTopic)).all().get();
            log.info("Topic {} created", selfManagedTopicName);

        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    @SuppressWarnings("resource")
    public static class SelfManagedKeyValueStore implements KeyValueStore<String, Topic> {

        ConcurrentHashMap<String, Topic> cachedTopics;
        Producer<String, Topic> storeProducer;
        private final String storeTopic;

        private static final Logger log = LogManager.getLogger(SelfManagedPersistentStore.class);

        public SelfManagedKeyValueStore(String storeTopic, Map<String, Object> kafkaClientProperties) {
            if (kafkaClientProperties == null) {
                throw new RuntimeException("WTF");
            }
            cachedTopics = loadStore(storeTopic, kafkaClientProperties);
            this.storeTopic = storeTopic;
            Serializer<Topic> serializer = new TopicSerde().serializer();
            Serializer<Topic> passThroughSerialzier = (topic, data) -> {
                if (data == null) {
                    return null;
                }
                return serializer.serialize(topic, data);
            };
            storeProducer = new KafkaProducer<>(kafkaClientProperties, new StringSerializer(), passThroughSerialzier);
        }

        private ConcurrentHashMap<String, Topic> loadStore(String storeTopic, Map<String, Object> kafkaClientProperties) {
            log.info("Loading store topic into cache");
            Map<String, Object> initialStoreConsumerProps = new HashMap<>(kafkaClientProperties);
            initialStoreConsumerProps.put("group.id", "topic-operator-initial-store-consumer");
            Consumer<String, Topic> initialConsumer = new KafkaConsumer<>(initialStoreConsumerProps, new StringDeserializer(), new TopicSerde().deserializer());
            List<TopicPartition> tps = initialConsumer.partitionsFor(storeTopic).stream().map(pi -> new TopicPartition(storeTopic, pi.partition())).collect(Collectors.toList());
            initialConsumer.assign(tps);
            initialConsumer.seekToBeginning(tps);
            log.info("TPs for store topic: {}", tps);
            ConcurrentHashMap<String, Topic> cache = new ConcurrentHashMap<>();
            boolean caughtUp = false;
            while (!caughtUp) {
                List<Long> longStream = tps.stream().map(tp -> initialConsumer.currentLag(tp).orElse(-1)).collect(Collectors.toList());
                log.info("Store topic lags {}", longStream);
                initialConsumer.poll(pollDuration).forEach(cr -> cache.put(cr.key(), cr.value()));
                initialConsumer.commitSync();
                longStream = tps.stream().map(tp -> initialConsumer.currentLag(tp).orElse(-1)).collect(Collectors.toList());
                log.info("Store topic lags {}", longStream);
                caughtUp = longStream.stream().allMatch(lag -> lag == 0);
            }

            return cache;
        }

        @Override
        public void put(String key, Topic value) {
            writeToStoreTopic(key, value);
            cachedTopics.put(key, value);
        }

        @Override
        public Topic putIfAbsent(String key, Topic value) {
            log.info("k: {}, v: {}", key, value);
            if (cachedTopics.containsKey(key)) {
                return cachedTopics.get(key);
            } else {
                put(key, value);
                return null;
            }
        }

        @Override
        public void putAll(List<KeyValue<String, Topic>> entries) {

        }

        @Override
        public Topic delete(String key) {
            Topic removedTopic = cachedTopics.remove(key);
            if (removedTopic != null) {
                writeToStoreTopic(key, null);
            }
            return removedTopic;
        }

        @Override
        public String name() {
            return null;
        }

        @Override
        public void init(ProcessorContext context, StateStore root) {

        }

        @Override
        public void flush() {

        }

        @Override
        public void close() {

        }

        @Override
        public boolean persistent() {
            return false;
        }

        @Override
        public boolean isOpen() {
            return false;
        }

        @Override
        public Topic get(String key) {
            return cachedTopics.get(key);
        }

        @Override
        public KeyValueIterator<String, Topic> range(String from, String to) {
            return null;
        }

        @Override
        public KeyValueIterator<String, Topic> all() {
            return null;
        }

        @Override
        public long approximateNumEntries() {
            return 0;
        }


        private void writeToStoreTopic(String key, Topic value) {
            try {
                storeProducer.send(new ProducerRecord<>(storeTopic, key, value)).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
