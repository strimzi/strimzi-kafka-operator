/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.stores;

import io.micrometer.core.instrument.util.NamedThreadFactory;
import io.strimzi.operator.topic.Topic;
import io.strimzi.operator.topic.TopicCommand;
import io.strimzi.operator.topic.TopicCommandSerde;
import io.strimzi.operator.topic.TopicSerde;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

@SuppressWarnings("resource")
public class KafkaPersistentStore implements AutoCloseable {

    private final String persistenceTopic;
    private final Map<String, Object> baseClientProperties;
    private final ForeachAction<? super String, ? super Integer> dispatcher;
    private final String commandTopic;
    private final ExecutorService executor;
    private final Function<Map<String, Object>, Consumer<String, TopicCommand>> commandConsumerCreator;
    private final Function<Map<String, Object>, Consumer<String, Topic>> persistenceConsumerCreator;
    private final Function<Map<String, Object>, Producer<String, Topic>> persistenceProducerCreator;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Admin admin;

    private static final Duration COMMAND_POLL_WAIT = Duration.ofMillis(500);

    private static final Logger LOG = LogManager.getLogger(KafkaPersistentStore.class);

    KafkaPersistentStore(String commandTopic,
                         String persistenceTopic,
                         Map<String, Object> baseClientProperties,
                         Function<Map<String, Object>, Admin> adminCreator,
                         Function<Map<String, Object>, Consumer<String, TopicCommand>> commandConsumerCreator,
                         Function<Map<String, Object>, Consumer<String, Topic>> persistenceConsumerCreator,
                         Function<Map<String, Object>, Producer<String, Topic>> persistenceProducerCreator,
                         ForeachAction<? super String, ? super Integer> dispatcher) {
        this.admin = adminCreator.apply(baseClientProperties);

        this.commandTopic = commandTopic;
        this.persistenceTopic = persistenceTopic;
        this.baseClientProperties = baseClientProperties;

        this.commandConsumerCreator = commandConsumerCreator;
        this.persistenceConsumerCreator = persistenceConsumerCreator;
        this.persistenceProducerCreator = persistenceProducerCreator;

        this.dispatcher = dispatcher;
        this.executor = Executors.newSingleThreadExecutor(new NamedThreadFactory("to-bla"));

    }

    public KafkaPersistentStore(String commandTopic, String persistenceTopic, Properties baseClientProperties, ForeachAction<? super String, ? super Integer> dispatcher) {
        this(commandTopic,
                persistenceTopic,
                baseClientProperties.entrySet()
                                    .stream()
                                    .filter(es -> es.getKey() != "application.id") //Remove the streams config bit to avoid warnings in logs
                                    .collect(Collectors.toUnmodifiableMap(es -> es.getKey().toString(), Map.Entry::getValue)),
                Admin::create,
                KafkaPersistentStore::createCommandConsumer,
                KafkaPersistentStore::createPersistenceConsumer,
                KafkaPersistentStore::createPersistenceProducer,
                dispatcher
        );
    }

    public KafkaPersistentKeyValueStore start() {
        createStoreTopicIfNeeded(admin, persistenceTopic);
        KafkaPersistentKeyValueStore keyValueStore = new KafkaPersistentKeyValueStore(persistenceTopic, admin, persistenceConsumerCreator, persistenceProducerCreator, baseClientProperties);
        LOG.info("Beginning command topic poll");
        executor.submit(commandConsumingLoop(keyValueStore));
        return keyValueStore;
    }


    private Runnable commandConsumingLoop(KafkaPersistentKeyValueStore keyValueStore) {
        return () -> {
            Consumer<String, TopicCommand> commandConsumer = commandConsumerCreator.apply(baseClientProperties);
            commandConsumer.subscribe(List.of(commandTopic));
            TopicCommandTransformer topicCommandTransformer = new TopicCommandTransformer(commandTopic, dispatcher).withStore(keyValueStore);
            while (running.get()) {
                LOG.trace("Polling...");

                commandConsumer.poll(COMMAND_POLL_WAIT).forEach(cr -> {
                    LOG.debug("Command received {}: {}", cr.key(), cr.value());
                    topicCommandTransformer.process(new Record<>(cr.key(), cr.value(), cr.timestamp()));
                });
                commandConsumer.commitSync();
            }
            LOG.debug("Polling loop completed");
            commandConsumer.close();
        };
    }

    private void createStoreTopicIfNeeded(Admin adminClient, String selfManagedTopicName) {
        Map<String, String> configuration = new HashMap<>();
        configuration.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        configuration.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "0");
        configuration.put(TopicConfig.SEGMENT_BYTES_CONFIG, String.valueOf(64 * 1024 * 1024));

        try {
            if (adminClient.listTopics().names().get().contains(selfManagedTopicName)) {
                LOG.info("Topic {} exists, not creating", selfManagedTopicName);
                return;
            }
            NewTopic makeTopic = new NewTopic(selfManagedTopicName, -1, (short) -1).configs(configuration);
            adminClient.createTopics(List.of(makeTopic)).all().get();
            LOG.info("Topic {} created", selfManagedTopicName);

        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private static Producer<String, Topic> createPersistenceProducer(Map<String, Object> baseClientProperties) {
        Map<String, Object> producerProps = new HashMap<>(baseClientProperties);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(producerProps, new StringSerializer(), new NullSafeTopicSerializer());
    }

    private static Consumer<String, Topic> createPersistenceConsumer(Map<String, Object> baseClientProperties) {
        Map<String, Object> persistenceConsumerProperties = commonConsumerConf(baseClientProperties);
        persistenceConsumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "topic-operator-persistence-consumer");
        return new KafkaConsumer<>(persistenceConsumerProperties,
                new StringDeserializer(),
                new TopicSerde()
        );
    }

    private static Consumer<String, TopicCommand> createCommandConsumer(Map<String, Object> baseClientProperties) {
        Map<String, Object> commandConsumerProperties = commonConsumerConf(baseClientProperties);
        commandConsumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "topic-operator-input-consumer");
        return new KafkaConsumer<>(commandConsumerProperties,
                new StringDeserializer(),
                new TopicCommandSerde().deserializer()
        );
    }

    private static Map<String, Object> commonConsumerConf(Map<String, Object> baseClientProperties) {
        Map<String, Object> consumerProperties = new HashMap<>(baseClientProperties);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return consumerProperties;
    }

    @Override
    public void close() throws Exception {
        running.set(false);
        executor.shutdown();
    }
}