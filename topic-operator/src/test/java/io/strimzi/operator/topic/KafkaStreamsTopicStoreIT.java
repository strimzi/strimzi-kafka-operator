/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.test.container.StrimziKafkaContainer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class KafkaStreamsTopicStoreIT extends TopicStoreTestBase {
    private static final Map<String, String> MANDATORY_CONFIG;
    private static StrimziKafkaContainer kafkaContainer;

    static {
        MANDATORY_CONFIG = new HashMap<>();
        MANDATORY_CONFIG.put(Config.NAMESPACE.key, "default");
    }

    private static KafkaStreamsTopicStoreService service;

    static KafkaStreamsTopicStoreService service(Map<String, String> configMap) throws Exception {
        Map<String, String> mergedMap = new HashMap<>(MANDATORY_CONFIG);
        mergedMap.putAll(configMap);
        Config config = new Config(mergedMap);

        Properties kafkaProperties = new Properties();
        kafkaProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.get(Config.KAFKA_BOOTSTRAP_SERVERS));
        kafkaProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, config.get(Config.APPLICATION_ID));

        String storeTopic = config.get(Config.STORE_TOPIC);

        try (AdminClient adminClient = AdminClient.create(kafkaProperties)) {
            Set<String> topics = adminClient.listTopics().names().get();
            if (!topics.contains(storeTopic)) {
                adminClient.createTopics(Collections.singleton(new NewTopic(storeTopic, 3, (short) 1))).all().get();
            }
        }

        KafkaStreamsTopicStoreService service = new KafkaStreamsTopicStoreService();
        service.start(config, kafkaProperties).toCompletableFuture().get();
        return service;
    }

    @BeforeAll
    public static void before() throws Exception {
        kafkaContainer = new StrimziKafkaContainer()
            .withBrokerId(1);
        kafkaContainer.start();

        MANDATORY_CONFIG.put(Config.KAFKA_BOOTSTRAP_SERVERS.key, kafkaContainer.getBootstrapServers());
        MANDATORY_CONFIG.put(Config.ZOOKEEPER_CONNECT.key, "zookeeper:2181");

        service = service(Collections.emptyMap());
    }

    @AfterAll
    public static void after() {
        if (service != null) {
            service.stop();
        }

        kafkaContainer.stop();
    }

    @BeforeEach
    public void setup() {
        if (service != null) {
            this.store = service.store;
        }
    }

}
