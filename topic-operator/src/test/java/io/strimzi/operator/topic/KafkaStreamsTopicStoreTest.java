/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.debezium.kafka.KafkaCluster;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class KafkaStreamsTopicStoreTest extends TopicStoreTestBase {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsTopicStoreTest.class);
    private static final Map<String, String> MANDATORY_CONFIG;

    static {
        MANDATORY_CONFIG = new HashMap<>();
        MANDATORY_CONFIG.put(Config.NAMESPACE.key, "default");
    }

    private static KafkaCluster kafkaCluster;
    private static KafkaStreamsTopicStoreService service;

    static KafkaCluster prepareKafkaCluster(Map<String, String> config) throws Exception {
        kafkaCluster = new KafkaCluster();
        kafkaCluster.addBrokers(1);
        kafkaCluster.deleteDataPriorToStartup(true);
        kafkaCluster.deleteDataUponShutdown(true);
        kafkaCluster.usingDirectory(Files.createTempDirectory("kafka_ts").toFile());
        kafkaCluster.withKafkaConfiguration(new Properties());
        kafkaCluster.startup();

        String brokerList = kafkaCluster.brokerList();
        log.info("Starting Kafka cluster, broker list: " + brokerList);
        config.put(Config.KAFKA_BOOTSTRAP_SERVERS.key, brokerList);
        config.put(Config.ZOOKEEPER_CONNECT.key, "localhost:" + kafkaCluster.zkPort());

        return kafkaCluster;
    }

    static void shutdownKafkaCluster() {
        if (kafkaCluster != null) {
            kafkaCluster.shutdown();
        }
    }

    @Override
    protected boolean canRunTest() {
        return true;
    }

    static KafkaStreamsTopicStoreService service(Map<String, String> configMap) throws Exception {
        Map<String, String> mergedMap = new HashMap<>(MANDATORY_CONFIG);
        mergedMap.putAll(configMap);
        Config config = new Config(mergedMap);

        Properties kafkaProperties = new Properties();
        kafkaProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.get(Config.KAFKA_BOOTSTRAP_SERVERS));
        kafkaProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, config.get(Config.APPLICATION_ID));
        kafkaProperties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, config.get(Config.APPLICATION_SERVER));

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
        prepareKafkaCluster(MANDATORY_CONFIG);
        service = service(Collections.emptyMap());
    }

    @AfterAll
    public static void after() {
        if (service != null) {
            service.stop();
        }
        shutdownKafkaCluster();
    }

    @BeforeEach
    public void setup() {
        if (service != null) {
            this.store = service.store;
        }
    }

}
