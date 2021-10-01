/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import io.apicurio.registry.utils.ConcurrentUtil;
import io.strimzi.operator.topic.zk.Zk;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class TopicStoreUpgradeTest {
    private static final Map<String, String> MANDATORY_CONFIG;
    private static EmbeddedKafkaCluster cluster;

    static {
        MANDATORY_CONFIG = new HashMap<>();
        MANDATORY_CONFIG.put(Config.NAMESPACE.key, "default");
        MANDATORY_CONFIG.put(Config.TC_TOPICS_PATH, "/strimzi/topics");
        MANDATORY_CONFIG.put(Config.TC_STALE_RESULT_TIMEOUT_MS, "5000"); //5sec
    }

    private static Vertx vertx;

    private static <T> T result(Future<T> future) throws Throwable {
        future = future.onComplete(ar -> {
            if (ar.cause() != null) {
                System.err.println("Error in future: " + ar.cause());
            }
        });
        if (future.failed()) {
            throw future.cause();
        } else {
            return future.result();
        }
    }

    @Test
    public void testUpgrade(VertxTestContext context) throws Throwable {
        String zkConnectionString = MANDATORY_CONFIG.get(Config.ZOOKEEPER_CONNECT.key);

        CompletableFuture<Void> flag = new CompletableFuture<>();
        Zk.create(vertx, zkConnectionString, 60_000, 10_000, ar -> {
            try {
                if (ar.failed()) {
                    flag.completeExceptionally(ar.cause());
                } else {
                    Zk zk = ar.result();
                    doTestUpgrade(zk);
                }
            } catch (Throwable t) {
                flag.completeExceptionally(t);
            } finally {
                flag.complete(null);
            }
        });
        flag.join();

        Zk zk = Zk.createSync(vertx, zkConnectionString, 60_000, 10_000);
        try {
            doTestUpgrade(zk);
        } finally {
            Checkpoint async = context.checkpoint();
            zk.disconnect(ar -> async.flag());
        }
    }

    private void doTestUpgrade(Zk zk) throws Throwable {
        Config config = new Config(MANDATORY_CONFIG);

        String topicsPath = config.get(Config.TOPICS_PATH);
        TopicStore zkTS = new ZkTopicStore(zk, topicsPath);
        String tn1 = "Topic1" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
        Topic t1 = new Topic.Builder(tn1, 1).build();
        result(zkTS.create(t1));
        String tn2 = "Topic2" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
        Topic t2 = new Topic.Builder(tn2, 1).build();
        result(zkTS.create(t2));

        Properties kafkaProperties = new Properties();
        kafkaProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.get(Config.KAFKA_BOOTSTRAP_SERVERS));
        kafkaProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, config.get(Config.APPLICATION_ID));

        ConcurrentUtil.result(Zk2KafkaStreams.upgrade(zk, config, kafkaProperties, true));

        KafkaStreamsTopicStoreService service = new KafkaStreamsTopicStoreService();
        try {
            TopicStore kTS = ConcurrentUtil.result(service.start(config, kafkaProperties));

            Topic topic1 = result(kTS.read(new TopicName(tn1)));
            Assertions.assertNotNull(topic1);
            Topic topic2 = result(kTS.read(new TopicName(tn2)));
            Assertions.assertNotNull(topic2);
        } finally {
            service.stop();
        }
    }

    @BeforeAll
    public static void before() throws Exception {
        vertx = Vertx.vertx();

        cluster = new EmbeddedKafkaCluster(1);
        cluster.start();

        MANDATORY_CONFIG.put(Config.KAFKA_BOOTSTRAP_SERVERS.key, cluster.bootstrapServers());
        MANDATORY_CONFIG.put(Config.ZOOKEEPER_CONNECT.key, cluster.zKConnectString());

        ZkClient zkc = new ZkClient(cluster.zKConnectString());
        try {
            zkc.create("/strimzi", null, CreateMode.PERSISTENT);
            zkc.create("/strimzi/topics", null, CreateMode.PERSISTENT);
        } finally {
            zkc.close();
        }
    }

    @AfterAll
    public static void after() {
        cluster.stop();
        vertx.close();
    }
}
