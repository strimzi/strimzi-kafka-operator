/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.apicurio.registry.utils.ConcurrentUtil;
import io.strimzi.operator.topic.zk.Zk;
import io.strimzi.test.container.StrimziKafkaCluster;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(VertxExtension.class)
public class TopicStoreUpgradeIT {

    private Map<String, String> mandatoryConfig;
    private StrimziKafkaCluster cluster;
    private WorkerExecutor worker;
    private Zk zk;
    private KafkaStreamsTopicStoreService service;

    @BeforeEach
    public void before(Vertx vertx) {
        cluster = new StrimziKafkaCluster(1, 1, Map.of("zookeeper.connect", "zookeeper:2181"));
        cluster.start();

        String zkConn = cluster.getZookeeper().getHost() + ":" + cluster.getZookeeper().getFirstMappedPort();

        mandatoryConfig = new HashMap<>();
        mandatoryConfig.put(Config.NAMESPACE.key, "default");
        mandatoryConfig.put(Config.TC_TOPICS_PATH, "/strimzi/topics");
        mandatoryConfig.put(Config.TC_STALE_RESULT_TIMEOUT_MS, "5000"); //5sec
        mandatoryConfig.put(Config.KAFKA_BOOTSTRAP_SERVERS.key, cluster.getBootstrapServers());
        mandatoryConfig.put(Config.ZOOKEEPER_CONNECT.key, zkConn);

        ZkClient zkc = new ZkClient(zkConn);
        try {
            zkc.create("/strimzi", null, CreateMode.PERSISTENT);
            zkc.create("/strimzi/topics", null, CreateMode.PERSISTENT);
        } finally {
            zkc.close();
        }

        worker = vertx.createSharedWorkerExecutor("blocking-test-ops", 2);
        zk = Zk.createSync(vertx, zkConn, 60_000, 10_000);
    }

    @AfterEach
    public void after() {
        service.stop();
        zk.disconnect(AsyncResult::result);
        cluster.stop();
    }

    @Test
    @SuppressWarnings({"checkstyle:Indentation"}) //Suppressing this because what Checkstyle agrees with looks terrible
    public void testUpgrade(VertxTestContext context) throws Throwable {
        Config config = new Config(mandatoryConfig);

        // Create topic info in ZK store
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

        worker.executeBlocking(promise1 -> {
            ConcurrentUtil.result(Zk2KafkaStreams.upgrade(zk, config, kafkaProperties, true));
            promise1.complete();
        })
        .compose(ignored -> worker.executeBlocking((Handler<Promise<TopicStore>>) promise2 -> {
            service = new KafkaStreamsTopicStoreService();
            TopicStore kTS = ConcurrentUtil.result(service.start(config, kafkaProperties));
            promise2.complete(kTS);
        }))
        .onComplete(context.succeeding(kTS -> context.verify(() -> {
            Topic topic1 = result(kTS.read(new TopicName(tn1)));
            assertNotNull(topic1);
            Topic topic2 = result(kTS.read(new TopicName(tn2)));
            assertNotNull(topic2);
            context.completeNow();
        })));

        // The Kafka Streams store can start up quite slowly when there's an existing changelog topic for the store.
        // Which there is as the Zk2KafkaStreams.upgrade call creates it, so we need to allow more time for the second
        // creation of the store. This slow start-up will be fixed in a future PR.
        context.awaitCompletion(60, TimeUnit.SECONDS);
    }

    private <T> T result(Future<T> future) throws Throwable {
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
}
