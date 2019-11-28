/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.debezium.kafka.KafkaCluster;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.connect.cli.ConnectDistributed;
import org.apache.kafka.connect.runtime.Connect;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(VertxExtension.class)
public class KafkaConnectApiTest {

    private KafkaCluster cluster;
    private Vertx vertx;
    private Connect connect;
    private static final int PORT = 18083;

    @BeforeEach
    public void before() throws IOException, InterruptedException {
        vertx = Vertx.vertx();
        cluster = new KafkaCluster();
        cluster.addBrokers(3);
        cluster.deleteDataPriorToStartup(true);
        cluster.deleteDataUponShutdown(true);
        cluster.usingDirectory(Files.createTempDirectory("operator-integration-test").toFile());
        //cluster.withKafkaConfiguration(kafkaClusterConfig());
        cluster.startup();
        cluster.createTopics(getClass().getSimpleName() + "-offsets", getClass().getSimpleName() + "-config", getClass().getSimpleName() + "-status");
        // Somehow start connect distributed. Or can I just fire up the webserver hooked into some mock stuff?
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("listeners", "http://localhost:" + PORT);
        File tempDirectory = Files.createTempDirectory(getClass().getSimpleName()).toFile();
        workerProps.put("plugin.path", tempDirectory.toString());
        workerProps.put("group.id", toString());
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("key.converter.schemas.enable", "false");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter.schemas.enable", "false");
        workerProps.put("offset.storage.topic", getClass().getSimpleName() + "-offsets");
        workerProps.put("config.storage.topic", getClass().getSimpleName() + "-config");
        workerProps.put("status.storage.topic", getClass().getSimpleName() + "-status");
        workerProps.put("bootstrap.servers", cluster.brokerList());
        //DistributedConfig config = new DistributedConfig(workerProps);
        //RestServer rest = new RestServer(config);
        //rest.initializeServer();
        CountDownLatch l = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            ConnectDistributed connectDistributed = new ConnectDistributed();
            connect = connectDistributed.startConnect(workerProps);
            l.countDown();
            connect.awaitStop();
        });
        thread.setDaemon(false);
        thread.start();
        l.await();
    }

    @AfterEach
    public void after() {
        if (connect != null) {
            connect.stop();
            connect.awaitStop();
        }
        cluster.shutdown();
    }

    @Test
    public void test(VertxTestContext context) throws InterruptedException {
        KafkaConnectApi client = new KafkaConnectApiImpl(vertx);
        CountDownLatch async = new CountDownLatch(1);
        client.list("localhost", PORT)
            .compose(connectorNames -> {
                assertEquals(emptyList(), connectorNames);
                JsonObject o = new JsonObject()
                    .put("connector.class", "FileStreamSource")
                    .put("tasks.max", "1")
                    .put("file", "/dev/null")
                    .put("topic", "my-topic");
                return client.createOrUpdatePutRequest("localhost", PORT, "test", o);
            })
            .compose(created -> {
                JsonObject o = new JsonObject()
                    .put("connector.class", "ThisConnectorDoesNotExist")
                    .put("tasks.max", "1")
                    .put("file", "/dev/null")
                    .put("topic", "my-topic");
                return client.createOrUpdatePutRequest("localhost", PORT, "broken", o)
                    .compose(ignored -> Future.failedFuture(new AssertionError("Should fail")))
                    .recover(e -> {
                        if (e instanceof ConnectRestException) {
                            if (e.getMessage().contains("Failed to find any class that implements Connector and which name matches ThisConnectorDoesNotExist")) {
                                return Future.succeededFuture();
                            } else {
                                return Future.failedFuture(e.getMessage());
                            }
                        } else {
                            return Future.failedFuture(e);
                        }
                    });
            }).compose(created -> {
                JsonObject o = new JsonObject()
                    .put("connector.class", "FileStreamSource")
                    .put("tasks.max", "dog")
                    .put("file", "/dev/null")
                    .put("topic", "my-topic");
                return client.createOrUpdatePutRequest("localhost", PORT, "broken2", o)
                    .compose(ignored -> Future.failedFuture(new AssertionError("Should fail")))
                    .recover(e -> {
                        if (e instanceof ConnectRestException) {
                            if (e.getMessage().contains("Invalid value dog for configuration tasks.max: Not a number of type INT")) {
                                return Future.succeededFuture();
                            } else {
                                return Future.failedFuture(e.getMessage());
                            }
                        } else {
                            return Future.failedFuture(e);
                        }
                    });
            }).compose(createResponse -> {
                return client.list("localhost", PORT);
            }).compose(connectorNames -> {
                assertEquals(singletonList("test"), connectorNames);
                return client.delete("localhost", PORT, "test");
            }).compose(deleteResponse -> {
                return client.list("localhost", PORT);
            }).compose(connectorNames -> {
                assertEquals(emptyList(), connectorNames);
                return client.delete("localhost", PORT, "never-existed")
                    .compose(ignored -> Future.failedFuture(new AssertionError("Should fail")))
                    .recover(e -> {
                        if (e instanceof ConnectRestException) {
                            if (e.getMessage().contains("Connector never-existed not found")) {
                                return Future.succeededFuture();
                            } else {
                                return Future.failedFuture(e.getMessage());
                            }
                        } else {
                            return Future.failedFuture(e);
                        }
                    });
            }).compose(ignored -> {
                async.countDown();
                return Future.succeededFuture();
            }).recover(e -> {
                context.failNow(e);
                async.countDown();
                return Future.succeededFuture();
            });
        async.await(30, TimeUnit.SECONDS);
        context.completeNow();
    }
}
