/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.debezium.kafka.KafkaCluster;
import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.operator.common.BackOff;
import io.strimzi.test.TestUtils;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.connect.cli.ConnectDistributed;
import org.apache.kafka.connect.runtime.Connect;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(VertxExtension.class)
public class KafkaConnectApiTest {

    private KafkaCluster cluster;
    private static Vertx vertx;
    private Connect connect;
    private static final int PORT = 18083;

    @BeforeEach
    public void beforeEach() throws IOException, InterruptedException {
        // Start a 3 node Kafka cluster
        cluster = new KafkaCluster();
        cluster.addBrokers(3);
        cluster.deleteDataPriorToStartup(true);
        cluster.deleteDataUponShutdown(true);
        cluster.usingDirectory(Files.createTempDirectory("operator-integration-test").toFile());
        cluster.startup();
        cluster.createTopics(getClass().getSimpleName() + "-offsets", getClass().getSimpleName() + "-config", getClass().getSimpleName() + "-status");

        // Start a N node connect cluster
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
    public void afterEach() {
        if (connect != null) {
            connect.stop();
            connect.awaitStop();
        }
        cluster.shutdown();
    }

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    @Test
    @SuppressWarnings({"unchecked", "checkstyle:MethodLength", "checkstyle:NPathComplexity"})
    public void test(VertxTestContext context) throws InterruptedException {
        KafkaConnectApi client = new KafkaConnectApiImpl(vertx);
        CountDownLatch async = new CountDownLatch(1);
        client.listConnectorPlugins("localhost", PORT)
            .compose(connectorPlugins -> {
                assertThat(connectorPlugins.size(), greaterThanOrEqualTo(2));

                ConnectorPlugin fileSink = connectorPlugins.stream().filter(connector -> "org.apache.kafka.connect.file.FileStreamSinkConnector".equals(connector.getConnectorClass())).findFirst().orElse(null);
                assertNotNull(fileSink);
                assertEquals(fileSink.getType(), "sink");
                assertNotNull(fileSink.getVersion());
                assertFalse(fileSink.getVersion().isEmpty());

                ConnectorPlugin fileSource = connectorPlugins.stream().filter(connector -> "org.apache.kafka.connect.file.FileStreamSourceConnector".equals(connector.getConnectorClass())).findFirst().orElse(null);
                assertNotNull(fileSource);
                assertEquals(fileSource.getType(), "source");
                assertNotNull(fileSource.getVersion());
                assertFalse(fileSource.getVersion().isEmpty());

                return client.list("localhost", PORT);
            })
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
                Promise<Map<String, Object>> promise = Promise.promise();
                Handler<Long> handler = new Handler<Long>() {
                    @Override
                    public void handle(Long timerId) {
                        client.status("localhost", PORT, "test").setHandler(result -> {
                            if (result.succeeded()) {
                                Map<String, Object> status = result.result();
                                if ("RUNNING".equals(((Map) status.getOrDefault("connector", emptyMap())).get("state"))) {
                                    promise.complete(status);
                                    return;
                                } else {
                                    System.err.println(status);
                                }
                            } else {
                                result.cause().printStackTrace();
                            }
                            vertx.setTimer(1000, this);
                        });
                    }
                };
                vertx.setTimer(1000, handler);
                return promise.future();
            })
            .compose(status -> {
                try {
                    assertEquals("test", status.get("name"));
                    assertEquals("RUNNING", ((Map) status.getOrDefault("connector", emptyMap())).get("state"));
                    assertEquals("localhost:18083", ((Map) status.getOrDefault("connector", emptyMap())).get("worker_id"));
                    List<Map> tasks = (List<Map>) status.get("tasks");
                    for (Map an : tasks) {
                        assertEquals("RUNNING", an.get("state"));
                        assertEquals("localhost:18083", an.get("worker_id"));
                    }
                    return client.getConnectorConfig(new BackOff(10), "localhost", PORT, "test");
                } catch (Throwable e) {
                    return Future.failedFuture(e);
                }
            })
            .compose(config -> {
                try {
                    assertEquals(TestUtils.map("connector.class", "FileStreamSource",
                            "file", "/dev/null",
                            "tasks.max", "1",
                            "name", "test",
                            "topic", "my-topic"), config);
                    return client.getConnectorConfig(new BackOff(10), "localhost", PORT, "does-not-exist");
                } catch (Throwable e) {
                    return Future.failedFuture(e);
                }
            })
            .recover(error -> {
                try {
                    assertTrue(error instanceof ConnectRestException);
                    assertEquals(404, ((ConnectRestException) error).getStatusCode());
                    return Future.succeededFuture();
                } catch (Throwable e) {
                    return Future.failedFuture(e);
                }
            }).compose(ignored -> {
                return client.pause("localhost", PORT, "test");
            })
            .compose(ignored -> {
                return client.resume("localhost", PORT, "test");
            })
            .compose(ignored1 -> {
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
        if (async.await(60, TimeUnit.SECONDS)) {
            context.completeNow();
        } else {
            context.failNow(new TimeoutException("Timeout!"));
        }
    }
}
