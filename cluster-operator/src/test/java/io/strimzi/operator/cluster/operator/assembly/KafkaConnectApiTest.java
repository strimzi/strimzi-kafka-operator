/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.OrderedProperties;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.IsolatedTest;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.connect.cli.ConnectDistributed;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(VertxExtension.class)
public class KafkaConnectApiTest {
    private static EmbeddedKafkaCluster cluster;
    private static Vertx vertx;
    private Connect connect;
    private static final int PORT = 18083;

    @BeforeEach
    public void beforeEach() throws IOException, InterruptedException {
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
        workerProps.put("bootstrap.servers", cluster.bootstrapServers());

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
    }

    @BeforeAll
    public static void before() throws IOException {
        vertx = Vertx.vertx();

        cluster = new EmbeddedKafkaCluster(3);
        cluster.start();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    @IsolatedTest
    @SuppressWarnings({"unchecked", "checkstyle:MethodLength", "checkstyle:NPathComplexity"})
    public void test(VertxTestContext context) {
        KafkaConnectApi client = new KafkaConnectApiImpl(vertx);
        client.listConnectorPlugins(Reconciliation.DUMMY_RECONCILIATION, "localhost", PORT)
            .onComplete(context.succeeding(connectorPlugins -> context.verify(() -> {
                assertThat(connectorPlugins.size(), greaterThanOrEqualTo(2));

                ConnectorPlugin fileSink = connectorPlugins.stream()
                        .filter(connector -> "org.apache.kafka.connect.file.FileStreamSinkConnector".equals(connector.getConnectorClass()))
                        .findFirst().orElse(null);
                assertNotNull(fileSink);
                assertThat(fileSink.getType(), is("sink"));
                assertThat(fileSink.getVersion(), is(not(emptyString())));

                ConnectorPlugin fileSource = connectorPlugins.stream().filter(connector -> "org.apache.kafka.connect.file.FileStreamSourceConnector".equals(connector.getConnectorClass())).findFirst().orElse(null);
                assertNotNull(fileSource);
                assertThat(fileSource.getType(), is("source"));
                assertThat(fileSource.getVersion(), is(not(emptyString())));
            })))

            .compose(connectorPlugins -> client.list("localhost", PORT))
            .onComplete(context.succeeding(connectorNames -> context.verify(() -> assertThat(connectorNames, is(empty())))))

            .compose(connectorNames -> {
                JsonObject o = new JsonObject()
                    .put("connector.class", "FileStreamSource")
                    .put("tasks.max", "1")
                    .put("file", "/dev/null")
                    .put("topic", "my-topic");
                return client.createOrUpdatePutRequest(Reconciliation.DUMMY_RECONCILIATION, "localhost", PORT, "test", o);
            })
            .compose(created -> {
                Promise<Map<String, Object>> promise = Promise.promise();

                Handler<Long> handler = new Handler<>() {
                    @Override
                    public void handle(Long timerId) {
                        client.status(Reconciliation.DUMMY_RECONCILIATION, "localhost", PORT, "test").onComplete(result -> {
                            if (result.succeeded()) {
                                Map<String, Object> status = result.result();
                                if ("RUNNING".equals(((Map<String, String>) status.getOrDefault("connector", Map.of())).get("state"))) {
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
            .onComplete(context.succeeding(status -> context.verify(() -> {
                assertThat(status.get("name"), is("test"));
                Map<String, Object> connectorStatus = (Map<String, Object>) status.getOrDefault("connector", emptyMap());
                assertThat(connectorStatus.get("state"), is("RUNNING"));
                assertThat(connectorStatus.get("worker_id"), is("localhost:18083"));

                System.out.println("help " + connectorStatus);
                List<Map<String, String>> tasks = (List<Map<String, String>>) status.get("tasks");
                for (Map<String, String> an : tasks) {
                    assertThat(an.get("state"), is("RUNNING"));
                    assertThat(an.get("worker_id"), is("localhost:18083"));
                }
            })))
            .compose(status -> client.getConnectorConfig(Reconciliation.DUMMY_RECONCILIATION, new BackOff(10), "localhost", PORT, "test"))
            .onComplete(context.succeeding(config -> context.verify(() ->
                    assertThat(config, is(TestUtils.map("connector.class", "FileStreamSource",
                    "file", "/dev/null",
                    "tasks.max", "1",
                    "name", "test",
                    "topic", "my-topic"))))))
            .compose(config -> client.getConnectorConfig(Reconciliation.DUMMY_RECONCILIATION, new BackOff(10), "localhost", PORT, "does-not-exist"))
            .onComplete(context.failing(error -> context.verify(() -> {
                assertThat(error, instanceOf(ConnectRestException.class));
                assertThat(((ConnectRestException) error).getStatusCode(), is(404));
            })))
            .recover(error -> Future.succeededFuture())

            .compose(ignored -> client.pause("localhost", PORT, "test"))
            .compose(ignored -> client.resume("localhost", PORT, "test"))
            .compose(ignored -> client.restart("localhost", PORT, "test"))
            .compose(ignored -> client.restartTask("localhost", PORT, "test", 0))
            .compose(ignored -> {
                JsonObject o = new JsonObject()
                        .put("connector.class", "ThisConnectorDoesNotExist")
                        .put("tasks.max", "1")
                        .put("file", "/dev/null")
                        .put("topic", "my-topic");
                return client.createOrUpdatePutRequest(Reconciliation.DUMMY_RECONCILIATION, "localhost", PORT, "broken", o);
            })
            .onComplete(context.failing(error -> context.verify(() -> {
                assertThat(error, instanceOf(ConnectRestException.class));

                assertThat(error.getMessage(),
                        containsString("Failed to find any class that implements Connector and which name matches ThisConnectorDoesNotExist"));
            })))
            .recover(e -> Future.succeededFuture())
            .compose(ignored -> {
                JsonObject o = new JsonObject()
                        .put("connector.class", "FileStreamSource")
                        .put("tasks.max", "dog")
                        .put("file", "/dev/null")
                        .put("topic", "my-topic");
                return client.createOrUpdatePutRequest(Reconciliation.DUMMY_RECONCILIATION, "localhost", PORT, "broken2", o);
            })
            .onComplete(context.failing(error -> context.verify(() -> {
                assertThat(error, instanceOf(ConnectRestException.class));
                assertThat(error.getMessage(),
                        containsString("Invalid value dog for configuration tasks.max: Not a number of type INT"));
            })))
            .recover(e -> Future.succeededFuture())
            .compose(createResponse -> client.list("localhost", PORT))
            .onComplete(context.succeeding(connectorNames -> context.verify(() ->
                    assertThat(connectorNames, is(singletonList("test"))))))
            .compose(connectorNames -> client.delete(Reconciliation.DUMMY_RECONCILIATION, "localhost", PORT, "test"))
            .compose(deletedConnector -> client.list("localhost", PORT))
            .onComplete(context.succeeding(connectorNames -> assertThat(connectorNames, is(empty()))))
            .compose(connectorNames -> client.delete(Reconciliation.DUMMY_RECONCILIATION, "localhost", PORT, "never-existed"))
            .onComplete(context.failing(error -> {
                assertThat(error, instanceOf(ConnectRestException.class));
                assertThat(error.getMessage(),
                        containsString("Connector never-existed not found"));
                context.completeNow();
            }));
    }

    @IsolatedTest
    public void testChangeLoggers(VertxTestContext context) {
        String desired = "log4j.rootLogger=TRACE, CONSOLE\n" +
                "log4j.logger.org.apache.zookeeper=WARN\n" +
                "log4j.logger.org.I0Itec.zkclient=INFO\n" +
                "log4j.logger.org.reflections.Reflection=INFO\n" +
                "log4j.logger.org.reflections=FATAL\n" +
                "log4j.logger.foo=WARN\n" +
                "log4j.logger.foo.bar=TRACE\n" +
                "log4j.logger.foo.bar.quux=DEBUG";

        KafkaConnectApi client = new KafkaConnectApiImpl(vertx);

        OrderedProperties ops = new OrderedProperties();
        ops.addStringPairs(desired);

        client.updateConnectLoggers(Reconciliation.DUMMY_RECONCILIATION, "localhost", PORT, desired, ops)
                .onComplete(context.succeeding(a ->
                        client.listConnectLoggers(Reconciliation.DUMMY_RECONCILIATION, "localhost", PORT)
                        .onComplete(context.succeeding(map -> context.verify(() -> {
                            assertThat(map.get("org.apache.zookeeper"), is("WARN"));
                            assertThat(map.get("org.I0Itec.zkclient"), is("INFO"));
                            assertThat(map.get("org.reflections"), is("FATAL"));
                            assertThat(map.get("org.reflections.Reflection"), is("INFO"));
                            assertThat(map.get("foo"), is("WARN"));
                            assertThat(map.get("foo.bar"), is("TRACE"));
                            assertThat(map.get("foo.bar.quux"), is("DEBUG"));
                            context.completeNow();
                        })))));
    }
}
