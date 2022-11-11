/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.OrderedProperties;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.IsolatedTest;
import io.strimzi.test.container.StrimziKafkaCluster;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
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
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(VertxExtension.class)
public class KafkaConnectApiIT {
    private static StrimziKafkaCluster cluster;
    private static Vertx vertx;

    private ConnectCluster connectCluster;
    private int port;

    @BeforeEach
    public void beforeEach() throws InterruptedException {
        // Start a 3 node connect cluster
        connectCluster = new ConnectCluster()
                .usingBrokers(cluster.getBootstrapServers())
                .addConnectNodes(3);
        connectCluster.startup();
        port = connectCluster.getPort(2);
    }

    @AfterEach
    public void afterEach() {
        if (connectCluster != null) {
            connectCluster.shutdown();
        }
    }

    @BeforeAll
    public static void before() throws IOException {
        vertx = Vertx.vertx();
        final Map<String, String> kafkaClusterConfiguration = new HashMap<>();
        kafkaClusterConfiguration.put("zookeeper.connect", "zookeeper:2181");
        cluster = new StrimziKafkaCluster(3, 1, kafkaClusterConfiguration);
        cluster.start();
    }

    @AfterAll
    public static void after() {
        cluster.stop();
        vertx.close();
    }

    @IsolatedTest
    @SuppressWarnings({"unchecked", "checkstyle:MethodLength", "checkstyle:NPathComplexity"})
    public void test(VertxTestContext context) throws InterruptedException {
        KafkaConnectApi client = new KafkaConnectApiImpl(vertx);
        Checkpoint async = context.checkpoint();
        Thread.sleep(10_000L);
        client.listConnectorPlugins(Reconciliation.DUMMY_RECONCILIATION, "localhost", port)
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

            .compose(connectorPlugins -> client.list("localhost", port))
            .onComplete(context.succeeding(connectorNames -> context.verify(() -> assertThat(connectorNames, is(empty())))))

            .compose(connectorNames -> {
                JsonObject o = new JsonObject()
                    .put("connector.class", "FileStreamSource")
                    .put("tasks.max", "1")
                    .put("file", "/dev/null")
                    .put("topic", "my-topic");
                return client.createOrUpdatePutRequest(Reconciliation.DUMMY_RECONCILIATION, "localhost", port, "test", o);
            })
            .onComplete(context.succeedingThenComplete())
            .compose(created -> {

                Promise<Map<String, Object>> promise = Promise.promise();

                Handler<Long> handler = new Handler<>() {
                    @Override
                    public void handle(Long timerId) {
                        client.status(Reconciliation.DUMMY_RECONCILIATION, "localhost", port, "test").onComplete(result -> {
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
            .onComplete(context.succeeding(status -> context.verify(() -> {
                assertThat(status.get("name"), is("test"));
                Map<String, Object> connectorStatus = (Map<String, Object>) status.getOrDefault("connector", emptyMap());
                assertThat(connectorStatus.get("state"), is("RUNNING"));
                assertThat(connectorStatus.get("worker_id").toString(), startsWith("localhost:"));

                List<Map> tasks = (List<Map>) status.get("tasks");
                for (Map an : tasks) {
                    assertThat(an.get("state"), is("RUNNING"));
                    assertThat(an.get("worker_id").toString(), startsWith("localhost:"));
                }
            })))
            .compose(status -> client.getConnectorConfig(Reconciliation.DUMMY_RECONCILIATION, new BackOff(10), "localhost", port, "test"))
            .onComplete(context.succeeding(config -> context.verify(() -> {
                assertThat(config, is(TestUtils.map("connector.class", "FileStreamSource",
                        "file", "/dev/null",
                        "tasks.max", "1",
                        "name", "test",
                        "topic", "my-topic")));
            })))
            .compose(config -> client.getConnectorConfig(Reconciliation.DUMMY_RECONCILIATION, new BackOff(10), "localhost", port, "does-not-exist"))
            .onComplete(context.failing(error -> context.verify(() -> {
                assertThat(error, instanceOf(ConnectRestException.class));
                assertThat(((ConnectRestException) error).getStatusCode(), is(404));
            })))
            .recover(error -> Future.succeededFuture())

            .compose(ignored -> client.pause("localhost", port, "test"))
            .onComplete(context.succeedingThenComplete())

            .compose(ignored -> client.resume("localhost", port, "test"))
            .onComplete(context.succeedingThenComplete())

            .compose(ignored -> client.restart("localhost", port, "test", true, true))
            .onComplete(context.succeedingThenComplete())

            .compose(ignored -> client.restartTask("localhost", port, "test", 0))
            .onComplete(context.succeedingThenComplete())

            .compose(ignored -> {
                JsonObject o = new JsonObject()
                        .put("connector.class", "ThisConnectorDoesNotExist")
                        .put("tasks.max", "1")
                        .put("file", "/dev/null")
                        .put("topic", "my-topic");
                return client.createOrUpdatePutRequest(Reconciliation.DUMMY_RECONCILIATION, "localhost", port, "broken", o);
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
                return client.createOrUpdatePutRequest(Reconciliation.DUMMY_RECONCILIATION, "localhost", port, "broken2", o);
            })
            .onComplete(context.failing(error -> context.verify(() -> {
                assertThat(error, instanceOf(ConnectRestException.class));
                assertThat(error.getMessage(),
                        containsString("Invalid value dog for configuration tasks.max: Not a number of type INT"));
            })))
            .recover(e -> Future.succeededFuture())
            .compose(createResponse -> client.list("localhost", port))
            .onComplete(context.succeeding(connectorNames -> context.verify(() ->
                    assertThat(connectorNames, is(singletonList("test"))))))
            .compose(connectorNames -> client.delete(Reconciliation.DUMMY_RECONCILIATION, "localhost", port, "test"))
            .onComplete(context.succeedingThenComplete())
            .compose(deletedConnector -> client.list("localhost", port))
            .onComplete(context.succeeding(connectorNames -> assertThat(connectorNames, is(empty()))))
            .compose(connectorNames -> client.delete(Reconciliation.DUMMY_RECONCILIATION, "localhost", port, "never-existed"))
            .onComplete(context.failing(error -> {
                assertThat(error, instanceOf(ConnectRestException.class));
                assertThat(error.getMessage(),
                        containsString("Connector never-existed not found"));
                async.flag();
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
        Checkpoint async = context.checkpoint();

        OrderedProperties ops = new OrderedProperties();
        ops.addStringPairs(desired);

        client.updateConnectLoggers(Reconciliation.DUMMY_RECONCILIATION, "localhost", port, desired, ops)
                .onComplete(context.succeeding(wasChanged -> context.verify(() -> assertEquals(true, wasChanged))))
                .compose(a -> client.listConnectLoggers(Reconciliation.DUMMY_RECONCILIATION, "localhost", port)
                        .onComplete(context.succeeding(map -> context.verify(() -> {
                            assertThat(map.get("root"), is("TRACE"));
                            assertThat(map.get("org.apache.zookeeper"), is("WARN"));
                            assertThat(map.get("org.I0Itec.zkclient"), is("INFO"));
                            assertThat(map.get("org.reflections"), is("FATAL"));
                            assertThat(map.get("org.reflections.Reflection"), is("INFO"));
                            assertThat(map.get("org.reflections.Reflection"), is("INFO"));
                            assertThat(map.get("foo"), is("WARN"));
                            assertThat(map.get("foo.bar"), is("TRACE"));
                            assertThat(map.get("foo.bar.quux"), is("DEBUG"));

                        }))))
                .compose(a -> client.updateConnectLoggers(Reconciliation.DUMMY_RECONCILIATION, "localhost", port, desired, ops)
                        .onComplete(context.succeeding(wasChanged -> context.verify(() -> {
                            assertEquals(false, wasChanged);
                            async.flag();
                        }))));
    }

    @IsolatedTest
    public void testHierarchy() {
        String rootLevel = "TRACE";
        String desired = "log4j.rootLogger=" + rootLevel + ", CONSOLE\n" +
                "log4j.logger.oorg.apache.zookeeper=WARN\n" +
                "log4j.logger.oorg.I0Itec.zkclient=INFO\n" +
                "log4j.logger.oorg.reflections.Reflection=INFO\n" +
                "log4j.logger.oorg.reflections=FATAL\n" +
                "log4j.logger.foo=WARN\n" +
                "log4j.logger.foo.bar=TRACE\n" +
                "log4j.logger.oorg.eclipse.jetty.util=DEBUG\n" +
                "log4j.logger.foo.bar.quux=DEBUG";

        KafkaConnectApiImpl client = new KafkaConnectApiImpl(vertx);
        OrderedProperties ops = new OrderedProperties();
        ops.addStringPairs(desired);
        assertEquals("TRACE", client.getEffectiveLevel("foo.bar", ops.asMap()));
        assertEquals("WARN", client.getEffectiveLevel("foo.lala", ops.asMap()));
        assertEquals(rootLevel, client.getEffectiveLevel("bar.faa", ops.asMap()));
        assertEquals("TRACE", client.getEffectiveLevel("org", ops.asMap()));
        assertEquals("DEBUG", client.getEffectiveLevel("oorg.eclipse.jetty.util.thread.strategy.EatWhatYouKill", ops.asMap()));
        assertEquals(rootLevel, client.getEffectiveLevel("oorg.eclipse.group.art", ops.asMap()));
    }
}