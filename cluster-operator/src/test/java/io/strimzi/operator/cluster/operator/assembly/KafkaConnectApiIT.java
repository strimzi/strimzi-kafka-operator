/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.connect.ConnectorPlugin;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.test.container.StrimziKafkaCluster;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class KafkaConnectApiIT {
    private static StrimziKafkaCluster cluster;

    private ConnectCluster connectCluster;
    private int port;

    @BeforeEach
    public void beforeEach() throws InterruptedException {
        // Start a 1 node connect cluster
        connectCluster = new ConnectCluster()
                .usingBrokers(cluster.getBootstrapServers())
                .addConnectNodes(1);
        connectCluster.startup();
        port = connectCluster.getPort(0);
    }

    @AfterEach
    public void afterEach() {
        if (connectCluster != null) {
            connectCluster.shutdown();
        }
    }

    @BeforeAll
    public static void before() throws IOException {
        cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(1)
                .withInternalTopicReplicationFactor(1)
                .withSharedNetwork()
                .build();
        cluster.start();
    }

    @AfterAll
    public static void after() {
        cluster.stop();
    }

    private void checkStatusWithDelay(Supplier<CompletableFuture<Map<String, Object>>> statusSupplier,
                                      ScheduledExecutorService singleExecutor,
                                      CompletableFuture<Map<String, Object>> completableFuture,
                                      long delay) {
        singleExecutor.schedule(() -> {
            statusSupplier.get().whenComplete((status, error) -> {
                if (error == null) {
                    if ("RUNNING".equals(((Map<String, Object>) status.getOrDefault("connector", emptyMap())).get("state"))) {
                        completableFuture.complete(status);
                    } else {
                        System.err.println(status);
                        checkStatusWithDelay(statusSupplier, singleExecutor, completableFuture, delay);
                    }
                } else {
                    error.printStackTrace();
                    checkStatusWithDelay(statusSupplier, singleExecutor, completableFuture, delay);
                }
            });
        }, delay, TimeUnit.MILLISECONDS);
    }

    @Test
    @SuppressWarnings({"unchecked", "checkstyle:MethodLength", "checkstyle:NPathComplexity"})
    public void test() throws InterruptedException {
        KafkaConnectApi client = new KafkaConnectApiImpl();
        Thread.sleep(10_000L);
        client.listConnectorPlugins(Reconciliation.DUMMY_RECONCILIATION, "localhost", port)
            .whenComplete((connectorPlugins, error) -> {
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
            })

            .thenCompose(connectorPlugins -> client.list(Reconciliation.DUMMY_RECONCILIATION, "localhost", port))
            .whenComplete((connectorNames, error) -> assertThat(connectorNames, is(empty())))

            .thenCompose(connectorNames -> {
                JsonObject o = new JsonObject()
                    .put("connector.class", "FileStreamSource")
                    .put("tasks.max", "1")
                    .put("file", "/dev/null")
                    .put("topic", "my-topic");
                return client.createOrUpdatePutRequest(Reconciliation.DUMMY_RECONCILIATION, "localhost", port, "test", o);
            })

            .thenCompose(created -> {
                CompletableFuture<Map<String, Object>> completableFuture = new CompletableFuture<>();
                ScheduledExecutorService singleExecutor = Executors.newSingleThreadScheduledExecutor(
                        runnable -> new Thread(runnable, "kafka-connect-api-test"));
                checkStatusWithDelay(() -> client.status(Reconciliation.DUMMY_RECONCILIATION, "localhost", port, "test"), singleExecutor, completableFuture, 1000);
                completableFuture.whenComplete((r, e) -> singleExecutor.shutdown());
                return completableFuture;
            })
            .whenComplete((status, error) -> {
                assertNull(error);
                assertThat(status.get("name"), is("test"));
                Map<String, Object> connectorStatus = (Map<String, Object>) status.getOrDefault("connector", emptyMap());
                assertThat(connectorStatus.get("state"), is("RUNNING"));
                assertThat(connectorStatus.get("worker_id").toString(), startsWith("localhost:"));

                List<Map<String, String>> tasks = (List<Map<String, String>>) status.get("tasks");
                for (Map<String, String> an : tasks) {
                    assertThat(an.get("state"), is("RUNNING"));
                    assertThat(an.get("worker_id"), startsWith("localhost:"));
                }
            })

            .thenCompose(status -> client.getConnectorConfig(Reconciliation.DUMMY_RECONCILIATION, new BackOff(10), "localhost", port, "test"))
            .whenComplete((config, error) -> {
                assertNull(error);
                assertThat(config, is(Map.of("connector.class", "FileStreamSource",
                        "file", "/dev/null",
                        "tasks.max", "1",
                        "name", "test",
                        "topic", "my-topic")));
            })

            .thenCompose(ignored -> client.getConnectorConfig(Reconciliation.DUMMY_RECONCILIATION, new BackOff(10), "localhost", port, "does-not-exist")
                        .handle((r, e) -> {
                            assertThat(e, is(instanceOf(ConnectRestException.class)));
                            assertThat(((ConnectRestException) e).getStatusCode(), is(404));
                            if (e == null) {
                                throw new RuntimeException("Expected failure when getting config for connector that doesn't exist");
                            }
                            return null;
                        }))

            .thenCompose(ignored -> client.pause(Reconciliation.DUMMY_RECONCILIATION, "localhost", port, "test"))

            .thenCompose(ignored -> client.resume(Reconciliation.DUMMY_RECONCILIATION, "localhost", port, "test"))

            .thenCompose(ignored -> client.stop(Reconciliation.DUMMY_RECONCILIATION, "localhost", port, "test"))

            .thenCompose(ignored -> client.resume(Reconciliation.DUMMY_RECONCILIATION, "localhost", port, "test"))

            .thenCompose(ignored -> client.restart("localhost", port, "test", true, true))

            .thenCompose(ignored -> client.restartTask("localhost", port, "test", 0))

            .thenCompose(ignored -> {
                JsonObject o = new JsonObject()
                        .put("connector.class", "ThisConnectorDoesNotExist")
                        .put("tasks.max", "1")
                        .put("file", "/dev/null")
                        .put("topic", "my-topic");
                return client.createOrUpdatePutRequest(Reconciliation.DUMMY_RECONCILIATION, "localhost", port, "broken", o)
                        .handle((r, e) -> {
                            assertThat(e.getCause(), is(instanceOf(ConnectRestException.class)));
                            assertThat(e.getMessage(), containsString("Failed to find any class that implements Connector and which name matches ThisConnectorDoesNotExist"));
                            if (e == null) {
                                throw new RuntimeException("Expected error: Failed to find any class that implements Connector and which name matches ThisConnectorDoesNotExist");
                            }
                            return null;
                        });
            })

            .thenCompose(ignored -> {
                JsonObject o = new JsonObject()
                        .put("connector.class", "FileStreamSource")
                        .put("tasks.max", "dog")
                        .put("file", "/dev/null")
                        .put("topic", "my-topic");
                return client.createOrUpdatePutRequest(Reconciliation.DUMMY_RECONCILIATION, "localhost", port, "broken2", o)
                        .handle((r, e) -> {
                            assertThat(e.getCause(), is(instanceOf(ConnectRestException.class)));
                            assertThat(e.getMessage(), containsString("Invalid value dog for configuration tasks.max: Not a number of type INT"));
                            if (e == null) {
                                throw new RuntimeException("Expected error: Invalid value dog for configuration tasks.max: Not a number of type INT");
                            }
                            return null;
                        });
            })

            .thenCompose(createResponse -> client.list(Reconciliation.DUMMY_RECONCILIATION, "localhost", port))
            .whenComplete((connectorNames, error) ->
                    assertThat(connectorNames, is(singletonList("test"))))

            .thenCompose(connectorNames -> client.delete(Reconciliation.DUMMY_RECONCILIATION, "localhost", port, "test"))

            .thenCompose(deletedConnector -> client.list(Reconciliation.DUMMY_RECONCILIATION, "localhost", port))
            .whenComplete((connectorNames, error) -> assertThat(connectorNames, is(empty())))

            .thenCompose(connectorNames -> client.delete(Reconciliation.DUMMY_RECONCILIATION, "localhost", port, "never-existed")
                    .handle((r, e) -> {
                        assertThat(e.getCause(), is(instanceOf(ConnectRestException.class)));
                        assertThat(e.getMessage(), containsString("Connector never-existed not found"));
                        if (e == null) {
                            throw new RuntimeException("Expected error: Connector never-existed not found");
                        }
                        return null;
                    }))
            .join();
    }
}