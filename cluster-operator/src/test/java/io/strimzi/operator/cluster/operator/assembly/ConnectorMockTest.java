/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.KafkaConnectS2IList;
import io.strimzi.api.kafka.KafkaConnectorList;
import io.strimzi.api.kafka.model.DoneableKafkaConnect;
import io.strimzi.api.kafka.model.DoneableKafkaConnectS2I;
import io.strimzi.api.kafka.model.DoneableKafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnectorBuilder;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.test.mockkube.MockKube;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import static io.strimzi.test.TestUtils.map;
import static io.strimzi.test.TestUtils.set;
import static io.strimzi.test.TestUtils.waitFor;
import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class ConnectorMockTest {

    private static final Logger log = LogManager.getLogger(ConnectorMockTest.class);
    private static final String NAMESPACE = "ns";

    private Vertx vertx;
    private KubernetesClient client;
    private KafkaConnectApi api;
    private HashMap<String, JsonObject> runningConnectors;
    private KafkaConnectS2IAssemblyOperator kafkaConnectS2iOperator;
    private KafkaConnectAssemblyOperator kafkaConnectOperator;

    @BeforeEach
    public void setup(VertxTestContext testContext) throws InterruptedException {
        vertx = Vertx.vertx();
        client = new MockKube()
                .withCustomResourceDefinition(Crds.kafkaConnect(), KafkaConnect.class, KafkaConnectList.class, DoneableKafkaConnect.class,
                        KafkaConnect::getStatus, KafkaConnect::setStatus).end()
                .withCustomResourceDefinition(Crds.kafkaConnectS2I(), KafkaConnectS2I.class, KafkaConnectS2IList.class, DoneableKafkaConnectS2I.class,
                        KafkaConnectS2I::getStatus, KafkaConnectS2I::setStatus).end()
                .withCustomResourceDefinition(Crds.kafkaConnector(), KafkaConnector.class, KafkaConnectorList.class, DoneableKafkaConnector.class,
                        KafkaConnector::getStatus, KafkaConnector::setStatus).end()
                .build();

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, KubernetesVersion.V1_11);
        MockCertManager certManager = new MockCertManager();

        api = mock(KafkaConnectApi.class);
        runningConnectors = new HashMap<>();
        when(api.list(any(), anyInt())).thenAnswer(i -> {
            return Future.succeededFuture(new ArrayList<>(runningConnectors.keySet()));
        });
        when(api.createOrUpdatePutRequest(any(), anyInt(), anyString(), any())).thenAnswer(invocation -> {
            String connectorName = invocation.getArgument(2);
            runningConnectors.put(connectorName, invocation.getArgument(3));
            return Future.succeededFuture();
        });
        when(api.delete(any(), anyInt(), anyString())).thenAnswer(invocation -> {
            String connectorName = invocation.getArgument(2);
            JsonObject remove = runningConnectors.remove(connectorName);
            return remove != null ? Future.succeededFuture() : Future.failedFuture("No such connector " + connectorName);
        });

        ResourceOperatorSupplier ros = new ResourceOperatorSupplier(vertx, client, pfa, 10_000);
        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(map(
            ClusterOperatorConfig.STRIMZI_KAFKA_IMAGES, "2.1.0=foo 2.1.1=foo 2.2.0=foo 2.2.1=foo 2.3.0=foo",
            ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_IMAGES, "2.1.0=foo 2.1.1=foo 2.2.0=foo 2.2.1=foo 2.3.0=foo",
            ClusterOperatorConfig.STRIMZI_KAFKA_CONNECT_S2I_IMAGES, "2.1.0=foo 2.1.1=foo 2.2.0=foo 2.2.1=foo 2.3.0=foo",
            ClusterOperatorConfig.STRIMZI_FULL_RECONCILIATION_INTERVAL_MS, Long.toString(Long.MAX_VALUE)));
        kafkaConnectOperator = new KafkaConnectAssemblyOperator(vertx,
            pfa,
            ros,
            config,
            x -> api);
        CountDownLatch async2 = new CountDownLatch(1);
        kafkaConnectOperator.createWatch(NAMESPACE, e -> testContext.failNow(e)).setHandler(asyncResultHandler(testContext, async2));
        async2.await(30, TimeUnit.SECONDS);
        kafkaConnectS2iOperator = new KafkaConnectS2IAssemblyOperator(vertx,
            pfa,
            ros,
            config,
            x -> api);
        CountDownLatch async1 = new CountDownLatch(1);
        kafkaConnectS2iOperator.createWatch(NAMESPACE, e -> testContext.failNow(e)).setHandler(asyncResultHandler(testContext, async1));
        async1.await(30, TimeUnit.SECONDS);
        CountDownLatch async = new CountDownLatch(1);
        AbstractConnectOperator.createConnectorWatch(kafkaConnectOperator, kafkaConnectS2iOperator, NAMESPACE).setHandler(asyncResultHandler(testContext, async));
        async.await(30, TimeUnit.SECONDS);

        testContext.completeNow();
    }

    @AfterEach
    public void teardown() {
        vertx.close();
    }

    public <T> Handler<AsyncResult<T>> asyncResultHandler(VertxTestContext testContext, CountDownLatch async) {
        return ar -> {
            if (ar.failed()) {
                testContext.failNow(ar.cause());
            }
            async.countDown();
        };
    }

    public Predicate<KafkaConnect> connectReady() {
        return c -> {
            log.debug(c);
            return c.getStatus() != null && c.getStatus().getConditions().stream()
                    .anyMatch(condition -> "Ready".equals(condition.getType()));
        };
    }

    public Predicate<KafkaConnectS2I> connectS2IReady() {
        return c -> {
            log.debug(c);
            return c.getStatus() != null && c.getStatus().getConditions().stream()
                    .anyMatch(condition -> "Ready".equals(condition.getType()));
        };
    }

    public Predicate<KafkaConnector> connectorReady() {
        return c -> {
            log.debug(c);
            return c.getStatus() != null && c.getStatus().getConditions().stream()
                    .anyMatch(condition -> "Ready".equals(condition.getType()));
        };
    }

    public void waitForConnectReady(String connectName) {
        Resource<KafkaConnect, DoneableKafkaConnect> resource = Crds.kafkaConnectOperation(client)
                .inNamespace(NAMESPACE)
                .withName(connectName);
        try {
            resource.waitUntilCondition(connectReady(), 5, TimeUnit.HOURS);
        } catch (Exception e) {
            if (!(e instanceof TimeoutException)) {
                throw new RuntimeException(e);
            }
            String conditions =
                    resource.get().getStatus() == null ? "no status" :
                            String.valueOf(resource.get().getStatus().getConditions());
            fail(connectName + " never became ready: " + conditions);
        }
    }

    public void waitForConnectUnready(String connectName, String reason, String message) {
        Resource<KafkaConnect, DoneableKafkaConnect> resource = Crds.kafkaConnectOperation(client)
                .inNamespace(NAMESPACE)
                .withName(connectName);
        try {
            resource.waitUntilCondition(connectUnreadyWithReason(reason, message), 5, TimeUnit.SECONDS);
        } catch (Exception e) {
            if (!(e instanceof TimeoutException)) {
                throw new RuntimeException(e);
            }
            String conditions =
                    resource.get().getStatus() == null ? "no status" :
                            String.valueOf(resource.get().getStatus().getConditions());
            fail(connectName + " never became unready: " + conditions);
        }
    }

    public void waitForConnectS2iReady(VertxTestContext testContext, String connectName) {
        Resource<KafkaConnectS2I, DoneableKafkaConnectS2I> resource = Crds.kafkaConnectS2iOperation(client)
                .inNamespace(NAMESPACE)
                .withName(connectName);
        try {
            resource.waitUntilCondition(connectS2IReady(), 5, TimeUnit.HOURS);
        } catch (Exception e) {
            if (!(e instanceof TimeoutException)) {
                throw new RuntimeException(e);
            }
            String conditions =
                    resource.get().getStatus() == null ? "no status" :
                            String.valueOf(resource.get().getStatus().getConditions());
            testContext.failNow(new AssertionError(connectName + " never became ready: " + conditions));
        }
    }

    public void waitForConnectS2iUnready(String connectName, String reason, String message) {
        Resource<KafkaConnectS2I, DoneableKafkaConnectS2I> resource = Crds.kafkaConnectS2iOperation(client)
                .inNamespace(NAMESPACE)
                .withName(connectName);
        try {
            resource.waitUntilCondition(connectS2IUnreadyWithReason(reason, message), 5, TimeUnit.SECONDS);
        } catch (Exception e) {
            if (!(e instanceof TimeoutException)) {
                throw new RuntimeException(e);
            }
            String conditions =
                    resource.get().getStatus() == null ? "no status" :
                            String.valueOf(resource.get().getStatus().getConditions());
            fail(connectName + " never became unready: " + conditions);
        }
    }

    public void waitForConnectorReady(String connectorName) {
        Resource<KafkaConnector, DoneableKafkaConnector> resource = Crds.kafkaConnectorOperation(client)
                .inNamespace(NAMESPACE)
                .withName(connectorName);
        try {
            resource.waitUntilCondition(connectorReady(), 5, TimeUnit.SECONDS);
        } catch (Exception e) {
            if (!(e instanceof TimeoutException)) {
                throw new RuntimeException(e);
            }
            String conditions =
                    resource.get().getStatus() == null ? "no status" :
                    String.valueOf(resource.get().getStatus().getConditions());
            fail(connectorName + " never became ready: " + conditions);
        }
    }

    public void waitForConnectorUnready(String connectorName, String reason, String message) {
        Resource<KafkaConnector, DoneableKafkaConnector> resource = Crds.kafkaConnectorOperation(client)
                .inNamespace(NAMESPACE)
                .withName(connectorName);
        try {
            resource.waitUntilCondition(connectorUnreadyWithReason(reason, message), 5, TimeUnit.SECONDS);
        } catch (Exception e) {
            if (!(e instanceof TimeoutException)) {
                throw new RuntimeException(e);
            }
            String conditions =
                    resource.get().getStatus() == null ? "no status" :
                            String.valueOf(resource.get().getStatus().getConditions());
            fail(connectorName + " never became unready: " + conditions);
        }
    }

    public Predicate<KafkaConnect> connectUnreadyWithReason(String reason, String message) {
        return c -> {
            log.debug(c);
            return c.getStatus() != null && c.getStatus().getConditions().stream()
                    .anyMatch(condition ->
                        "NotReady".equals(condition.getType())
                            && "True".equals(condition.getStatus())
                            && reason.equals(condition.getReason())
                            && Objects.equals(message, condition.getMessage())
                    );
        };
    }

    public Predicate<KafkaConnectS2I> connectS2IUnreadyWithReason(String reason, String message) {
        return c -> {
            log.debug(c);
            return c.getStatus() != null && c.getStatus().getConditions().stream()
                    .anyMatch(condition ->
                            "NotReady".equals(condition.getType())
                                    && "True".equals(condition.getStatus())
                                    && reason.equals(condition.getReason())
                                    && Objects.equals(message, condition.getMessage())
                    );
        };
    }

    public Predicate<KafkaConnector> connectorUnreadyWithReason(String reason, String message) {
        return c -> {
            log.debug(c);
            return c.getStatus() != null && c.getStatus().getConditions().stream()
                    .anyMatch(condition ->
                            "NotReady".equals(condition.getType())
                                    && "True".equals(condition.getStatus())
                                    && reason.equals(condition.getReason())
                                    && Objects.equals(message, condition.getMessage())
                    );
        };
    }

    @Test
    public void testConnectWithoutSpec() {
        String connectName = "cluster";

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(connectName)
                .endMetadata()
                .done();
        waitForConnectUnready(connectName, "InvalidResourceException", "spec property is required");
        return;
    }

    @Test
    public void testConnectorWithoutLabel() {
        String connectorName = "connector";

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connectorName)
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .done();
        waitForConnectorUnready(connectorName, "InvalidResourceException",
                "Resource lacks label 'strimzi.io/cluster': No connect cluster in which to create this connector.");
        return;
    }

    @Test
    public void testConnectorButConnectDoesNotExist() {
        String connectorName = "connector";

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(connectorName)
                .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, "cluster")
                .endMetadata()
                .done();
        waitForConnectorUnready(connectorName, "NoSuchResourceException",
                "KafkaConnect resource 'cluster' identified by label 'strimzi.io/cluster' does not exist in namespace ns.");
        return;
    }

    @Test
    public void testConnectorWithoutSpec() {
        String connectName = "cluster";
        String connectorName = "connector";

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connectName)
                    .addToAnnotations("strimzi.io/use-connector-resources", "true")
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .done();
        waitForConnectReady(connectName);

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connectorName)
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName)
                .endMetadata()
                .done();
        waitForConnectorUnready(connectorName, "InvalidResourceException", "spec property is required");
        return;
    }

    @Test
    public void testConnectorButConnectNotConfiguredForConnectors() {
        String connectName = "cluster";
        String connectorName = "connector";

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(connectName)
                //.addToAnnotations("strimzi.io/use-connector-resources", "true")
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .done();
        waitForConnectReady(connectName);

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withName(connectorName)
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName)
                .endMetadata()
                .withNewSpec().endSpec()
                .done();
        assertNotNull(Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).withName(connectorName).get());
        waitForConnectorUnready(connectorName, "NoSuchResourceException", "Not configured for connector management");
    }

    /** Create connect, create connector, delete connector, delete connect */
    @Test
    public void testConnectConnectorConnectorConnect() {
        String connectName = "cluster";
        String connectorName = "connector";

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connectName)
                    .addToAnnotations("strimzi.io/use-connector-resources", "true")
                .endMetadata()
                .withNewSpec()
                .endSpec()
            .done();
        waitForConnectReady(connectName);

        // triggered twice (creation+status update)
        verify(api, atLeastOnce()).list(
                eq(KafkaConnectResources.serviceName(connectName)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, never()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.serviceName(connectName)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withName(connectorName)
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName)
                .endMetadata()
                .withNewSpec()
                .endSpec()
            .done();
        waitForConnectorReady(connectorName);

        verify(api, atLeastOnce()).list(
                eq(KafkaConnectResources.serviceName(connectName)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, atLeastOnce()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.serviceName(connectName)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());
        assertEquals(runningConnectors.keySet(), set(connectorName));
        
        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).withName(connectorName).delete();
        //waitForConnectReady(testContext, connectName);
        waitFor("delete call on connect REST api", 1_000, 30_000, () -> {
            return runningConnectors.isEmpty();
        });
        verify(api).delete(
                eq(KafkaConnectResources.serviceName(connectName)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName));

        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).withName(connectName).delete();
    }

    /** Create connector, create connect, delete connector, delete connect */
    @Test
    public void testConnectorConnectConnectorConnect() {
        String connectName = "cluster";
        String connectorName = "connector";

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                .withName(connectorName)
                .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName)
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .done();
        waitForConnectorUnready(connectorName, "NoSuchResourceException",
            "KafkaConnect resource 'cluster' identified by label 'strimzi.io/cluster' does not exist in namespace ns.");

        verify(api, never()).list(
                eq(KafkaConnectResources.serviceName(connectName)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, never()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.serviceName(connectName)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());
        assertEquals(runningConnectors.keySet(), emptySet());

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connectName)
                    .addToAnnotations("strimzi.io/use-connector-resources", "true")
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .done();
        waitForConnectReady(connectName);
        // (connect crt, connector status, connect status)
        verify(api, atLeastOnce()).list(
                eq(KafkaConnectResources.serviceName(connectName)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, atLeastOnce()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.serviceName(connectName)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());
        assertEquals(runningConnectors.keySet(), set(connectorName));

        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).withName(connectorName).delete();
        //waitForConnectReady(testContext, connectName);
        waitFor("delete call on connect REST api", 1_000, 30_000, () -> {
            return runningConnectors.isEmpty();
        });
        verify(api).delete(
                eq(KafkaConnectResources.serviceName(connectName)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName));

        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).withName(connectName).delete();
    }

    /** Create connect, create connector, delete connect, delete connector */
    @Test
    public void testConnectConnectorConnectConnector() {
        String connectName = "cluster";
        String connectorName = "connector";

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connectName)
                    .addToAnnotations("strimzi.io/use-connector-resources", "true")
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .done();
        waitForConnectReady(connectName);

        verify(api, atLeastOnce()).list(
                eq(KafkaConnectResources.serviceName(connectName)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, never()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.serviceName(connectName)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                .withName(connectorName)
                .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName)
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .done();
        waitForConnectorReady(connectorName);

        verify(api, atLeastOnce()).list(
                eq(KafkaConnectResources.serviceName(connectName)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, atLeastOnce()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.serviceName(connectName)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());
        assertEquals(runningConnectors.keySet(), set(connectorName));

        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).withName(connectName).delete();
        waitForConnectorUnready(connectorName,
                "NoSuchResourceException", "KafkaConnect resource 'cluster' identified by label 'strimzi.io/cluster' does not exist in namespace ns.");

        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).withName(connectorName).delete();
        verify(api, never()).delete(
                eq(KafkaConnectResources.serviceName(connectName)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName));
    }

    /** Create connector, create connect, delete connect, delete connector */
    @Test
    public void testConnectorConnectConnectConnector() {
        String connectName = "cluster";
        String connectorName = "connector";

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withName(connectorName)
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, connectName)
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .done();
        waitForConnectorUnready(connectorName, "NoSuchResourceException",
                "KafkaConnect resource 'cluster' identified by label 'strimzi.io/cluster' does not exist in namespace ns.");

        verify(api, never()).list(
                eq(KafkaConnectResources.serviceName(connectName)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, never()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.serviceName(connectName)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());
        assertEquals(runningConnectors.keySet(), emptySet());

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connectName)
                    .addToAnnotations("strimzi.io/use-connector-resources", "true")
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .done();
        waitForConnectReady(connectName);

        verify(api, atLeastOnce()).list(
                eq(KafkaConnectResources.serviceName(connectName)), eq(KafkaConnectCluster.REST_API_PORT));
        verify(api, atLeastOnce()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.serviceName(connectName)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());
        assertEquals(runningConnectors.keySet(), set(connectorName));

        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).withName(connectName).delete();
        waitForConnectorUnready(connectorName,
                "NoSuchResourceException", "KafkaConnect resource 'cluster' identified by label 'strimzi.io/cluster' does not exist in namespace ns.");

        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).withName(connectorName).delete();
        verify(api, never()).delete(
                eq(KafkaConnectResources.serviceName(connectName)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName));
    }

    /** Change the cluster label from one cluster to another; check the connector is deleted from the old cluster */
    @Test
    public void testChangeLabel() throws InterruptedException {
        String connect1Name = "cluster1";
        String connect2Name = "cluster2";
        String connectorName = "connector";

        // Create two connect clusters
        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connect1Name)
                    .addToAnnotations("strimzi.io/use-connector-resources", "true")
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .done();
        waitForConnectReady(connect1Name);
        Crds.kafkaConnectOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(connect2Name)
                    .addToAnnotations("strimzi.io/use-connector-resources", "true")
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .done();
        waitForConnectReady(connect2Name);

        // Create KafkaConnect cluster and wait till it's ready
        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).createNew()
                .withNewMetadata()
                    .withName(connectorName)
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, connect1Name)
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .done();
        waitForConnectorReady(connectorName);

        verify(api, atLeastOnce()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.serviceName(connect1Name)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());
        verify(api, never()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.serviceName(connect2Name)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());

        Crds.kafkaConnectorOperation(client).inNamespace(NAMESPACE).withName(connectorName).patch(new KafkaConnectorBuilder()
                .withNewMetadata()
                    .withName(connectorName)
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, connect2Name)
                .endMetadata()
                .withNewSpec()
                .endSpec().build());
        waitForConnectorReady(connectorName);

        // Note: The connector does not get deleted immediately from cluster 1, only on the next timed reconciliation

        verify(api, never()).delete(
                eq(KafkaConnectResources.serviceName(connect1Name)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName));
        verify(api, atLeastOnce()).createOrUpdatePutRequest(
                eq(KafkaConnectResources.serviceName(connect2Name)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName), any());

        CountDownLatch async = new CountDownLatch(1);
        kafkaConnectOperator.reconcile(new Reconciliation("test", "KafkaConnect", NAMESPACE, connect1Name)).setHandler(ar -> {
            async.countDown();
        });
        async.await(30, TimeUnit.SECONDS);
        verify(api, atLeastOnce()).delete(
                eq(KafkaConnectResources.serviceName(connect1Name)), eq(KafkaConnectCluster.REST_API_PORT),
                eq(connectorName));
    }

    /*
     * With and without openshift
     *
     */


}
