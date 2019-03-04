/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.SelfSignedCertificate;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.strimzi.test.TestUtils.map;
import static java.lang.Integer.parseInt;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class ZookeeperLeaderFinderTest {

    private static final Logger log = LogManager.getLogger(ZookeeperLeaderFinderTest.class);

    public static final String NAMESPACE = "testns";
    public static final String CLUSTER = "testcluster";

    private Vertx vertx = Vertx.vertx();
    private SecretOperator mock = mock(SecretOperator.class);
    private SelfSignedCertificate zkCertificate = SelfSignedCertificate.create();
    private SelfSignedCertificate coCertificate = SelfSignedCertificate.create();

    private static final int MAX_ATTEMPTS = 4;

    class TestingZookeeperLeaderFinder extends ZookeeperLeaderFinder {
        private final int[] ports;

        public TestingZookeeperLeaderFinder(Supplier<BackOff> backOffSupplier, int[] ports) {
            super(vertx, mock, backOffSupplier);
            this.ports = ports;
        }

        @Override
        NetClientOptions clientOptions(Secret coCertKeySecret, Secret clusterCaCertificateSecret) {
            return new NetClientOptions()
                    .setKeyCertOptions(coCertificate.keyCertOptions())
                    .setTrustOptions(zkCertificate.trustOptions())
                    .setSsl(true);
        }

        @Override
        protected String host(Pod pod) {
            return "localhost";
        }

        @Override
        protected int port(Pod pod) {
            String name = pod.getMetadata().getName();
            int idx = name.lastIndexOf('-');
            return ports[parseInt(name.substring(idx + 1))];
        }
    }

    List<FakeZk> zks = new ArrayList<>();

    class FakeZk {
        private final int id;
        private final Function<Integer, Boolean> isLeader;
        private final AtomicInteger attempts = new AtomicInteger();
        private final NetServer netServer;

        FakeZk(int id, Function<Integer, Boolean> isLeader) {
            this.id = id;
            this.isLeader = isLeader;
            NetServerOptions nso = new NetServerOptions()
                    .setSsl(true)
                    .setKeyCertOptions(zkCertificate.keyCertOptions())
                    .setTrustOptions(coCertificate.trustOptions());
            netServer = vertx.createNetServer(nso);
        }

        public void stop() {
            netServer.close();
        }

        public Future<Integer> start() {
            Future<Integer> future = Future.future();

            netServer.exceptionHandler(ex -> log.error(ex))
                .connectHandler(socket -> {
                    log.debug("ZK {}: client connection to {}, from {}", id, socket.localAddress(), socket.remoteAddress());
                    socket.exceptionHandler(ex -> log.error(ex));
                    StringBuffer sb = new StringBuffer();
                    socket.handler(buf -> {
                        sb.append(buf.toString());
                        if (sb.toString().startsWith("stat")) {
                            socket.write("vesvsebserb\n");
                            int attempt = attempts.getAndIncrement();
                            if (isLeader.apply(attempt)) {
                                log.debug("ZK {}: is leader on attempt {}", id, attempt);
                                socket.write("Mode: ");
                                socket.write("leader\n");
                            } else {
                                log.debug("ZK {}: is not leader on attempt {}", id, attempt);
                            }
                            socket.write("vesvsebserb\n");
                            log.debug("ZK {}: Sent response, closing", id);
                            socket.close();
                        }
                    });
                })
                .listen(ar -> {
                    if (ar.succeeded()) {
                        future.complete(ar.result().actualPort());
                    } else {
                        future.fail(ar.cause());
                    }
                });
            return future;
        }
    }

    private int[] startMockZks(TestContext context, int num, BiFunction<Integer, Integer, Boolean> fn) {
        int[] result = new int[num];
        Async async = context.async(num);
        for (int i = 0; i < num; i++) {
            final int id = i;
            FakeZk zk = new FakeZk(id, attempt -> fn.apply(id, attempt));
            zks.add(zk);
            zk.start().setHandler(ar -> {
                if (ar.succeeded()) {
                    Integer port = ar.result();
                    log.debug("ZK {} listening on port {}", id, port);
                    result[id] = port;
                    async.countDown();
                } else {
                    context.fail(ar.cause());
                    async.complete();
                }
            });
        }
        async.await();
        return result;
    }

    @After
    public void stopZks() {
        for (FakeZk zk : zks) {
            zk.stop();
        }
    }

    Secret coKeySecret() {
        return new Secret();
    }

    @Test
    public void test0Pods() {
        ZookeeperLeaderFinder finder = new ZookeeperLeaderFinder(vertx, null, this::backoff);
        assertEquals(Integer.valueOf(-1),
                finder.findZookeeperLeader(CLUSTER, NAMESPACE,
                        emptyList(), coKeySecret()).result());
    }

    BackOff backoff() {
        return new BackOff(50, 2, MAX_ATTEMPTS);
    }

    @Test
    public void test1Pods() {
        ZookeeperLeaderFinder finder = new ZookeeperLeaderFinder(vertx, null, this::backoff);
        assertEquals(Integer.valueOf(0),
                finder.findZookeeperLeader(CLUSTER, NAMESPACE,
                        asList(getPod(0)), coKeySecret()).result());
    }

    @Test
    public void testSecretsNotFound() {
        SecretOperator mock = mock(SecretOperator.class);
        ZookeeperLeaderFinder finder = new ZookeeperLeaderFinder(vertx, mock, this::backoff);

        Mockito.reset(mock);
        when(mock.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER))))
                .thenReturn(Future.succeededFuture(
                        new SecretBuilder()
                                .withNewMetadata()
                                    .withName(KafkaResources.clusterCaCertificateSecretName(CLUSTER))
                                    .withNamespace(NAMESPACE)
                                .endMetadata()
                                .withData(emptyMap())
                                .build()));
        assertEquals("The Secret testns/testcluster-cluster-operator-certs is missing the key cluster-operator.key",
                finder.findZookeeperLeader(CLUSTER, NAMESPACE,
                        asList(getPod(0), getPod(1)), new SecretBuilder()
                                .withNewMetadata()
                                .withName(ClusterOperator.secretName(CLUSTER))
                                .withNamespace(NAMESPACE)
                                .endMetadata()
                                .withData(emptyMap())
                                .build()).cause().getMessage());
    }

    @Test
    public void testSecretsCorrupted() {
        SecretOperator mock = mock(SecretOperator.class);
        ZookeeperLeaderFinder finder = new ZookeeperLeaderFinder(vertx, mock, this::backoff);

        when(mock.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER))))
                .thenReturn(Future.succeededFuture(
                        new SecretBuilder()
                                .withNewMetadata()
                                .withName(KafkaResources.clusterCaCertificateSecretName(CLUSTER))
                                .withNamespace(NAMESPACE)
                                .endMetadata()
                                .withData(map(Ca.CA_CRT, "notacert"))
                                .build()));
        Throwable cause = finder.findZookeeperLeader(CLUSTER, NAMESPACE,
                asList(getPod(0), getPod(1)), new SecretBuilder()
                        .withNewMetadata()
                        .withName(ClusterOperator.secretName(CLUSTER))
                        .withNamespace(NAMESPACE)
                        .endMetadata()
                        .withData(map("cluster-operator.key", "notacert",
                                "cluster-operator.crt", "notacert"))
                        .build()).cause();
        assertTrue(cause instanceof RuntimeException);
        assertEquals("Bad/corrupt certificate found in data.cluster-operator\\.crt of Secret testcluster-cluster-operator-certs in namespace testns", cause.getMessage());
    }

    @Test
    public void testTimeout(TestContext context) {
        String coSecretName = ClusterOperator.secretName(CLUSTER);
        when(mock.getAsync(eq(NAMESPACE), eq(coSecretName)))
                .thenReturn(Future.succeededFuture(
                        new SecretBuilder()
                                .withNewMetadata()
                                .withName(coSecretName)
                                .withNamespace(NAMESPACE)
                                .endMetadata()
                                .withData(map("cluster-operator.key", "notacert",
                                        "cluster-operator.crt", "notacert"))
                                .build()));
        String clusterCaSecretName = KafkaResources.clusterCaCertificateSecretName(CLUSTER);
        when(mock.getAsync(eq(NAMESPACE), eq(clusterCaSecretName)))
                .thenReturn(Future.succeededFuture(
                        new SecretBuilder()
                                .withNewMetadata()
                                .withName(clusterCaSecretName)
                                .withNamespace(NAMESPACE)
                                .endMetadata()
                                .withData(map(Ca.CA_CRT, "notacert"))
                                .build()));

        int[] ports = startMockZks(context, 2, (id, attempt) -> false);

        ZookeeperLeaderFinder finder = new TestingZookeeperLeaderFinder(this::backoff, ports);

        Async a = context.async();
        finder.findZookeeperLeader(CLUSTER, NAMESPACE,
                    asList(getPod(0), getPod(1)), coKeySecret())
            .setHandler(ar -> {
                if (ar.succeeded()) {
                    context.assertEquals(-1, ar.result());
                    for (FakeZk zk : zks) {
                        context.assertEquals(MAX_ATTEMPTS + 1, zk.attempts.get(),
                                "Unexpected number of attempts for node " + zk.id);
                    }
                } else {
                    ar.cause().printStackTrace();
                    context.fail();
                }
                a.complete();
            });
    }

    @Test
    public void testTimeoutDueToNetworkExceptions(TestContext context) {
        when(mock.getAsync(eq(NAMESPACE), eq(ClusterOperator.secretName(CLUSTER))))
                .thenReturn(Future.succeededFuture(
                        new SecretBuilder()
                                .withNewMetadata()
                                .withName(ClusterOperator.secretName(CLUSTER))
                                .withNamespace(NAMESPACE)
                                .endMetadata()
                                .withData(map("cluster-operator.key", "notacert",
                                        "cluster-operator.crt", "notacert"))
                                .build()));
        when(mock.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER))))
                .thenReturn(Future.succeededFuture(
                        new SecretBuilder()
                                .withNewMetadata()
                                .withName(KafkaResources.clusterCaCertificateSecretName(CLUSTER))
                                .withNamespace(NAMESPACE)
                                .endMetadata()
                                .withData(map(Ca.CA_CRT, "notacert"))
                                .build()));

        int[] ports = startMockZks(context, 2, (id, attempt) -> false);
        // Use closed ports
        stopZks();

        ZookeeperLeaderFinder finder = new TestingZookeeperLeaderFinder(this::backoff, ports);

        Async a = context.async();
        finder.findZookeeperLeader(CLUSTER, NAMESPACE,
                asList(getPod(0), getPod(1)), coKeySecret())
                .setHandler(result -> {
                    context.assertTrue(result.failed());
                    for (FakeZk zk : zks) {
                        context.assertEquals(0, zk.attempts.get(),
                                "Unexpected number of attempts for node " + zk.id);
                    }
                    a.complete();
                });
    }

    @Test
    public void testLeaderFoundThirdAttempt(TestContext context) {
        int leader = 1;
        int succeedOnAttempt = 2;
        when(mock.getAsync(eq(NAMESPACE), eq(ClusterOperator.secretName(CLUSTER))))
                .thenReturn(Future.succeededFuture(
                        new SecretBuilder()
                                .withNewMetadata()
                                .withName(ClusterOperator.secretName(CLUSTER))
                                .withNamespace(NAMESPACE)
                                .endMetadata()
                                .withData(map("cluster-operator.key", "notacert",
                                        "cluster-operator.crt", "notacert"))
                                .build()));
        when(mock.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER))))
                .thenReturn(Future.succeededFuture(
                        new SecretBuilder()
                                .withNewMetadata()
                                .withName(KafkaResources.clusterCaCertificateSecretName(CLUSTER))
                                .withNamespace(NAMESPACE)
                                .endMetadata()
                                .withData(map(Ca.CA_CRT, "notacert"))
                                .build()));

        int[] ports = startMockZks(context, 2, (id, attempt) -> attempt == succeedOnAttempt && id == leader);

        TestingZookeeperLeaderFinder finder = new TestingZookeeperLeaderFinder(this::backoff, ports);

        Async a = context.async();
        finder.findZookeeperLeader(CLUSTER, NAMESPACE,
                asList(getPod(0), getPod(1)), coKeySecret())
                .setHandler(ar -> {
                    if (ar.succeeded()) {
                        context.assertEquals(leader, ar.result());
                        for (FakeZk zk : zks) {
                            context.assertEquals(succeedOnAttempt + 1, zk.attempts.get(),
                                    "Unexpected number of attempts for node " + zk.id);
                        }
                    } else {
                        ar.cause().printStackTrace();
                        context.fail();
                    }
                    a.complete();
                });
    }

    @Test
    public void testLeaderFoundFirstAttempt(TestContext context) {
        int leader = 1;
        when(mock.getAsync(eq(NAMESPACE), eq(ClusterOperator.secretName(CLUSTER))))
                .thenAnswer(i -> Future.succeededFuture(
                        new SecretBuilder()
                                .withNewMetadata()
                                .withName(ClusterOperator.secretName(CLUSTER))
                                .withNamespace(NAMESPACE)
                                .endMetadata()
                                .withData(map("cluster-operator.key", "notacert",
                                        "cluster-operator.crt", "notacert"))
                                .build()));
        when(mock.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER))))
                .thenAnswer(i -> Future.succeededFuture(
                        new SecretBuilder()
                                .withNewMetadata()
                                .withName(KafkaResources.clusterCaCertificateSecretName(CLUSTER))
                                .withNamespace(NAMESPACE)
                                .endMetadata()
                                .withData(map(Ca.CA_CRT, "notacert"))
                                .build()));

        int[] ports = startMockZks(context, 2, (id, attempt) -> id == leader);

        ZookeeperLeaderFinder finder = new TestingZookeeperLeaderFinder(this::backoff, ports);

        Async a = context.async();
        finder.findZookeeperLeader(CLUSTER, NAMESPACE, asList(getPod(0), getPod(1)), coKeySecret())
                .setHandler(asyncResult -> {
                    if (asyncResult.failed()) {
                        asyncResult.cause().printStackTrace();
                    }
                    context.assertTrue(asyncResult.succeeded());
                    context.assertEquals(leader, asyncResult.result());
                    for (FakeZk zk : zks) {
                        context.assertEquals(1, zk.attempts.get(),
                                "Unexpected number of attempts for node " + zk.id);
                    }
                    a.complete();
                });
    }

    Pod getPod(int id) {
        return new PodBuilder().withNewMetadata().withName("my-cluster-kafka-" + id).endMetadata().build();
    }

    @Test
    public void testGetHost() {
        Pod pod = new PodBuilder()
                .withNewMetadata()
                    .withName(ZookeeperCluster.zookeeperPodName("my-cluster", 3))
                    .withNamespace("myproject")
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, "my-cluster")
                .endMetadata()
            .build();
        assertEquals("my-cluster-zookeeper-3.my-cluster-zookeeper-nodes.myproject.svc.cluster.local",
                new ZookeeperLeaderFinder(vertx, null, this::backoff).host(pod));
    }
}
