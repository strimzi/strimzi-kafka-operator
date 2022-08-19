/*
 * Copyright Strimzi authors.
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
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.SelfSignedCertificate;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.strimzi.test.TestUtils.map;
import static java.lang.Integer.parseInt;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class ZookeeperLeaderFinderTest {

    private static final Logger LOGGER = LogManager.getLogger(ZookeeperLeaderFinderTest.class);

    public static final String NAMESPACE = "testns";
    public static final String CLUSTER = "testcluster";

    private static Vertx vertx;
    private final SelfSignedCertificate zkCertificate = SelfSignedCertificate.create();
    private final SelfSignedCertificate coCertificate = SelfSignedCertificate.create();

    private static final int MAX_ATTEMPTS = 4;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    class TestingZookeeperLeaderFinder extends ZookeeperLeaderFinder {
        private final int[] ports;

        public TestingZookeeperLeaderFinder(Supplier<BackOff> backOffSupplier, int[] ports) {
            super(vertx, backOffSupplier);
            this.ports = ports;
        }

        @Override
        NetClientOptions clientOptions(Reconciliation reconciliation, Secret coCertKeySecret, Secret clusterCaCertificateSecret) {
            return new NetClientOptions()
                    .setKeyCertOptions(coCertificate.keyCertOptions())
                    .setTrustOptions(zkCertificate.trustOptions())
                    .setSsl(true);
        }

        @Override
        protected String host(Reconciliation reconciliation, String podName) {
            return "localhost";
        }

        @Override
        protected int port(String podName) {
            int idx = podName.lastIndexOf('-');
            return ports[parseInt(podName.substring(idx + 1))];
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
            CountDownLatch countDownLatch = new CountDownLatch(1);
            netServer.close(closeResult -> countDownLatch.countDown());
            try {
                countDownLatch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOGGER.error("Failed to close zk instance", e);
            }
        }

        public Future<Integer> start() {
            Promise<Integer> promise = Promise.promise();

            netServer.exceptionHandler(LOGGER::error)
                .connectHandler(socket -> {
                    LOGGER.debug("ZK {}: client connection to {}, from {}", id, socket.localAddress(), socket.remoteAddress());
                    socket.exceptionHandler(LOGGER::error);
                    StringBuffer sb = new StringBuffer();
                    socket.handler(buf -> {
                        sb.append(buf.toString());
                        if (sb.toString().startsWith("stat")) {
                            socket.write("vesvsebserb\n");
                            int attempt = attempts.getAndIncrement();
                            if (isLeader.apply(attempt)) {
                                LOGGER.debug("ZK {}: is leader on attempt {}", id, attempt);
                                socket.write("Mode: ");
                                socket.write("leader\n");
                            } else {
                                LOGGER.debug("ZK {}: is not leader on attempt {}", id, attempt);
                            }
                            socket.write("vesvsebserb\n");
                            LOGGER.debug("ZK {}: Sent response, closing", id);
                            socket.close();
                        }
                    });
                })
                .listen(ar -> {
                    if (ar.succeeded()) {
                        promise.complete(ar.result().actualPort());
                    } else {
                        promise.fail(ar.cause());
                    }
                });
            return promise.future();
        }
    }

    private int[] startMockZks(VertxTestContext context, int num, BiFunction<Integer, Integer, Boolean> fn) throws InterruptedException {
        int[] result = new int[num];
        CountDownLatch async = new CountDownLatch(num);
        for (int i = 0; i < num; i++) {
            final int id = i;
            FakeZk zk = new FakeZk(id, attempt -> fn.apply(id, attempt));
            zks.add(zk);
            zk.start().onComplete(context.succeeding(port -> {
                LOGGER.debug("ZK {} listening on port {}", id, port);
                result[id] = port;
                async.countDown();
            }));
        }
        if (!async.await(60, TimeUnit.SECONDS)) {
            context.failNow(new Throwable("Test timeout"));
        }
        return result;
    }

    @AfterEach
    public void stopZks() {
        for (FakeZk zk : zks) {
            zk.stop();
        }
    }

    Secret dummySecret() {
        return new Secret();
    }

    BackOff backoff() {
        return new BackOff(50, 2, MAX_ATTEMPTS);
    }

    static Set<String> treeSet(String value1, String value2)   {
        Set<String> treeSet = new TreeSet<>();
        treeSet.add(value1);
        treeSet.add(value2);

        return treeSet;
    }

    @Test
    public void test0PodsClusterReturnsUnknowLeader(VertxTestContext context) {
        ZookeeperLeaderFinder finder = new ZookeeperLeaderFinder(vertx, this::backoff);
        Checkpoint a = context.checkpoint();
        finder.findZookeeperLeader(Reconciliation.DUMMY_RECONCILIATION, emptySet(), dummySecret(), dummySecret())
            .onComplete(context.succeeding(leader -> {
                context.verify(() -> assertThat(leader, is(ZookeeperLeaderFinder.UNKNOWN_LEADER)));
                a.flag();
            }));
    }

    @Test
    public void test1PodClusterReturnsOnlyPodAsLeader(VertxTestContext context) {
        ZookeeperLeaderFinder finder = new ZookeeperLeaderFinder(vertx, this::backoff);
        Checkpoint a = context.checkpoint();
        int firstPodIndex = 0;
        finder.findZookeeperLeader(Reconciliation.DUMMY_RECONCILIATION, Set.of(createPodWithId(firstPodIndex)), dummySecret(), dummySecret())
            .onComplete(context.succeeding(leader -> {
                context.verify(() -> assertThat(leader, is("my-cluster-kafka-0")));
                a.flag();
            }));
    }

    @Test
    public void testSecretWithMissingClusterOperatorKeyThrowsException(VertxTestContext context) {
        SecretOperator mock = mock(SecretOperator.class);
        ZookeeperLeaderFinder finder = new ZookeeperLeaderFinder(vertx, this::backoff);

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

        Secret secretWithMissingClusterOperatorKey = new SecretBuilder()
                .withNewMetadata()
                .withName(ClusterOperator.secretName(CLUSTER))
                .withNamespace(NAMESPACE)
                .endMetadata()
                .withData(emptyMap())
                .build();

        Checkpoint a = context.checkpoint();

        finder.findZookeeperLeader(Reconciliation.DUMMY_RECONCILIATION, treeSet(createPodWithId(0), createPodWithId(1)), dummySecret(), secretWithMissingClusterOperatorKey)
            .onComplete(context.failing(e -> context.verify(() -> {
                assertThat(e, instanceOf(RuntimeException.class));
                assertThat(e.getMessage(),
                        is("The Secret testns/testcluster-cluster-operator-certs is missing the key cluster-operator.key"));
                a.flag();
            })));

    }

    @Test
    public void testSecretsCorrupted(VertxTestContext context) {
        SecretOperator mock = mock(SecretOperator.class);
        ZookeeperLeaderFinder finder = new ZookeeperLeaderFinder(vertx, this::backoff);

        when(mock.getAsync(eq(NAMESPACE), eq(KafkaResources.clusterCaCertificateSecretName(CLUSTER))))
                .thenReturn(Future.succeededFuture(
                        new SecretBuilder()
                                .withNewMetadata()
                                .withName(KafkaResources.clusterCaCertificateSecretName(CLUSTER))
                                .withNamespace(NAMESPACE)
                                .endMetadata()
                                .withData(map(Ca.CA_CRT, "notacert"))
                                .build()));

        Secret secretWithBadCertificate = new SecretBuilder()
                .withNewMetadata()
                .withName(ClusterOperator.secretName(CLUSTER))
                .withNamespace(NAMESPACE)
                .endMetadata()
                .withData(map("cluster-operator.key", "notacert",
                        "cluster-operator.crt", "notacert",
                        "cluster-operator.p12", "notatruststore",
                        "cluster-operator.password", "notapassword"))
                .build();

        Checkpoint a = context.checkpoint();

        finder.findZookeeperLeader(Reconciliation.DUMMY_RECONCILIATION, treeSet(createPodWithId(0), createPodWithId(1)), dummySecret(), secretWithBadCertificate)
                .onComplete(context.failing(e -> context.verify(() -> {
                    assertThat(e, instanceOf(RuntimeException.class));
                    assertThat(e.getMessage(), is("Bad/corrupt certificate found in data.cluster-operator\\.crt of Secret testcluster-cluster-operator-certs in namespace testns"));
                    a.flag();
                })));

    }

    @Test
    public void testReturnUnknownLeaderWhenMaxAttemptsExceeded(VertxTestContext context) throws InterruptedException {
        int[] ports = startMockZks(context, 2, (id, attempt) -> false);

        ZookeeperLeaderFinder finder = new TestingZookeeperLeaderFinder(this::backoff, ports);

        Checkpoint a = context.checkpoint();
        finder.findZookeeperLeader(Reconciliation.DUMMY_RECONCILIATION, treeSet(createPodWithId(0), createPodWithId(1)), dummySecret(), dummySecret())
            .onComplete(context.succeeding(leader -> context.verify(() -> {
                assertThat(leader, is(ZookeeperLeaderFinder.UNKNOWN_LEADER));
                for (FakeZk zk : zks) {
                    assertThat("Unexpected number of attempts for node " + zk.id, zk.attempts.get(), is(MAX_ATTEMPTS + 1));
                }
                a.flag();
            })));
    }

    @Test
    public void testReturnUnknownLeaderDuringNetworkExceptions(VertxTestContext context) throws InterruptedException {
        int[] ports = startMockZks(context, 2, (id, attempt) -> false);
        // Close ports to ensure closed ports are used so as to mock network problems
        stopZks();

        ZookeeperLeaderFinder finder = new TestingZookeeperLeaderFinder(this::backoff, ports);

        Checkpoint a = context.checkpoint();
        finder.findZookeeperLeader(Reconciliation.DUMMY_RECONCILIATION, treeSet(createPodWithId(0), createPodWithId(1)), dummySecret(), dummySecret())
            .onComplete(context.succeeding(leader -> context.verify(() -> {
                assertThat(leader, is(ZookeeperLeaderFinder.UNKNOWN_LEADER));
                for (FakeZk zk : zks) {
                    assertThat("Unexpected number of attempts for node " + zk.id, zk.attempts.get(), is(0));
                }
                a.flag();
            })));
    }

    @Test
    public void testFinderHandlesFailureByLeaderFoundOnThirdAttempt(VertxTestContext context) throws InterruptedException {
        int desiredLeaderId = 1;
        String leaderPod = "my-cluster-kafka-1";
        int succeedOnAttempt = 2;

        int[] ports = startMockZks(context, 2, (id, attempt) -> attempt == succeedOnAttempt && id == desiredLeaderId);

        TestingZookeeperLeaderFinder finder = new TestingZookeeperLeaderFinder(this::backoff, ports);

        Checkpoint a = context.checkpoint();
        finder.findZookeeperLeader(Reconciliation.DUMMY_RECONCILIATION, treeSet(createPodWithId(0), createPodWithId(1)), dummySecret(), dummySecret())
            .onComplete(context.succeeding(leader -> context.verify(() -> {
                assertThat(leader, is(leaderPod));
                for (FakeZk zk : zks) {
                    assertThat("Unexpected number of attempts for node " + zk.id, zk.attempts.get(), is(succeedOnAttempt + 1));
                }
                a.flag();
            })));
    }

    @Test
    public void testLeaderFoundFirstAttempt(VertxTestContext context) throws InterruptedException {
        int leader = 1;
        String leaderPod = "my-cluster-kafka-1";

        int[] ports = startMockZks(context, 2, (id, attempt) -> id == leader);

        ZookeeperLeaderFinder finder = new TestingZookeeperLeaderFinder(this::backoff, ports);

        Checkpoint a = context.checkpoint();
        finder.findZookeeperLeader(Reconciliation.DUMMY_RECONCILIATION, treeSet(createPodWithId(0), createPodWithId(1)), dummySecret(), dummySecret())
            .onComplete(context.succeeding(l -> context.verify(() -> {
                assertThat(l, is(leaderPod));
                for (FakeZk zk : zks) {
                    assertThat("Unexpected number of attempts for node " + zk.id, zk.attempts.get(), is(1));
                }
                a.flag();
            })));
    }

    String createPodWithId(int id) {
        return "my-cluster-kafka-" + id;
    }

    @Test
    public void testGetHostReturnsCorrectHostForGivenPod() {
        Pod pod = new PodBuilder()
                .withNewMetadata()
                    .withName(KafkaResources.zookeeperPodName("my-cluster", 3))
                    .withNamespace("myproject")
                    .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, "my-cluster")
                .endMetadata()
            .build();

        assertThat(new ZookeeperLeaderFinder(vertx, this::backoff).host(new Reconciliation("test", "Kafka", "myproject", "my-cluster"), KafkaResources.zookeeperPodName("my-cluster", 3)),
                is("my-cluster-zookeeper-3.my-cluster-zookeeper-nodes.myproject.svc.cluster.local"));
    }
}
