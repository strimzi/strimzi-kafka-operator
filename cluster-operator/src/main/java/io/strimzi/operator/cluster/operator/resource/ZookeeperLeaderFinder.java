/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.cluster.model.DnsNameGenerator;
import io.strimzi.operator.cluster.model.ZookeeperCluster;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.auth.PemAuthIdentity;
import io.strimzi.operator.common.auth.PemTrustSet;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetSocket;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;

import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper class for finding the leader of a ZK cluster
 */
public class ZookeeperLeaderFinder {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ZookeeperLeaderFinder.class);

    private static final Pattern LEADER_MODE_PATTERN = Pattern.compile("^Mode: leader$", Pattern.MULTILINE);

    /**
     * Unknown leader marker
     */
    public static final String UNKNOWN_LEADER = "-1";

    private final Vertx vertx;
    private final Supplier<BackOff> backOffSupplier;

    /**
     * Constructor
     *
     * @param vertx             Vert.x instance
     * @param backOffSupplier   Backoff supplier
     */
    public ZookeeperLeaderFinder(Vertx vertx, Supplier<BackOff> backOffSupplier) {
        this.vertx = vertx;
        this.backOffSupplier = backOffSupplier;
    }

    /*test*/ NetClientOptions clientOptions(PemTrustSet zkCaTrustSet, PemAuthIdentity coAuthIdentity) {
        PemTrustOptions pto = new PemTrustOptions();
        zkCaTrustSet.trustedCertificatesBytes().forEach(certBytes -> pto.addCertValue(Buffer.buffer(certBytes)));
        PemKeyCertOptions pkco = new PemKeyCertOptions()
                .setCertValue(Buffer.buffer(coAuthIdentity.certificateChainAsPemBytes()))
                .setKeyValue(Buffer.buffer(coAuthIdentity.privateKeyAsPemBytes()));
        return new NetClientOptions()
                .setConnectTimeout(10_000)
                .setSsl(true)
                .setHostnameVerificationAlgorithm("HTTPS")
                .setKeyCertOptions(pkco)
                .setTrustOptions(pto);
    }

    /**
     * Returns a Future which completes with the id of the Zookeeper leader.
     * An exponential backoff is used if no ZK node is leader on the attempt to find it.
     * If there is no leader after 3 attempts then the returned Future completes with {@link #UNKNOWN_LEADER}.
     */
    Future<String> findZookeeperLeader(Reconciliation reconciliation, Set<String> pods, TlsPemIdentity coTlsPemIdentity) {
        if (pods.size() == 0) {
            return Future.succeededFuture(UNKNOWN_LEADER);
        } else if (pods.size() == 1) {
            return Future.succeededFuture(pods.stream().findFirst().get());
        }

        try {
            NetClientOptions netClientOptions = clientOptions(coTlsPemIdentity.pemTrustSet(), coTlsPemIdentity.pemAuthIdentity());
            return zookeeperLeaderWithBackoff(reconciliation, pods, netClientOptions);
        } catch (Throwable e) {
            return Future.failedFuture(e);
        }
    }

    private Future<String> zookeeperLeaderWithBackoff(Reconciliation reconciliation, Set<String> pods, NetClientOptions netClientOptions) {
        Promise<String> result = Promise.promise();
        BackOff backOff = backOffSupplier.get();
        Handler<Long> handler = new Handler<Long>() {
            @Override
            public void handle(Long tid) {
                zookeeperLeader(reconciliation, pods, netClientOptions).onComplete(leader -> {
                    if (leader.succeeded()) {
                        if (!UNKNOWN_LEADER.equals(leader.result())) {
                            result.complete(leader.result());
                        } else {
                            rescheduleOrComplete(reconciliation, tid);
                        }
                    } else {
                        LOGGER.debugOp("Ignoring error", leader.cause());
                        if (backOff.done()) {
                            result.complete(UNKNOWN_LEADER);
                        } else {
                            rescheduleOrComplete(reconciliation, tid);
                        }
                    }
                });
            }

            void rescheduleOrComplete(Reconciliation reconciliation, Long tid) {
                if (backOff.done()) {
                    LOGGER.warnCr(reconciliation, "Giving up trying to find the leader of {}/{} after {} attempts taking {}ms",
                            reconciliation.name(), reconciliation.namespace(), backOff.maxAttempts(), backOff.totalDelayMs());
                    result.complete(UNKNOWN_LEADER);
                } else {
                    // Schedule ourselves to run again
                    long delay = backOff.delayMs();
                    LOGGER.infoCr(reconciliation, "No leader found for cluster {} in namespace {}; " +
                                    "backing off for {}ms (cumulative {}ms)",
                            reconciliation.name(), reconciliation.namespace(), delay, backOff.cumulativeDelayMs());
                    if (delay < 1) {
                        this.handle(tid);
                    } else {
                        vertx.setTimer(delay, this);
                    }
                }
            }
        };
        handler.handle(null);
        return result.future();
    }

    /**
     * Synchronously find the leader by testing each pod in the given list
     * using {@link #isLeader(Reconciliation, String, NetClientOptions)}.
     */
    private Future<String> zookeeperLeader(Reconciliation reconciliation, Set<String> pods, NetClientOptions netClientOptions) {
        try {
            Future<String> f = Future.succeededFuture(UNKNOWN_LEADER);

            for (String podName : pods) {
                f = f.compose(leader -> {
                    if (UNKNOWN_LEADER.equals(leader)) {
                        LOGGER.debugCr(reconciliation, "Checker whether {} is leader", podName);
                        return isLeader(reconciliation, podName, netClientOptions).map(isLeader -> {
                            if (isLeader != null && isLeader) {
                                LOGGER.infoCr(reconciliation, "Pod {} is leader", podName);
                                return podName;
                            } else {
                                LOGGER.infoCr(reconciliation, "Pod {} is not a leader", podName);
                                return UNKNOWN_LEADER;
                            }
                        });
                    } else {
                        return Future.succeededFuture(leader);
                    }
                });
            }

            return f;
        } catch (Throwable t) {
            return Future.failedFuture(t);
        }
    }

    /**
     * Returns whether the given pod is the zookeeper leader.
     */
    protected Future<Boolean> isLeader(Reconciliation reconciliation, String podName, NetClientOptions netClientOptions) {

        Promise<Boolean> promise = Promise.promise();
        String host = host(reconciliation, podName);
        int port = port(podName);
        LOGGER.debugCr(reconciliation, "Connecting to zookeeper on {}:{}", host, port);
        vertx.createNetClient(netClientOptions)
            .connect(port, host, ar -> {
                if (ar.failed()) {
                    LOGGER.warnCr(reconciliation, "ZK {}:{}: failed to connect to zookeeper:", host, port, ar.cause().getMessage());
                    promise.fail(ar.cause());
                } else {
                    LOGGER.debugCr(reconciliation, "ZK {}:{}: connected", host, port);
                    NetSocket socket = ar.result();
                    socket.exceptionHandler(ex -> {
                        if (!promise.tryFail(ex)) {
                            LOGGER.debugCr(reconciliation, "ZK {}:{}: Ignoring error, since leader status of pod {} is already known: {}",
                                    host, port, podName, ex);
                        }
                    });
                    StringBuilder sb = new StringBuilder();
                    // We could use socket idle timeout, but this times out even if the server just responds
                    // very slowly
                    long timerId = vertx.setTimer(10_000, tid -> {
                        LOGGER.debugCr(reconciliation, "ZK {}:{}: Timeout waiting for Zookeeper {} to close socket",
                                host, port, socket.remoteAddress());
                        socket.close();
                    });
                    socket.closeHandler(v -> {
                        vertx.cancelTimer(timerId);
                        Matcher matcher = LEADER_MODE_PATTERN.matcher(sb);
                        boolean isLeader = matcher.find();
                        LOGGER.debugCr(reconciliation, "ZK {}:{}: {} leader", host, port, isLeader ? "is" : "is not");
                        if (!promise.tryComplete(isLeader)) {
                            LOGGER.debugCr(reconciliation, "ZK {}:{}: Ignoring leader result: Future is already complete",
                                    host, port);
                        }
                    });
                    LOGGER.debugCr(reconciliation, "ZK {}:{}: upgrading to TLS", host, port);
                    socket.handler(buffer -> {
                        LOGGER.traceCr(reconciliation, "buffer: {}", buffer);
                        sb.append(buffer.toString());
                    });
                    LOGGER.debugCr(reconciliation, "ZK {}:{}: sending stat", host, port);
                    socket.write("stat");
                }

            });

        return promise.future().recover(error -> {
            LOGGER.debugOp("ZK {}:{}: Error trying to determine leader ({}) => not leader", host, port, error);
            return Future.succeededFuture(Boolean.FALSE);
        });
    }

    /**
     * The hostname for connecting to zookeeper in the given pod.
     *
     * @param reconciliation    Reconciliation used to obtain the namespace and cluster name
     * @param podName           Name of the pod for which the hostname should be constructed
     *
     * @return                  Hostname of the ZooKeeper node
     */
    protected String host(Reconciliation reconciliation, String podName) {
        return DnsNameGenerator.podDnsName(reconciliation.namespace(), KafkaResources.zookeeperHeadlessServiceName(reconciliation.name()), podName);
    }

    /**
     * The port number for connecting to zookeeper in the given pod.
     *
     * @param podName   Name of the pod for which we want to get the port number (not used here, but used in tests)
     *
     * @return          Port number of the ZooKeeper node
     */
    protected int port(String podName) {
        return ZookeeperCluster.CLIENT_TLS_PORT;
    }
}
