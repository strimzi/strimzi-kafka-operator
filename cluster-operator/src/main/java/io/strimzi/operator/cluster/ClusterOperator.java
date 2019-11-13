/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.strimzi.operator.cluster.operator.assembly.AbstractConnectOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaBridgeAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaConnectAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaConnectS2IAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaMirrorMakerAssemblyOperator;
import io.strimzi.operator.common.AbstractOperator;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;

/**
 * An "operator" for managing assemblies of various types <em>in a particular namespace</em>.
 * The Cluster Operator's multiple namespace support is achieved by deploying multiple
 * {@link ClusterOperator}'s in Vertx.
 */
public class ClusterOperator extends AbstractVerticle {

    private static final Logger log = LogManager.getLogger(ClusterOperator.class.getName());

    public static final String STRIMZI_CLUSTER_OPERATOR_DOMAIN = "cluster.operator.strimzi.io";
    private static final String NAME_SUFFIX = "-cluster-operator";
    private static final String CERTS_SUFFIX = NAME_SUFFIX + "-certs";

    private static final int HEALTH_SERVER_PORT = 8080;

    private final KubernetesClient client;
    private final String namespace;
    private final long reconciliationInterval;

    private final Map<String, Watch> watchByKind = new ConcurrentHashMap<>();

    private long reconcileTimer;
    private final KafkaAssemblyOperator kafkaAssemblyOperator;
    private final KafkaConnectAssemblyOperator kafkaConnectAssemblyOperator;
    private final KafkaConnectS2IAssemblyOperator kafkaConnectS2IAssemblyOperator;
    private final KafkaMirrorMakerAssemblyOperator kafkaMirrorMakerAssemblyOperator;
    private final KafkaBridgeAssemblyOperator kafkaBridgeAssemblyOperator;

    public ClusterOperator(String namespace,
                           long reconciliationInterval,
                           KubernetesClient client,
                           KafkaAssemblyOperator kafkaAssemblyOperator,
                           KafkaConnectAssemblyOperator kafkaConnectAssemblyOperator,
                           KafkaConnectS2IAssemblyOperator kafkaConnectS2IAssemblyOperator,
                           KafkaMirrorMakerAssemblyOperator kafkaMirrorMakerAssemblyOperator,
                           KafkaBridgeAssemblyOperator kafkaBridgeAssemblyOperator) {
        log.info("Creating ClusterOperator for namespace {}", namespace);
        this.namespace = namespace;
        this.reconciliationInterval = reconciliationInterval;
        this.client = client;
        this.kafkaAssemblyOperator = kafkaAssemblyOperator;
        this.kafkaConnectAssemblyOperator = kafkaConnectAssemblyOperator;
        this.kafkaConnectS2IAssemblyOperator = kafkaConnectS2IAssemblyOperator;
        this.kafkaMirrorMakerAssemblyOperator = kafkaMirrorMakerAssemblyOperator;
        this.kafkaBridgeAssemblyOperator = kafkaBridgeAssemblyOperator;
    }

    @Override
    public void start(Future<Void> start) {
        log.info("Starting ClusterOperator for namespace {}", namespace);

        // Configure the executor here, but it is used only in other places
        getVertx().createSharedWorkerExecutor("kubernetes-ops-pool", 10, TimeUnit.SECONDS.toNanos(120));

        List<Future> watchFutures = new ArrayList<>();
        List<AbstractOperator<?, ?>> operators = new ArrayList<>(asList(
                kafkaAssemblyOperator, kafkaMirrorMakerAssemblyOperator,
                kafkaConnectAssemblyOperator, kafkaBridgeAssemblyOperator));
        if (kafkaConnectS2IAssemblyOperator != null) {
            operators.add(kafkaConnectS2IAssemblyOperator);
        }
        for (AbstractOperator<?, ?> operator : operators) {
            watchFutures.add(operator.createWatch(namespace, operator.recreateWatch(namespace)).compose(w -> {
                log.info("Opened watch for {} operator", operator.kind());
                watchByKind.put(operator.kind(), w);
                return Future.succeededFuture();
            }));
        }

        watchFutures.add(AbstractConnectOperator.createConnectorWatch(kafkaConnectAssemblyOperator, kafkaConnectS2IAssemblyOperator, namespace));

        CompositeFuture.join(watchFutures)
                .compose(f -> {
                    log.info("Setting up periodic reconciliation for namespace {}", namespace);
                    this.reconcileTimer = vertx.setPeriodic(this.reconciliationInterval, res2 -> {
                        log.info("Triggering periodic reconciliation for namespace {}...", namespace);
                        reconcileAll("timer");
                    });
                    return startHealthServer().map((Void) null);
                })
                .compose(start::complete, start);
    }


    @Override
    public void stop(Future<Void> stop) {
        log.info("Stopping ClusterOperator for namespace {}", namespace);
        vertx.cancelTimer(reconcileTimer);
        for (Watch watch : watchByKind.values()) {
            if (watch != null) {
                watch.close();
            }
            // TODO remove the watch from the watchByKind
        }
        client.close();

        stop.complete();
    }

    /**
      Periodical reconciliation (in case we lost some event)
     */
    private void reconcileAll(String trigger) {
        Handler<AsyncResult<Void>> ignore = ignored -> { };
        kafkaAssemblyOperator.reconcileAll(trigger, namespace, ignore);
        kafkaMirrorMakerAssemblyOperator.reconcileAll(trigger, namespace, ignore);
        kafkaConnectAssemblyOperator.reconcileAll(trigger, namespace, ignore);
        kafkaBridgeAssemblyOperator.reconcileAll(trigger, namespace, ignore);

        if (kafkaConnectS2IAssemblyOperator != null) {
            kafkaConnectS2IAssemblyOperator.reconcileAll(trigger, namespace, ignore);
        }
    }

    /**
     * Start an HTTP health server
     */
    private Future<HttpServer> startHealthServer() {
        Future<HttpServer> result = Future.future();
        this.vertx.createHttpServer()
                .requestHandler(request -> {

                    if (request.path().equals("/healthy")) {
                        request.response().setStatusCode(200).end();
                    } else if (request.path().equals("/ready")) {
                        request.response().setStatusCode(200).end();
                    }
                })
                .listen(HEALTH_SERVER_PORT, ar -> {
                    if (ar.succeeded()) {
                        log.info("ClusterOperator is now ready (health server listening on {})", HEALTH_SERVER_PORT);
                    } else {
                        log.error("Unable to bind health server on {}", HEALTH_SERVER_PORT, ar.cause());
                    }
                    result.handle(ar);
                });
        return result;
    }

    public static String secretName(String cluster) {
        return cluster + CERTS_SUFFIX;
    }
}
