/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;

import io.strimzi.controller.cluster.model.AssemblyType;
import io.strimzi.controller.cluster.model.Labels;
import io.strimzi.controller.cluster.operator.assembly.AbstractAssemblyOperator;
import io.strimzi.controller.cluster.operator.assembly.KafkaAssemblyOperator;
import io.strimzi.controller.cluster.operator.assembly.KafkaConnectAssemblyOperator;
import io.strimzi.controller.cluster.operator.assembly.KafkaConnectS2IAssemblyOperator;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * An "operator" for managing assemblies of various types <em>in a particular namespace</em>.
 * The Cluster Controller's multiple namespace support is achieved by deploying multiple
 * {@link ClusterController}'s in Vertx.
 */
public class ClusterController extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(ClusterController.class.getName());

    public static final String STRIMZI_CLUSTER_CONTROLLER_DOMAIN = "cluster.controller.strimzi.io";
    public static final String STRIMZI_CLUSTER_CONTROLLER_SERVICE_ACCOUNT = "strimzi-cluster-controller";

    private static final int HEALTH_SERVER_PORT = 8080;

    private final KubernetesClient client;
    private final Labels selector;
    private final String namespace;
    private final long reconciliationInterval;

    private volatile Watch configMapWatch;

    private long reconcileTimer;
    private final KafkaAssemblyOperator kafkaAssemblyOperator;
    private final KafkaConnectAssemblyOperator kafkaConnectAssemblyOperator;
    private final KafkaConnectS2IAssemblyOperator kafkaConnectS2IAssemblyOperator;

    public ClusterController(String namespace,
                             long reconciliationInterval,
                             KubernetesClient client,
                             KafkaAssemblyOperator kafkaAssemblyOperator,
                             KafkaConnectAssemblyOperator kafkaConnectAssemblyOperator,
                             KafkaConnectS2IAssemblyOperator kafkaConnectS2IAssemblyOperator) {
        log.info("Creating ClusterController for namespace {}", namespace);
        this.namespace = namespace;
        this.selector = Labels.forKind("cluster");
        this.reconciliationInterval = reconciliationInterval;
        this.client = client;
        this.kafkaAssemblyOperator = kafkaAssemblyOperator;
        this.kafkaConnectAssemblyOperator = kafkaConnectAssemblyOperator;
        this.kafkaConnectS2IAssemblyOperator = kafkaConnectS2IAssemblyOperator;
    }

    @Override
    public void start(Future<Void> start) {
        log.info("Starting ClusterController for namespace {}", namespace);

        // Configure the executor here, but it is used only in other places
        getVertx().createSharedWorkerExecutor("kubernetes-ops-pool", 10, TimeUnit.SECONDS.toNanos(120));

        createConfigMapWatch(res -> {
            if (res.succeeded())    {
                configMapWatch = res.result();

                log.info("Setting up periodical reconciliation for namespace {}", namespace);
                this.reconcileTimer = vertx.setPeriodic(this.reconciliationInterval, res2 -> {
                    log.info("Triggering periodic reconciliation for namespace {}...", namespace);
                    reconcileAll("timer");
                });

                log.info("ClusterController running for namespace {}", namespace);

                // start the HTTP server for healthchecks
                this.startHealthServer();

                start.complete();
            } else {
                log.error("ClusterController startup failed for namespace {}", namespace, res.cause());
                start.fail("ClusterController startup failed for namespace " + namespace);
            }
        });
    }

    @Override
    public void stop(Future<Void> stop) {
        log.info("Stopping ClusterController for namespace {}", namespace);
        vertx.cancelTimer(reconcileTimer);
        configMapWatch.close();
        client.close();

        stop.complete();
    }

    private void createConfigMapWatch(Handler<AsyncResult<Watch>> handler) {
        getVertx().executeBlocking(
            future -> {
                Watch watch = client.configMaps().inNamespace(namespace).withLabels(selector.toMap()).watch(new Watcher<ConfigMap>() {
                    @Override
                    public void eventReceived(Action action, ConfigMap cm) {
                        Labels labels = Labels.fromResource(cm);
                        AssemblyType type;
                        try {
                            type = labels.type();
                        } catch (IllegalArgumentException e) {
                            log.warn("Unknown {} label {} received in Config Map {} in namespace {}",
                                    Labels.STRIMZI_TYPE_LABEL,
                                    cm.getMetadata().getLabels().get(Labels.STRIMZI_TYPE_LABEL),
                                    cm.getMetadata().getName(), namespace);
                            return;
                        }

                        final AbstractAssemblyOperator cluster;
                        if (type == null) {
                            log.warn("Missing label {} in Config Map {} in namespace {}", Labels.STRIMZI_TYPE_LABEL, cm.getMetadata().getName(), namespace);
                            return;
                        } else {
                            switch (type) {
                                case KAFKA:
                                    cluster = kafkaAssemblyOperator;
                                    break;
                                case CONNECT:
                                    cluster = kafkaConnectAssemblyOperator;
                                    break;
                                case CONNECT_S2I:
                                    cluster = kafkaConnectS2IAssemblyOperator;
                                    break;
                                default:
                                    return;
                            }
                        }
                        String name = cm.getMetadata().getName();
                        switch (action) {
                            case ADDED:
                            case DELETED:
                            case MODIFIED:
                                Reconciliation reconciliation = new Reconciliation("watch", type, namespace, name);
                                log.info("{}: ConfigMap {} in namespace {} was {}", reconciliation, name, namespace, action);
                                cluster.reconcileAssembly(reconciliation, result -> {
                                    if (result.succeeded()) {
                                        log.info("{}: assembly reconciled", reconciliation);
                                    } else {
                                        log.error("{}: Failed to reconcile", reconciliation, result.cause());
                                    }
                                });
                                break;
                            case ERROR:
                                log.error("Failed ConfigMap {} in namespace{} ", name, namespace);
                                reconcileAll("watch error");
                                break;
                            default:
                                log.error("Unknown action: {} in namespace {}", name, namespace);
                                reconcileAll("watch unknown");
                        }
                    }

                    @Override
                    public void onClose(KubernetesClientException e) {
                        if (e != null) {
                            log.error("Watcher closed with exception in namespace {}", namespace, e);
                            recreateConfigMapWatch();
                        } else {
                            log.info("Watcher closed in namespace {}", namespace);
                        }
                    }
                });
                future.complete(watch);
            }, res -> {
                if (res.succeeded())    {
                    log.info("ConfigMap watcher running for labels {}", selector);
                    handler.handle(Future.succeededFuture((Watch) res.result()));
                } else {
                    log.info("ConfigMap watcher failed to start", res.cause());
                    handler.handle(Future.failedFuture("ConfigMap watcher failed to start"));
                }
            }
        );
    }

    private void recreateConfigMapWatch() {
        createConfigMapWatch(res -> {
            if (res.succeeded())    {
                log.info("ConfigMap watch recreated in namespace {}", namespace);
                configMapWatch = res.result();
            } else {
                log.error("Failed to recreate ConfigMap watch in namespace {}", namespace);
                // We failed to recreate the Watch. We cannot continue without it. Lets close Vert.x and exit.
                vertx.close();
            }
        });
    }

    /**
      Periodical reconciliation (in case we lost some event)
     */
    private void reconcileAll(String trigger) {
        kafkaAssemblyOperator.reconcileAll(trigger, namespace, selector);
        kafkaConnectAssemblyOperator.reconcileAll(trigger, namespace, selector);

        if (kafkaConnectS2IAssemblyOperator != null) {
            kafkaConnectS2IAssemblyOperator.reconcileAll(trigger, namespace, selector);
        }
    }

    /**
     * Start an HTTP health server
     */
    private void startHealthServer() {

        this.vertx.createHttpServer()
                .requestHandler(request -> {

                    if (request.path().equals("/healthy")) {
                        request.response().setStatusCode(200).end();
                    } else if (request.path().equals("/ready")) {
                        request.response().setStatusCode(200).end();
                    }
                })
                .listen(HEALTH_SERVER_PORT);
    }

}
