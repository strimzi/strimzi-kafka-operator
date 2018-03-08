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
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.controller.cluster.operations.cluster.AbstractClusterOperations;
import io.strimzi.controller.cluster.operations.cluster.KafkaClusterOperations;
import io.strimzi.controller.cluster.operations.cluster.KafkaConnectClusterOperations;
import io.strimzi.controller.cluster.operations.cluster.KafkaConnectS2IClusterOperations;
import io.strimzi.controller.cluster.resources.KafkaCluster;
import io.strimzi.controller.cluster.resources.KafkaConnectCluster;
import io.strimzi.controller.cluster.resources.KafkaConnectS2ICluster;
import io.strimzi.controller.cluster.resources.Labels;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class ClusterController extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(ClusterController.class.getName());

    public static final String STRIMZI_CLUSTER_CONTROLLER_DOMAIN = "cluster.controller.strimzi.io";
    public static final String STRIMZI_CLUSTER_CONTROLLER_SERVICE_ACCOUNT = "strimzi-cluster-controller";

    private static final int HEALTH_SERVER_PORT = 8080;

    private final KubernetesClient client;
    private final Labels labels;
    private final String namespace;
    private final long reconciliationInterval;

    private Watch configMapWatch;

    private long reconcileTimer;
    private final KafkaClusterOperations kafkaClusterOperations;
    private final KafkaConnectClusterOperations kafkaConnectClusterOperations;
    private final KafkaConnectS2IClusterOperations kafkaConnectS2IClusterOperations;
    private boolean stopping;

    public ClusterController(String namespace,
                             long reconciliationInterval,
                             KubernetesClient client,
                             KafkaClusterOperations kafkaClusterOperations,
                             KafkaConnectClusterOperations kafkaConnectClusterOperations,
                             KafkaConnectS2IClusterOperations kafkaConnectS2IClusterOperations) {
        log.info("Creating ClusterController for namespace {}", namespace);
        this.namespace = namespace;
        this.labels = Labels.forKind("cluster");
        this.reconciliationInterval = reconciliationInterval;
        this.client = client;
        this.kafkaClusterOperations = kafkaClusterOperations;
        this.kafkaConnectClusterOperations = kafkaConnectClusterOperations;
        this.kafkaConnectS2IClusterOperations = kafkaConnectS2IClusterOperations;
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
                    reconcileAllClusters();
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
        stopping = true;
        log.info("Stopping ClusterController for namespace {}", namespace);
        vertx.cancelTimer(reconcileTimer);
        configMapWatch.close();
        client.close();

        stop.complete();
    }

    private void createConfigMapWatch(Handler<AsyncResult<Watch>> handler) {
        getVertx().executeBlocking(
            future -> {
                Watch watch = client.configMaps().inNamespace(namespace).withLabels(labels.toMap()).watch(new Watcher<ConfigMap>() {
                    @Override
                    public void eventReceived(Action action, ConfigMap cm) {
                        Labels labels = Labels.fromResource(cm);
                        String type = labels.type();

                        final AbstractClusterOperations<?, ?> cluster;
                        if (type == null) {
                            log.warn("Missing label {} in Config Map {} in namespace {}", Labels.STRIMZI_TYPE_LABEL, cm.getMetadata().getName(), namespace);
                            return;
                        } else if (type.equals(KafkaCluster.TYPE)) {
                            cluster = kafkaClusterOperations;
                        } else if (type.equals(KafkaConnectCluster.TYPE)) {
                            cluster = kafkaConnectClusterOperations;
                        } else if (type.equals(KafkaConnectS2ICluster.TYPE)) {
                            if (kafkaConnectS2IClusterOperations != null)   {
                                cluster = kafkaConnectS2IClusterOperations;
                            } else {
                                log.warn("Cluster type {} cannot be used outside of OpenShift as requested by Config Map {} in namespace {}", type, cm.getMetadata().getName(), namespace);
                                return;
                            }
                        } else {
                            log.warn("Unknown type {} received in Config Map {} in namespace {}", type, cm.getMetadata().getName(), namespace);
                            return;
                        }
                        String name = cm.getMetadata().getName();
                        switch (action) {
                            case ADDED:
                            case DELETED:
                            case MODIFIED:
                                log.info("ConfigMap {} in namespace {} was {}", name, namespace, action);
                                cluster.reconcileCluster(namespace, name);
                                break;
                            case ERROR:
                                log.error("Failed ConfigMap {} in namespace{} ", name, namespace);
                                reconcileAllClusters();
                                break;
                            default:
                                log.error("Unknown action: {} in namespace {}", name, namespace);
                                reconcileAllClusters();
                        }
                    }

                    @Override
                    public void onClose(KubernetesClientException e) {
                        if (e != null) {
                            log.error("Watcher closed with exception in namespace {}", namespace, e);
                        } else {
                            log.info("Watcher closed in namespace {}", namespace);
                        }

                        recreateConfigMapWatch();
                    }
                });
                future.complete(watch);
            }, res -> {
                if (res.succeeded())    {
                    log.info("ConfigMap watcher running for labels {}", labels);
                    handler.handle(Future.succeededFuture((Watch) res.result()));
                } else {
                    log.info("ConfigMap watcher failed to start", res.cause());
                    handler.handle(Future.failedFuture("ConfigMap watcher failed to start"));
                }
            }
        );
    }

    private void recreateConfigMapWatch() {
        if (stopping) {
            return;
        }
        configMapWatch.close();

        createConfigMapWatch(res -> {
            if (res.succeeded())    {
                log.info("ConfigMap watch recreated in namespace {}", namespace);
                configMapWatch = res.result();
            } else {
                log.error("Failed to recreate ConfigMap watch in namespace {}", namespace);
            }
        });
    }

    /**
      Periodical reconciliation (in case we lost some event)
     */
    private void reconcileAllClusters() {
        kafkaClusterOperations.reconcileAll(namespace, labels);
        kafkaConnectClusterOperations.reconcileAll(namespace, labels);

        if (kafkaConnectS2IClusterOperations != null) {
            kafkaConnectS2IClusterOperations.reconcileAll(namespace, labels);
        }
    }

    /**
     * Start an HTTP health server
     */
    private void startHealthServer() {

        this.vertx.createHttpServer()
                .requestHandler(request -> {

                    if (request.path().equals("/healthy")) {
                        request.response().setStatusCode(HttpResponseStatus.OK.code()).end();
                    } else if (request.path().equals("/ready")) {
                        request.response().setStatusCode(HttpResponseStatus.OK.code()).end();
                    }
                })
                .listen(HEALTH_SERVER_PORT);
    }

}
