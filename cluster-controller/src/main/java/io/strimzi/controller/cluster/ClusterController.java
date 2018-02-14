/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
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
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ClusterController extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(ClusterController.class.getName());

    public static final String STRIMZI_DOMAIN = "strimzi.io";
    public static final String STRIMZI_CLUSTER_CONTROLLER_DOMAIN = "cluster.controller.strimzi.io";
    public static final String STRIMZI_KIND_LABEL = STRIMZI_DOMAIN + "/kind";
    public static final String STRIMZI_TYPE_LABEL = STRIMZI_DOMAIN + "/type";
    public static final String STRIMZI_CLUSTER_LABEL = STRIMZI_DOMAIN + "/cluster";
    public static final String STRIMZI_NAME_LABEL = STRIMZI_DOMAIN + "/name";

    private static final int HEALTH_SERVER_PORT = 8080;

    private final KubernetesClient client;
    private final Map<String, String> labels;
    private final String namespace;
    private final long reconciliationInterval;

    private Watch configMapWatch;

    private long reconcileTimer;
    private final KafkaClusterOperations kafkaClusterOperations;
    private final KafkaConnectClusterOperations kafkaConnectClusterOperations;
    private final KafkaConnectS2IClusterOperations kafkaConnectS2IClusterOperations;

    public ClusterController(ClusterControllerConfig config,
                             KafkaClusterOperations kafkaClusterOperations,
                             KafkaConnectClusterOperations kafkaConnectClusterOperations,
                             KafkaConnectS2IClusterOperations kafkaConnectS2IClusterOperations) {
        log.info("Creating ClusterController");
        this.namespace = config.getNamespace();
        this.labels = config.getLabels();
        this.reconciliationInterval = config.getReconciliationInterval();
        this.client = new DefaultKubernetesClient();
        this.kafkaClusterOperations = kafkaClusterOperations;
        this.kafkaConnectClusterOperations = kafkaConnectClusterOperations;
        this.kafkaConnectS2IClusterOperations = kafkaConnectS2IClusterOperations;
    }

    @Override
    public void start(Future<Void> start) {
        log.info("Starting ClusterController");

        // Configure the executor here, but it is used only in other places
        getVertx().createSharedWorkerExecutor("kubernetes-ops-pool", 10, TimeUnit.SECONDS.toNanos(120));

        createConfigMapWatch(res -> {
            if (res.succeeded())    {
                configMapWatch = res.result();

                log.info("Setting up periodical reconciliation");
                this.reconcileTimer = vertx.setPeriodic(this.reconciliationInterval, res2 -> {
                    log.info("Triggering periodic reconciliation ...");
                    reconcile();
                });

                log.info("ClusterController up and running");

                // start the HTTP server for healthchecks
                this.startHealthServer();

                start.complete();
            }
            else {
                log.error("ClusterController startup failed");
                start.fail("ClusterController startup failed");
            }
        });
    }

    @Override
    public void stop(Future<Void> stop) throws Exception {

        vertx.cancelTimer(reconcileTimer);
        configMapWatch.close();
        client.close();

        stop.complete();
    }

    private void createConfigMapWatch(Handler<AsyncResult<Watch>> handler) {
        getVertx().executeBlocking(
                future -> {
                    Watch watch = client.configMaps().inNamespace(namespace).withLabels(labels).watch(new Watcher<ConfigMap>() {
                        @Override
                        public void eventReceived(Action action, ConfigMap cm) {
                            Map<String, String> labels = cm.getMetadata().getLabels();
                            String type = labels.get(ClusterController.STRIMZI_TYPE_LABEL);

                            final AbstractClusterOperations<?> cluster;
                            if (type == null) {
                                log.warn("Missing type in Config Map {}", cm.getMetadata().getName());
                                return;
                            } else if (type.equals(KafkaCluster.TYPE)) {
                                cluster = kafkaClusterOperations;
                            } else if (type.equals(KafkaConnectCluster.TYPE)) {
                                cluster = kafkaConnectClusterOperations;
                            } else if (type.equals(KafkaConnectS2ICluster.TYPE)) {
                                cluster = kafkaConnectS2IClusterOperations;
                            } else {
                                log.warn("Unknown type {} received in Config Map {}", labels.get(ClusterController.STRIMZI_TYPE_LABEL), cm.getMetadata().getName());
                                return;
                            }
                            String name = cm.getMetadata().getName();
                            switch (action) {
                                case ADDED:
                                    log.info("New ConfigMap {}", name);
                                    cluster.create(namespace, name);
                                    break;
                                case DELETED:
                                    log.info("Deleted ConfigMap {}", name);
                                    cluster.delete(namespace, name);
                                    break;
                                case MODIFIED:
                                    log.info("Modified ConfigMap {}", name);
                                    cluster.update(namespace, name);
                                    break;
                                case ERROR:
                                    log.error("Failed ConfigMap {}", name);
                                    reconcile();
                                    break;
                                default:
                                    log.error("Unknown action: {}", name);
                                    reconcile();
                            }
                        }

                        @Override
                        public void onClose(KubernetesClientException e) {
                            if (e != null) {
                                log.error("Watcher closed with exception", e);
                            }
                            else {
                                log.error("Watcher closed");
                            }

                            recreateConfigMapWatch();
                        }
                    });
                    future.complete(watch);
                }, res -> {
                    if (res.succeeded())    {
                        log.info("ConfigMap watcher up and running for labels {}", labels);
                        handler.handle(Future.succeededFuture((Watch)res.result()));
                    }
                    else {
                        log.info("ConfigMap watcher failed to start");
                        handler.handle(Future.failedFuture("ConfigMap watcher failed to start"));
                    }
                }
        );
    }

    private void recreateConfigMapWatch() {
        configMapWatch.close();

        createConfigMapWatch(res -> {
            if (res.succeeded())    {
                log.info("ConfigMap watch recreated");
                configMapWatch = res.result();
            }
            else {
                log.error("Failed to recreate ConfigMap watch");
            }
        });
    }

    /*
      Periodical reconciliation (in case we lost some event)
     */
    private void reconcile() {
        kafkaClusterOperations.reconcile(namespace, labels);
        kafkaConnectClusterOperations.reconcile(namespace, labels);
        kafkaConnectS2IClusterOperations.reconcile(namespace, labels);
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
