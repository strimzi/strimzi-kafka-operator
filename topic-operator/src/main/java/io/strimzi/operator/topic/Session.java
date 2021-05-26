/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.apicurio.registry.utils.ConcurrentUtil;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.topic.zk.Zk;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.micrometer.backends.BackendRegistries;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.streams.StreamsConfig;

import java.security.Security;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class Session extends AbstractVerticle {

    private final static ReconciliationLogger LOGGER = ReconciliationLogger.create(Session.class);

    private static final int HEALTH_SERVER_PORT = 8080;


    private final Config config;
    private final KubernetesClient kubeClient;

    /*test*/ KafkaImpl kafka;
    private AdminClient adminClient;
    /*test*/ K8sImpl k8s;
    private KafkaStreamsTopicStoreService service; // if used
    /*test*/ TopicOperator topicOperator;
    /*test*/ Watch topicWatch;
    /*test*/ ZkTopicsWatcher topicsWatcher;
    /*test*/ TopicConfigsWatcher topicConfigsWatcher;
    /*test*/ ZkTopicWatcher topicWatcher;
    /*test*/ PrometheusMeterRegistry metricsRegistry;
    K8sTopicWatcher watcher;
    /** The id of the periodic reconciliation timer. This is null during a periodic reconciliation. */
    private volatile Long timerId;
    private volatile boolean stopped = false;
    private Zk zk;
    private volatile HttpServer healthServer;

    public Session(KubernetesClient kubeClient, Config config) {
        this.kubeClient = kubeClient;
        this.config = config;
        StringBuilder sb = new StringBuilder(System.lineSeparator());
        for (Config.Value<?> v: Config.keys()) {
            sb.append("\t").append(v.key).append(": ").append(Util.maskPassword(v.key, config.get(v).toString())).append(System.lineSeparator());
        }
        LOGGER.infoOp("Using config:{}", sb.toString());
        this.metricsRegistry = (PrometheusMeterRegistry) BackendRegistries.getDefaultNow();
    }

    /**
     * Stop the operator.
     */
    @Override
    public void stop(Promise<Void> stop) throws Exception {
        this.stopped = true;
        Long timerId = this.timerId;
        if (timerId != null) {
            vertx.cancelTimer(timerId);
        }
        vertx.executeBlocking(blockingResult -> {
            long timeout = 120_000L;
            long deadline = System.currentTimeMillis() + timeout;
            LOGGER.infoOp("Stopping");
            LOGGER.debugOp("Stopping kube watch");
            topicWatch.close();
            LOGGER.debugOp("Stopping zk watches");
            topicsWatcher.stop();

            Promise<Void> promise = Promise.promise();
            Handler<Long> longHandler = new Handler<>() {
                @Override
                public void handle(Long inflightTimerId) {
                    if (!topicOperator.isWorkInflight()) {
                        LOGGER.debugOp("Inflight work has finished");
                        promise.complete();
                    } else if (System.currentTimeMillis() > deadline) {
                        LOGGER.errorOp("Timeout waiting for inflight work to finish");
                        promise.complete();
                    } else {
                        LOGGER.debugOp("Waiting for inflight work to finish");
                        vertx.setTimer(1_000, this);
                    }
                }
            };
            longHandler.handle(null);

            promise.future().compose(ignored -> {
                if (service != null) {
                    service.stop();
                }
                return Future.succeededFuture();
            });

            promise.future().compose(ignored -> {

                LOGGER.debugOp("Disconnecting from zookeeper {}", zk);
                zk.disconnect(zkResult -> {
                    if (zkResult.failed()) {
                        LOGGER.warnOp("Error disconnecting from zookeeper: {}", String.valueOf(zkResult.cause()));
                    }
                    long timeoutMs = Math.max(1, deadline - System.currentTimeMillis());
                    LOGGER.debugOp("Closing AdminClient {} with timeout {}ms", adminClient, timeoutMs);
                    try {
                        adminClient.close(Duration.ofMillis(timeoutMs));
                        HttpServer healthServer = this.healthServer;
                        if (healthServer != null) {
                            healthServer.close();
                        }
                    } catch (TimeoutException e) {
                        LOGGER.warnOp("Timeout while closing AdminClient with timeout {}ms", timeoutMs, e);
                    } finally {
                        LOGGER.infoOp("Stopped");
                        blockingResult.complete();
                    }
                });
                return Future.succeededFuture();
            });
        }, stop);
    }

    @Override
    public void start(Promise<Void> start) {
        LOGGER.infoOp("Starting");
        Properties kafkaClientProps = new Properties();

        String dnsCacheTtl = System.getenv("STRIMZI_DNS_CACHE_TTL") == null ? "30" : System.getenv("STRIMZI_DNS_CACHE_TTL");
        Security.setProperty("networkaddress.cache.ttl", dnsCacheTtl);
        kafkaClientProps.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.get(Config.KAFKA_BOOTSTRAP_SERVERS));
        kafkaClientProps.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, config.get(Config.APPLICATION_ID));

        if (Boolean.parseBoolean(config.get(Config.TLS_ENABLED))) {
            kafkaClientProps.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SSL");
            kafkaClientProps.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, config.get(Config.TLS_TRUSTSTORE_LOCATION));
            kafkaClientProps.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, config.get(Config.TLS_TRUSTSTORE_PASSWORD));
            kafkaClientProps.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, config.get(Config.TLS_KEYSTORE_LOCATION));
            kafkaClientProps.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, config.get(Config.TLS_KEYSTORE_PASSWORD));
            kafkaClientProps.setProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, config.get(Config.TLS_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM));
        }

        this.adminClient = AdminClient.create(kafkaClientProps);
        LOGGER.debugOp("Using AdminClient {}", adminClient);
        this.kafka = new KafkaImpl(adminClient, vertx);
        LOGGER.debugOp("Using Kafka {}", kafka);
        Labels labels = config.get(Config.LABELS);

        String namespace = config.get(Config.NAMESPACE);
        LOGGER.debugOp("Using namespace {}", namespace);
        this.k8s = new K8sImpl(vertx, kubeClient, labels, namespace);
        LOGGER.debugOp("Using k8s {}", k8s);

        String clientId = config.get(Config.CLIENT_ID);
        LOGGER.debugOp("Using client-Id {}", clientId);

        Zk.create(vertx, config.get(Config.ZOOKEEPER_CONNECT),
                this.config.get(Config.ZOOKEEPER_SESSION_TIMEOUT_MS).intValue(),
                this.config.get(Config.ZOOKEEPER_CONNECTION_TIMEOUT_MS).intValue(),
            zkResult -> {
                if (zkResult.failed()) {
                    start.fail(zkResult.cause());
                    return;
                }
                this.zk = zkResult.result();
                LOGGER.debugOp("Using ZooKeeper {}", zk);

                String topicsPath = config.get(Config.TOPICS_PATH);
                TopicStore topicStore;
                if (config.get(Config.USE_ZOOKEEPER_TOPIC_STORE)) {
                    topicStore = new ZkTopicStore(zk, topicsPath);
                } else {
                    boolean exists = zk.getPathExists(topicsPath);
                    CompletionStage<KafkaStreamsTopicStoreService> cs;
                    if (exists) {
                        cs = Zk2KafkaStreams.upgrade(zk, config, kafkaClientProps, false);
                    } else {
                        KafkaStreamsTopicStoreService ksc = new KafkaStreamsTopicStoreService();
                        cs = ksc.start(config, kafkaClientProps).thenCompose(s -> CompletableFuture.completedFuture(ksc));
                    }
                    topicStore = ConcurrentUtil.result(
                            cs.handle((s, t) -> {
                                if (t != null) {
                                    LOGGER.errorOp("Failed to create topic store.", t);
                                    start.fail(t);
                                    return null; // [1]
                                } else {
                                    service = s;
                                    return s.store;
                                }
                            }));
                    if (topicStore == null) {
                        return; // [1]
                    }
                }

                LOGGER.debugOp("Using TopicStore {}", topicStore);

                this.topicOperator = new TopicOperator(vertx, kafka, k8s, topicStore, labels, namespace, config, new MicrometerMetricsProvider());
                LOGGER.debugOp("Using Operator {}", topicOperator);

                this.topicConfigsWatcher = new TopicConfigsWatcher(topicOperator);
                LOGGER.debugOp("Using TopicConfigsWatcher {}", topicConfigsWatcher);
                this.topicWatcher = new ZkTopicWatcher(topicOperator);
                LOGGER.debugOp("Using TopicWatcher {}", topicWatcher);
                this.topicsWatcher = new ZkTopicsWatcher(topicOperator, topicConfigsWatcher, topicWatcher);
                LOGGER.debugOp("Using TopicsWatcher {}", topicsWatcher);
                topicsWatcher.start(zk);

                Promise<Void> initReconcilePromise = Promise.promise();

                watcher = new K8sTopicWatcher(topicOperator, initReconcilePromise.future(), this::startWatcher);
                LOGGER.debugOp("Starting watcher");
                startWatcher().compose(
                    ignored -> {
                        LOGGER.debugOp("Starting health server");
                        return Future.<Void>succeededFuture();
                    })
                    .compose(i -> startHealthServer())
                    .onComplete(finished -> {
                        Session.this.healthServer = finished.result();
                        start.complete();
                    });

                final Long interval = config.get(Config.FULL_RECONCILIATION_INTERVAL_MS);
                Handler<Long> periodic = new Handler<>() {
                    @Override
                    public void handle(Long oldTimerId) {
                        if (!stopped) {
                            timerId = null;
                            boolean isInitialReconcile = oldTimerId == null;
                            topicOperator.getPeriodicReconciliationsCounter().increment();
                            topicOperator.reconcileAllTopics(isInitialReconcile ? "initial " : "periodic ").onComplete(result -> {
                                if (isInitialReconcile) {
                                    initReconcilePromise.complete();
                                }
                                if (!stopped) {
                                    timerId = vertx.setTimer(interval, this);
                                }
                            });
                        }
                    }
                };
                periodic.handle(null);
                LOGGER.infoOp("Started");
            });
    }

    Future<Void> startWatcher() {
        Promise<Void> promise = Promise.promise();
        try {
            LOGGER.debugOp("Watching KafkaTopics matching {}", config.get(Config.LABELS).labels());

            Session.this.topicWatch = kubeClient.customResources(KafkaTopic.class, KafkaTopicList.class)
                    .inNamespace(config.get(Config.NAMESPACE)).withLabels(config.get(Config.LABELS).labels()).watch(watcher);
            LOGGER.debugOp("Watching setup");
            promise.complete();
        } catch (Throwable t) {
            promise.fail(t);
        }
        return promise.future();
    }

    /**
     * Start an HTTP health server
     */
    private Future<HttpServer> startHealthServer() {

        return this.vertx.createHttpServer()
                .requestHandler(request -> {

                    if (request.path().equals("/healthy")) {
                        request.response().setStatusCode(200).end();
                    } else if (request.path().equals("/ready")) {
                        request.response().setStatusCode(200).end();
                    } else if (request.path().equals("/metrics")) {
                        request.response().setStatusCode(200).end(metricsRegistry.scrape());
                    }
                })
                .listen(HEALTH_SERVER_PORT);
    }
}
