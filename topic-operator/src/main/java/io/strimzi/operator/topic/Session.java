/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.DoneableKafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.MicrometerMetricsProvider;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.Security;
import java.time.Duration;
import java.util.Properties;
import io.micrometer.prometheus.PrometheusMeterRegistry;

public class Session extends AbstractVerticle {

    private final static Logger LOGGER = LogManager.getLogger(Session.class);

    private static final int HEALTH_SERVER_PORT = 8080;


    private final Config config;
    private final KubernetesClient kubeClient;

    /*test*/ KafkaImpl kafka;
    private AdminClient adminClient;
    /*test*/ K8sImpl k8s;
    /*test*/ TopicOperator topicOperator;
    private Watch topicWatch;
    /*test*/ ZkTopicsWatcher topicsWatcher;
    /*test*/ TopicConfigsWatcher topicConfigsWatcher;
    /*test*/ ZkTopicWatcher topicWatcher;
    /*test*/ PrometheusMeterRegistry metricsRegistry;
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
        LOGGER.info("Using config:{}", sb.toString());
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
            LOGGER.info("Stopping");
            LOGGER.debug("Stopping kube watch");
            topicWatch.close();
            LOGGER.debug("Stopping zk watches");
            topicsWatcher.stop();

            Promise<Void> promise = Promise.promise();
            Handler<Long> longHandler = new Handler<Long>() {
                @Override
                public void handle(Long inflightTimerId) {
                    if (!topicOperator.isWorkInflight()) {
                        LOGGER.debug("Inflight work has finished");
                        promise.complete();
                    } else if (System.currentTimeMillis() > deadline) {
                        LOGGER.error("Timeout waiting for inflight work to finish");
                        promise.complete();
                    } else {
                        LOGGER.debug("Waiting for inflight work to finish");
                        vertx.setTimer(1_000, this);
                    }
                }
            };
            longHandler.handle(null);
            promise.future().compose(ignored -> {

                LOGGER.debug("Disconnecting from zookeeper {}", zk);
                zk.disconnect(zkResult -> {
                    if (zkResult.failed()) {
                        LOGGER.warn("Error disconnecting from zookeeper: {}", String.valueOf(zkResult.cause()));
                    }
                    long timeoutMs = Math.max(1, deadline - System.currentTimeMillis());
                    LOGGER.debug("Closing AdminClient {} with timeout {}ms", adminClient, timeoutMs);
                    try {
                        adminClient.close(Duration.ofMillis(timeoutMs));
                        HttpServer healthServer = this.healthServer;
                        if (healthServer != null) {
                            healthServer.close();
                        }
                    } catch (TimeoutException e) {
                        LOGGER.warn("Timeout while closing AdminClient with timeout {}ms", e, timeoutMs);
                    } finally {
                        LOGGER.info("Stopped");
                        blockingResult.complete();
                    }
                });
                return Future.succeededFuture();
            });
        }, stop);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void start(Promise<Void> start) {
        LOGGER.info("Starting");
        Properties adminClientProps = new Properties();

        String dnsCacheTtl = System.getenv("STRIMZI_DNS_CACHE_TTL") == null ? "30" : System.getenv("STRIMZI_DNS_CACHE_TTL");
        Security.setProperty("networkaddress.cache.ttl", dnsCacheTtl);
        adminClientProps.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.get(Config.KAFKA_BOOTSTRAP_SERVERS));

        if (Boolean.valueOf(config.get(Config.TLS_ENABLED))) {
            adminClientProps.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SSL");
            adminClientProps.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, config.get(Config.TLS_TRUSTSTORE_LOCATION));
            adminClientProps.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, config.get(Config.TLS_TRUSTSTORE_PASSWORD));
            adminClientProps.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, config.get(Config.TLS_KEYSTORE_LOCATION));
            adminClientProps.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, config.get(Config.TLS_KEYSTORE_PASSWORD));
            adminClientProps.setProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, config.get(Config.TLS_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM));
        }

        this.adminClient = AdminClient.create(adminClientProps);
        LOGGER.debug("Using AdminClient {}", adminClient);
        this.kafka = new KafkaImpl(adminClient, vertx);
        LOGGER.debug("Using Kafka {}", kafka);
        Labels labels = config.get(Config.LABELS);

        String namespace = config.get(Config.NAMESPACE);
        LOGGER.debug("Using namespace {}", namespace);
        this.k8s = new K8sImpl(vertx, kubeClient, labels, namespace);
        LOGGER.debug("Using k8s {}", k8s);

        Zk.create(vertx, config.get(Config.ZOOKEEPER_CONNECT),
                this.config.get(Config.ZOOKEEPER_SESSION_TIMEOUT_MS).intValue(),
                this.config.get(Config.ZOOKEEPER_CONNECTION_TIMEOUT_MS).intValue(),
            zkResult -> {
                if (zkResult.failed()) {
                    start.fail(zkResult.cause());
                    return;
                }
                this.zk = zkResult.result();
                LOGGER.debug("Using ZooKeeper {}", zk);

                String topicsPath = config.get(Config.TOPICS_PATH);
                ZkTopicStore topicStore = new ZkTopicStore(zk, topicsPath);

                LOGGER.debug("Using TopicStore {}", topicStore);

                this.topicOperator = new TopicOperator(vertx, kafka, k8s, topicStore, labels, namespace, config, new MicrometerMetricsProvider());
                LOGGER.debug("Using Operator {}", topicOperator);

                this.topicConfigsWatcher = new TopicConfigsWatcher(topicOperator);
                LOGGER.debug("Using TopicConfigsWatcher {}", topicConfigsWatcher);
                this.topicWatcher = new ZkTopicWatcher(topicOperator);
                LOGGER.debug("Using TopicWatcher {}", topicWatcher);
                this.topicsWatcher = new ZkTopicsWatcher(topicOperator, topicConfigsWatcher, topicWatcher);
                LOGGER.debug("Using TopicsWatcher {}", topicsWatcher);
                topicsWatcher.start(zk);

                Promise<Void> promise = Promise.promise();
                Promise<Void> initReconcilePromise = Promise.promise();
                K8sTopicWatcher watcher = new K8sTopicWatcher(topicOperator, initReconcilePromise.future());
                Thread resourceThread = new Thread(() -> {
                    try {
                        LOGGER.debug("Watching KafkaTopics matching {}", labels.labels());

                        Session.this.topicWatch = kubeClient.customResources(Crds.kafkaTopic(), KafkaTopic.class, KafkaTopicList.class, DoneableKafkaTopic.class)
                                .inNamespace(namespace).withLabels(labels.labels()).watch(watcher);
                        LOGGER.debug("Watching setup");

                        // start the HTTP server for healthchecks
                        healthServer = this.startHealthServer();
                        promise.complete();
                    } catch (Throwable t) {
                        promise.fail(t);
                    }

                }, "resource-watcher");
                LOGGER.debug("Starting {}", resourceThread);
                resourceThread.start();

                final Long interval = config.get(Config.FULL_RECONCILIATION_INTERVAL_MS);
                Handler<Long> periodic = new Handler<Long>() {
                    @Override
                    public void handle(Long oldTimerId) {
                        if (!stopped) {
                            timerId = null;
                            boolean isInitialReconcile = oldTimerId == null;
                            topicOperator.reconcileAllTopics(isInitialReconcile ? "initial " : "periodic ").onComplete(result -> {
                                topicOperator.getPeriodicReconciliationsCounter().increment();
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
                promise.future().onComplete(start);
                LOGGER.info("Started");
            });
    }

    /**
     * Start an HTTP health server
     */
    private HttpServer startHealthServer() {

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
