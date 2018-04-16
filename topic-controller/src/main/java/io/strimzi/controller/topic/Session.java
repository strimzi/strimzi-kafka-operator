/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.topic;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.strimzi.controller.topic.zk.Zk;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Session extends AbstractVerticle {

    private final static Logger LOGGER = LoggerFactory.getLogger(Session.class);

    private static final int HEALTH_SERVER_PORT = 8080;

    private final Config config;
    private final KubernetesClient kubeClient;

    ControllerAssignedKafkaImpl kafka;
    AdminClient adminClient;
    K8sImpl k8s;
    Controller controller;
    Watch topicCmWatch;
    TopicsWatcher topicsWatcher;
    TopicConfigsWatcher topicConfigsWatcher;
    TopicWatcher topicWatcher;
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
            sb.append("\t").append(v.key).append(": ").append(config.get(v)).append(System.lineSeparator());
        }
        LOGGER.info("Using config:{}", sb.toString());
    }

    /**
     * Stop the controller.
     */
    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        this.stopped = true;
        Long timerId = this.timerId;
        if (timerId != null) {
            vertx.cancelTimer(timerId);
        }
        vertx.executeBlocking(blockingResult -> {
            long t0 = System.currentTimeMillis();
            long timeout = 120_000L;
            LOGGER.info("Stopping");
            LOGGER.debug("Stopping kube watch");
            topicCmWatch.close();
            LOGGER.debug("Stopping zk watches");
            topicsWatcher.stop();

            while (controller.isWorkInflight()) {
                if (System.currentTimeMillis() - t0 > timeout) {
                    LOGGER.error("Timeout waiting for inflight work to finish");
                    break;
                }
                LOGGER.debug("Waiting for inflight work to finish");
                try {
                    Thread.sleep(1_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            LOGGER.debug("Stopping kafka {}", kafka);
            kafka.stop();
            try {
                LOGGER.debug("Disconnecting from zookeeper {}", zk);
                zk.disconnect();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            LOGGER.debug("Closing AdminClient {}", adminClient);
            adminClient.close(timeout - (System.currentTimeMillis() - t0), TimeUnit.MILLISECONDS);

            HttpServer healthServer = this.healthServer;
            if (healthServer != null) {
                healthServer.close();
            }

            LOGGER.info("Stopped");
            blockingResult.complete();
        }, stopFuture);
    }

    @Override
    public void start() {
        LOGGER.info("Starting");
        Properties adminClientProps = new Properties();
        adminClientProps.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.get(Config.KAFKA_BOOTSTRAP_SERVERS));
        this.adminClient = AdminClient.create(adminClientProps);
        LOGGER.debug("Using AdminClient {}", adminClient);
        this.kafka = new ControllerAssignedKafkaImpl(adminClient, vertx, config);
        LOGGER.debug("Using Kafka {}", kafka);
        LabelPredicate cmPredicate = config.get(Config.LABELS);

        String namespace = config.get(Config.NAMESPACE);
        LOGGER.debug("Using namespace {}", namespace);
        this.k8s = new K8sImpl(vertx, kubeClient, cmPredicate, namespace);
        LOGGER.debug("Using k8s {}", k8s);

        this.zk = Zk.create(vertx, config.get(Config.ZOOKEEPER_CONNECT), this.config.get(Config.ZOOKEEPER_SESSION_TIMEOUT_MS).intValue());
        LOGGER.debug("Using ZooKeeper {}", zk);

        ZkTopicStore topicStore = new ZkTopicStore(zk);
        LOGGER.debug("Using TopicStore {}", topicStore);

        this.controller = new Controller(vertx, kafka, k8s, topicStore, cmPredicate, namespace, config);
        LOGGER.debug("Using Controller {}", controller);

        this.topicConfigsWatcher = new TopicConfigsWatcher(controller);
        LOGGER.debug("Using TopicConfigsWatcher {}", topicConfigsWatcher);
        this.topicWatcher = new TopicWatcher(controller);
        LOGGER.debug("Using TopicWatcher {}", topicWatcher);
        this.topicsWatcher = new TopicsWatcher(controller, topicConfigsWatcher, topicWatcher);
        LOGGER.debug("Using TopicsWatcher {}", topicsWatcher);
        topicsWatcher.start(zk);

        Thread configMapThread = new Thread(() -> {
            LOGGER.debug("Watching configmaps matching {}", cmPredicate);
            Session.this.topicCmWatch = kubeClient.configMaps().inNamespace(kubeClient.getNamespace()).watch(new ConfigMapWatcher(controller, cmPredicate));
            LOGGER.debug("Watching setup");

            // start the HTTP server for healthchecks
            healthServer = this.startHealthServer();

        }, "configmap-watcher");
        LOGGER.debug("Starting {}", configMapThread);
        configMapThread.start();

        final Long interval = config.get(Config.FULL_RECONCILIATION_INTERVAL_MS);
        Handler<Long> periodic = new Handler<Long>() {
            @Override
            public void handle(Long oldTimerId) {
                if (!stopped) {
                    timerId = null;
                    controller.reconcileAllTopics("periodic").setHandler(result -> {
                        if (!stopped) {
                            timerId = vertx.setTimer(interval, this);
                        }
                    });
                }
            }
        };
        periodic.handle(null);
        LOGGER.info("Started");
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
                    }
                })
                .listen(HEALTH_SERVER_PORT);
    }
}
