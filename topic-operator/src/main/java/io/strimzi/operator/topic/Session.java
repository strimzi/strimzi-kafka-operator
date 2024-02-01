/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicList;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.topic.zk.Zk;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.HttpServer;
import io.vertx.micrometer.backends.BackendRegistries;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.Security;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static io.strimzi.operator.topic.Config.ZOOKEEPER_CONNECT;
import static io.strimzi.operator.topic.Config.ZOOKEEPER_CONNECTION_TIMEOUT_MS;
import static io.strimzi.operator.topic.Config.ZOOKEEPER_SESSION_TIMEOUT_MS;

/** Session of Topic operator */
public class Session extends AbstractVerticle {

    private final static Logger LOGGER = LogManager.getLogger(Session.class);

    private final static String SASL_TYPE_PLAIN = "plain";
    private final static String SASL_TYPE_SCRAM_SHA_256 = "scram-sha-256";
    private final static String SASL_TYPE_SCRAM_SHA_512 = "scram-sha-512";
    private final static String SASL_TYPE_CUSTOM = "custom";

    private static final int HEALTH_SERVER_PORT = 8080;

    private static final TypeReference<HashMap<String, String>> STRING_HASH_MAP_TYPE_REFERENCE = new TypeReference<>() { };

    private final Config config;
    // this field is required to keep the underlying shared worker pool alive
    private WorkerExecutor executor;
    // this field is required to keep the underlying shared worker pool alive
    @SuppressWarnings("unused")
    private WorkerExecutor kubernetesOpsExecutor;
    private final TopicOperatorState topicOperatorState;
    private final KubernetesClient kubeClient;
    private final BiFunction<Zk, Config, TopicStore> topicStoreCreator;
    private final BiFunction<Vertx, Config, Future<Zk>> zkClientCreator;

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
    /**
     * The id of the periodic reconciliation timer. This is null during a periodic reconciliation.
     */
    private volatile Long timerId;
    private volatile boolean stopped = false;
    private Zk zk;
    private volatile HttpServer healthServer;

    /**
     * @param kubeClient kubernetes client
     * @param config     TopicOperator config
     */
    public Session(KubernetesClient kubeClient, Config config) {
        this(kubeClient, config, null, null, new TopicOperatorState());
    }

    /**
     * Constructor is package private for testing
     *
     * @param kubeClient         kubernetes client
     * @param config             TopicOperator config
     * @param topicStoreCreator  indirection to allow unit testing of start-up sequence with mock topicstore creation
     * @param zkClientCreator    indirection to allow unit testing of start-up sequence with mock ZK client creation
     * @param topicOperatorState used to communicate liveness/readiness to health server from things that can potentially fail
     */
    Session(KubernetesClient kubeClient,
            Config config,
            BiFunction<Zk, Config, TopicStore> topicStoreCreator,
            BiFunction<Vertx, Config, Future<Zk>> zkClientCreator,
            TopicOperatorState topicOperatorState) {
        this.topicStoreCreator = topicStoreCreator == null ? this::createTopicStore : topicStoreCreator;
        this.zkClientCreator = zkClientCreator == null ? this::createZk : zkClientCreator;
        this.kubeClient = kubeClient;
        this.config = config;
        this.topicOperatorState = topicOperatorState;
        StringBuilder sb = new StringBuilder(System.lineSeparator());
        for (Config.Value<?> v : Config.keys()) {
            sb.append("\t").append(v.key).append(": ").append(Util.maskPassword(v.key, config.get(v).toString())).append(System.lineSeparator());
        }
        LOGGER.info("Using config:{}", sb.toString());
        this.metricsRegistry = (PrometheusMeterRegistry) BackendRegistries.getDefaultNow();
    }

    @Override
    public void init(Vertx vertx, Context context) {
        super.init(vertx, context);
        executor = vertx.createSharedWorkerExecutor("blocking-startup-ops", 1);
        kubernetesOpsExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
    }

    /**
     * Stop the operator.
     */
    @Override
    public void stop(Promise<Void> stop) throws Exception {
        LOGGER.info("Stopping");
        this.stopped = true;
        if (this.timerId != null) {
            vertx.cancelTimer(this.timerId);
        }
        closeKubernetesWatch()
                .onSuccess(ignored -> LOGGER.debug("Kubernetes watch closed"))
                .onFailure(cause -> LOGGER.error("Failed to close Kubernetes watch", cause))
            .compose(ignored -> stopZooKeeperWatcher())
                .onSuccess(ignored -> LOGGER.debug("ZooKeeper topics watcher stopped"))
                .onFailure(cause -> LOGGER.error("Failed to stop ZooKeeper topics watcher", cause))
            .compose(ignored -> stopTopicStoreService())
                .onSuccess(ignored -> LOGGER.debug("Topic store service stopped"))
                .onFailure(cause -> LOGGER.error("Failed to stop topic store service", cause))
            .compose(ignored -> disconnectFromZk())
                .onSuccess(ignored -> LOGGER.debug("ZooKeeper disconnected"))
                .onFailure(cause -> LOGGER.error("Failed to disconnect from ZooKeeper", cause))
            .compose(ignored -> closeKafkaAdminClient())
                .onSuccess(ignored -> LOGGER.debug("Kafka admin client closed"))
                .onFailure(cause -> LOGGER.error("Failed to close Kafka admin client", cause))
            .compose(ignored -> closeHttpHealthServer())
                .onSuccess(ignored -> LOGGER.debug("HTTP health server closed"))
                .onFailure(cause -> LOGGER.error("Failed to close HTTP health server", cause))
            .compose(ignored -> {
                LOGGER.info("Stopped");
                return Future.future(handle -> stop.complete());
            });
    }

    private Future<Void> closeKubernetesWatch() {
        LOGGER.debug("Closing Kubernetes watch");
        Promise<Void> promise = Promise.promise();
        if (topicWatch == null) {
            promise.complete();
        } else {
            vertx.<Void>executeBlocking(() -> {
                try {
                    topicWatch.close();
                } finally {
                    promise.complete();
                    return null;
                }
            });
        }
        return promise.future();
    }

    private Future<Void> stopZooKeeperWatcher() {
        LOGGER.debug("Stopping ZooKeeper watcher");
        Promise<Void> promise = Promise.promise();
        if (topicsWatcher == null) {
            promise.complete();
        } else {
            vertx.<Void>executeBlocking(() -> {
                try {
                    topicsWatcher.stop();
                } finally {
                    promise.complete();
                    return null;
                }
            });
        }
        return promise.future();
    }

    private Future<Void> stopTopicStoreService() {
        LOGGER.debug("Stopping topic store service");
        if (topicOperator != null && service != null) {
            int timeoutSec = 5;
            while (topicOperator.isWorkInflight() && timeoutSec-- > 0) {
                try {
                    LOGGER.debug("Waiting for inflight operations to complete");
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            service.stop();
        }
        return Future.succeededFuture();
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    private Future<Void> disconnectFromZk() {
        LOGGER.debug("Disconnecting from ZooKeeper");
        Promise<Void> promise = Promise.promise();
        if (zk != null) {
            promise.complete();
        } else {
            vertx.<Void>executeBlocking(() -> {
                zk.disconnect(result -> {
                    if (result.failed()) {
                        LOGGER.warn("Error disconnecting from ZooKeeper: {}", String.valueOf(result.cause()));
                    }
                    promise.complete();
                });
                return null;
            });
        }
        return promise.future();
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    private Future<Void> closeKafkaAdminClient() {
        LOGGER.debug("Closing Kafka admin client");
        Promise<Void> promise = Promise.promise();
        if (adminClient == null) {
            promise.complete();
        } else {
            vertx.<Void>executeBlocking(() -> {
                try {
                    adminClient.close(Duration.ofSeconds(2));
                } finally {
                    promise.complete();
                    return null;
                }
            });
        }
        return promise.future();
    }

    private Future<Void> closeHttpHealthServer() {
        LOGGER.debug("Closing HTTP health server");
        if (healthServer != null) {
            healthServer.close();
        }
        return Future.succeededFuture();
    }

    /**
     * Starts the operator.
     */
    @SuppressWarnings({"JavaNCSS", "MethodLength", "CyclomaticComplexity"})
    @Override
    public void start(Promise<Void> start) {
        LOGGER.info("Starting");

        String dnsCacheTtl = System.getenv("STRIMZI_DNS_CACHE_TTL") == null ? "30" : System.getenv("STRIMZI_DNS_CACHE_TTL");
        Security.setProperty("networkaddress.cache.ttl", dnsCacheTtl);

        this.adminClient = AdminClient.create(adminClientProperties());
        LOGGER.debug("Using AdminClient {}", adminClient);
        this.kafka = new KafkaImpl(adminClient, vertx);
        LOGGER.debug("Using Kafka {}", kafka);
        Labels labels = config.get(Config.LABELS);

        String namespace = config.get(Config.NAMESPACE);
        LOGGER.debug("Using namespace {}", namespace);
        this.k8s = new K8sImpl(vertx, kubeClient, labels, namespace);
        LOGGER.debug("Using k8s {}", k8s);

        String clientId = config.get(Config.CLIENT_ID);
        LOGGER.debug("Using client-Id {}", clientId);

        startHealthServer(topicOperatorState)
                .onFailure(cause -> LOGGER.error("Failed to start health server", cause))
                .onSuccess(httpServer -> healthServer = httpServer)
                .compose(ignored -> zkClientCreator.apply(vertx, config))
                .onSuccess(zookeeper -> zk = zookeeper)
                .compose(zk -> createTopicStoreAsync(zk, config))
                .onSuccess(topicStore -> LOGGER.debug("Using TopicStore {}", topicStore))
                .compose(topicStore -> createTopicOperatorAndZkWatchers(labels, namespace, topicStore))
                .compose(this::createK8sWatcher)
                .onSuccess(this::createPeriodicReconcileTrigger)
                .onSuccess(ignored -> {
                    start.complete();
                    topicOperatorState.setReady(true);
                    LOGGER.info("Started");
                })
                .onFailure(cause -> {
                    topicOperatorState.setAlive(false);
                    start.fail(cause);
                    LOGGER.error("Topic operator start up failed, cause was", cause);
                });
    }
    
    private Future<Promise<Void>> createK8sWatcher(TopicOperator topicOperator) {
        Promise<Promise<Void>> blockingResult = Promise.promise();
        executor.executeBlocking(() -> {
            Promise<Void> initReconcilePromise = Promise.promise();
            watcher = new K8sTopicWatcher(topicOperator, initReconcilePromise.future(), this::startWatcher);
            LOGGER.debug("Starting watcher");
            startWatcher().onSuccess(v -> blockingResult.complete(initReconcilePromise));
            return null;
        });
        return blockingResult.future();
    }

    private Future<TopicStore> createTopicStoreAsync(Zk zk, Config config) {
        return executor.executeBlocking(() -> {
            Instant startedAt = Instant.now();
            try {
                TopicStore topicStore = topicStoreCreator.apply(zk, config);
                LOGGER.info("Topic store created, took {} ms", Duration.between(startedAt, Instant.now()).toMillis());
                return topicStore;
            } catch (Exception e) {
                LOGGER.error("Failed to create topic store.", e);
                throw e;
            }
        });
    }

    private Future<Zk> createZk(Vertx vertx, Config config) {
        return Zk.create(vertx,
                         config.get(ZOOKEEPER_CONNECT),
                         config.get(ZOOKEEPER_SESSION_TIMEOUT_MS).intValue(),
                         config.get(ZOOKEEPER_CONNECTION_TIMEOUT_MS).intValue())
                 .onFailure(cause -> LOGGER.error("Failed to start ZK client", cause))
                 .onSuccess(zookeeper -> LOGGER.debug("Using ZooKeeper {}", zookeeper));
    }

    private Future<TopicOperator> createTopicOperatorAndZkWatchers(Labels labels, String namespace, TopicStore topicStore) {
        topicOperator = new TopicOperator(vertx, kafka, k8s, topicStore, labels, namespace, config, new MicrometerMetricsProvider());
        LOGGER.debug("Using Operator {}", topicOperator);

        topicConfigsWatcher = new TopicConfigsWatcher(topicOperator);
        LOGGER.debug("Using TopicConfigsWatcher {}", topicConfigsWatcher);
        topicWatcher = new ZkTopicWatcher(topicOperator);
        LOGGER.debug("Using TopicWatcher {}", topicWatcher);
        topicsWatcher = new ZkTopicsWatcher(topicOperator, topicConfigsWatcher, topicWatcher);
        LOGGER.debug("Using TopicsWatcher {}", topicsWatcher);
        topicsWatcher.start(zk);
        return Future.succeededFuture(topicOperator);
    }

    private void createPeriodicReconcileTrigger(Promise<Void> initReconcilePromise) {
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
    }

    private TopicStore createTopicStore(Zk zk, Config config) {
        String topicsPath = config.get(Config.TOPICS_PATH);
        if (config.get(Config.USE_ZOOKEEPER_TOPIC_STORE)) {
            return new ZkTopicStore(zk, topicsPath);
        } else {
            boolean exists = zk.getPathExists(topicsPath);
            CompletionStage<KafkaStreamsTopicStoreService> cs;
            if (exists) {
                cs = Zk2KafkaStreams.upgrade(zk, config, adminClientProperties(), false);
            } else {
                KafkaStreamsTopicStoreService ksc = new KafkaStreamsTopicStoreService();
                cs = ksc.start(config, adminClientProperties()).thenCompose(s -> CompletableFuture.completedFuture(ksc));
            }
            try {
                KafkaStreamsTopicStoreService kafkaStreamsTopicStoreService = cs.toCompletableFuture().get();
                this.service = kafkaStreamsTopicStoreService;
                return kafkaStreamsTopicStoreService.store;
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void setSaslConfigs(Properties kafkaClientProps) {
        String configSaslMechanism = config.get(Config.SASL_MECHANISM);
        
        switch (configSaslMechanism) {
            case SASL_TYPE_PLAIN:
            case SASL_TYPE_SCRAM_SHA_256:
            case SASL_TYPE_SCRAM_SHA_512:
                setStandardSaslConfigs(kafkaClientProps);
                break;
            case "custom":
                setCustomSaslConfigs(kafkaClientProps);
                break;
            default:
                throw new IllegalArgumentException("Invalid SASL_MECHANISM: '" + configSaslMechanism + "'");
        }
    }

    private void setCustomSaslConfigs(Properties kafkaClientProps) {
        String customPropsString = config.get(Config.SASL_CUSTOM_CONFIG);

        if (customPropsString.isEmpty()) {
            throw new InvalidConfigurationException(Config.SASL_MECHANISM + "=custom, but env var " + Config.SASL_CUSTOM_CONFIG + " is not set");
        }

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            Map<String, String> customProperties = objectMapper.readValue(customPropsString, STRING_HASH_MAP_TYPE_REFERENCE);

            if (customProperties.isEmpty()) {
                throw new InvalidConfigurationException(Config.SASL_MECHANISM + "=custom, but env var " + Config.SASL_CUSTOM_CONFIG + " is empty");
            }

            for (var entry : customProperties.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();

                if (key == null || key.isBlank() || !key.startsWith("sasl.")) {
                    throw new InvalidConfigurationException("SASL custom config properties not SASL properties. customProperty: '" + key + "' = '" + value + "'");
                }

                kafkaClientProps.setProperty(key, value);
            }
        } catch (JsonProcessingException e) {
            throw new InvalidConfigurationException("SASL custom config properties deserialize failed. customProperties: '" + customPropsString + "'");
        }
    }

    private void setStandardSaslConfigs(Properties kafkaClientProps) {
        String saslMechanism = null;
        String jaasConfig = null;
        String username = config.get(Config.SASL_USERNAME);
        String password = config.get(Config.SASL_PASSWORD);
        String configSaslMechanism = config.get(Config.SASL_MECHANISM);

        LOGGER.error("setStandardSaslConfigs()");
        LOGGER.error("setStandardSaslConfigs() username: {}, password: {}", username, password);

        if (username.isEmpty() || password.isEmpty()) {
            throw new InvalidConfigurationException("SASL credentials are not set");
        }

        if (SASL_TYPE_PLAIN.equals(configSaslMechanism)) {
            saslMechanism = "PLAIN";
            jaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";";
        } else if (SASL_TYPE_SCRAM_SHA_256.equals(configSaslMechanism) || SASL_TYPE_SCRAM_SHA_512.equals(configSaslMechanism)) {
            jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + username + "\" password=\"" + password + "\";";

            if (SASL_TYPE_SCRAM_SHA_256.equals(configSaslMechanism)) {
                saslMechanism = "SCRAM-SHA-256";
            } else {
                saslMechanism = "SCRAM-SHA-512";
            }
        } else {
            throw new IllegalArgumentException("Invalid SASL_MECHANISM type: " + config.get(Config.SASL_MECHANISM));
        }

        kafkaClientProps.setProperty(SaslConfigs.SASL_MECHANISM, saslMechanism);
        kafkaClientProps.setProperty(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
    }

    Properties adminClientProperties() {
        Properties kafkaClientProps = new Properties();
        kafkaClientProps.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.get(Config.KAFKA_BOOTSTRAP_SERVERS));
        kafkaClientProps.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, config.get(Config.APPLICATION_ID));

        String securityProtocol = config.get(Config.SECURITY_PROTOCOL);
        boolean tlsEnabled = Boolean.parseBoolean(config.get(Config.TLS_ENABLED));

        if (tlsEnabled && !securityProtocol.isEmpty()) {
            if (!securityProtocol.equals("SSL") && !securityProtocol.equals("SASL_SSL")) {
                throw new InvalidConfigurationException("TLS is enabled but the security protocol does not match SSL or SASL_SSL");
            }
        }

        if (!securityProtocol.isEmpty()) {
            kafkaClientProps.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        } else if (tlsEnabled) {
            kafkaClientProps.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SSL");
        } else {
            kafkaClientProps.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
        }

        if (securityProtocol.equals("SASL_SSL") || securityProtocol.equals("SSL") || tlsEnabled) {
            kafkaClientProps.setProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, config.get(Config.TLS_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM));

            if (!config.get(Config.TLS_TRUSTSTORE_LOCATION).isEmpty()) {
                kafkaClientProps.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, config.get(Config.TLS_TRUSTSTORE_LOCATION));
            }

            if (!config.get(Config.TLS_TRUSTSTORE_PASSWORD).isEmpty()) {
                if (config.get(Config.TLS_TRUSTSTORE_LOCATION).isEmpty()) {
                    throw new InvalidConfigurationException("TLS_TRUSTSTORE_PASSWORD was supplied but TLS_TRUSTSTORE_LOCATION was not supplied");
                }
                kafkaClientProps.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, config.get(Config.TLS_TRUSTSTORE_PASSWORD));
            }

            if (!config.get(Config.TLS_KEYSTORE_LOCATION).isEmpty() && !config.get(Config.TLS_KEYSTORE_PASSWORD).isEmpty()) {
                kafkaClientProps.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, config.get(Config.TLS_KEYSTORE_LOCATION));
                kafkaClientProps.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, config.get(Config.TLS_KEYSTORE_PASSWORD));
            }
        }

        if (Boolean.parseBoolean(config.get(Config.SASL_ENABLED))) {
            setSaslConfigs(kafkaClientProps);
        }
        return kafkaClientProps;
    }

    Future<Void> startWatcher() {
        Promise<Void> promise = Promise.promise();
        try {
            LOGGER.debug("Watching KafkaTopics matching {}", config.get(Config.LABELS).labels());

            Session.this.topicWatch = kubeClient.resources(KafkaTopic.class, KafkaTopicList.class)
                    .inNamespace(config.get(Config.NAMESPACE)).withLabels(config.get(Config.LABELS).labels()).watch(watcher);
            LOGGER.debug("Watching setup");
            promise.complete();
        } catch (Throwable t) {
            promise.fail(t);
        }
        return promise.future();
    }

    /**
     * Start an HTTP health server
     */
    private Future<HttpServer> startHealthServer(TopicOperatorState tos) {

        return this.vertx.createHttpServer()
                         .requestHandler(request -> {
                             switch (request.path()) {
                                 case "/healthy":
                                     request.response().setStatusCode(tos.isAlive() ? 204 : 500).end();
                                     break;
                                 case "/ready":
                                     request.response().setStatusCode(tos.isReady() ? 204 : 500).end();
                                     break;
                                 case "/metrics":
                                     request.response().setStatusCode(200).end(metricsRegistry.scrape());
                                     break;
                                 default:
                                     request.response().setStatusCode(404).end();
                             }
                         })
                         .listen(HEALTH_SERVER_PORT);
    }
}
