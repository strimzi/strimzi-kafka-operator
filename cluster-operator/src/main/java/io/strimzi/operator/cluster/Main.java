/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.strimzi.api.kafka.Crds;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.leaderelection.LeaderElectionManager;
import io.strimzi.operator.cluster.model.securityprofiles.PodSecurityProviderFactory;
import io.strimzi.operator.cluster.operator.assembly.KafkaAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaBridgeAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaConnectAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaMirrorMaker2AssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaMirrorMakerAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceAssemblyOperator;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.operator.common.OperatorKubernetesClientBuilder;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ShutdownHook;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.resource.ClusterRoleOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.Security;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * The main class used to start the Strimzi Cluster Operator
 */
@SuppressFBWarnings("DM_EXIT")
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling"})
public class Main {
    private static final Logger LOGGER = LogManager.getLogger(Main.class.getName());

    private static final int HEALTH_SERVER_PORT = 8080;

    static {
        try {
            Crds.registerCustomKinds();
        } catch (Error | RuntimeException t) {
            t.printStackTrace();
        }
    }

    /**
     * The main method used to run the Cluster Operator
     *
     * @param args  The command line arguments
     */
    public static void main(String[] args) {
        final String strimziVersion = Main.class.getPackage().getImplementationVersion();
        LOGGER.info("ClusterOperator {} is starting", strimziVersion);
        Util.printEnvInfo(); // Prints configured environment variables
        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(System.getenv());
        LOGGER.info("Cluster Operator configuration is {}", config);

        // setting DNS cache TTL
        Security.setProperty("networkaddress.cache.ttl", String.valueOf(config.getDnsCacheTtlSec()));

        // setup Micrometer metrics options
        VertxOptions options = new VertxOptions().setMetricsOptions(
            new MicrometerMetricsOptions()
                .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                .setJvmMetricsEnabled(true)
                .setEnabled(true));
        Vertx vertx = Vertx.vertx(options);
        Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook(vertx)));

        // Setup Micrometer Metrics provider
        MetricsProvider metricsProvider = new MicrometerMetricsProvider();
        KubernetesClient client = new OperatorKubernetesClientBuilder("strimzi-cluster-operator", strimziVersion).build();

        maybeCreateClusterRoles(vertx, config, client)
                .compose(i -> startHealthServer(vertx, metricsProvider))
                .compose(i -> leaderElection(client, config))
                .compose(i -> createPlatformFeaturesAvailability(vertx, client))
                .compose(pfa -> deployClusterOperatorVerticles(vertx, client, metricsProvider, pfa, config))
                .onComplete(res -> {
                    if (res.failed())   {
                        LOGGER.error("Unable to start operator for 1 or more namespace", res.cause());
                        System.exit(1);
                    }
                });
    }

    /**
     * Helper method used to get the PlatformFeaturesAvailability instance with the information about the Kubernetes
     * cluster we run on.
     *
     * @param vertx     Vertx instance
     * @param client    Kubernetes client instance
     *
     * @return  Future with the created PlatformFeaturesAvailability object
     */
    private static Future<PlatformFeaturesAvailability> createPlatformFeaturesAvailability(Vertx vertx, KubernetesClient client)    {
        Promise<PlatformFeaturesAvailability> promise = Promise.promise();

        PlatformFeaturesAvailability.create(vertx, client).onComplete(pfa -> {
            if (pfa.succeeded()) {
                LOGGER.info("Environment facts gathered: {}", pfa.result());
                promise.complete(pfa.result());
            } else {
                LOGGER.error("Failed to gather environment facts", pfa.cause());
                promise.fail(pfa.cause());
            }
        });

        return promise.future();
    }

    /**
     * Deploys the ClusterOperator verticles responsible for the actual Cluster Operator functionality. One verticle is
     * started for each namespace the operator watched. In case of watching the whole cluster, only one verticle is started.
     *
     * @param vertx             Vertx instance
     * @param client            Kubernetes client instance
     * @param metricsProvider   Metrics provider instance
     * @param pfa               PlatformFeaturesAvailability instance describing the Kubernetes cluster
     * @param config            Cluster Operator configuration
     *
     * @return  Future which completes when all Cluster Operator verticles are started and running
     */
    static CompositeFuture deployClusterOperatorVerticles(Vertx vertx, KubernetesClient client, MetricsProvider metricsProvider, PlatformFeaturesAvailability pfa, ClusterOperatorConfig config) {
        ResourceOperatorSupplier resourceOperatorSupplier = new ResourceOperatorSupplier(
                vertx,
                client,
                metricsProvider,
                pfa,
                config.getOperationTimeoutMs(),
                config.getOperatorName()
        );

        // Initialize the PodSecurityProvider factory to provide the user configured provider
        PodSecurityProviderFactory.initialize(config.getPodSecurityProviderClass(), pfa);

        KafkaAssemblyOperator kafkaClusterOperations = null;
        KafkaConnectAssemblyOperator kafkaConnectClusterOperations = null;
        KafkaMirrorMaker2AssemblyOperator kafkaMirrorMaker2AssemblyOperator = null;
        KafkaMirrorMakerAssemblyOperator kafkaMirrorMakerAssemblyOperator = null;
        KafkaBridgeAssemblyOperator kafkaBridgeAssemblyOperator = null;
        KafkaRebalanceAssemblyOperator kafkaRebalanceAssemblyOperator = null;

        if (!config.isPodSetReconciliationOnly()) {
            OpenSslCertManager certManager = new OpenSslCertManager();
            PasswordGenerator passwordGenerator = new PasswordGenerator(12,
                    "abcdefghijklmnopqrstuvwxyz" +
                            "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
                    "abcdefghijklmnopqrstuvwxyz" +
                            "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                            "0123456789");

            kafkaClusterOperations = new KafkaAssemblyOperator(vertx, pfa, certManager, passwordGenerator, resourceOperatorSupplier, config);
            kafkaConnectClusterOperations = new KafkaConnectAssemblyOperator(vertx, pfa, resourceOperatorSupplier, config);
            kafkaMirrorMaker2AssemblyOperator = new KafkaMirrorMaker2AssemblyOperator(vertx, pfa, resourceOperatorSupplier, config);
            kafkaMirrorMakerAssemblyOperator = new KafkaMirrorMakerAssemblyOperator(vertx, pfa, certManager, passwordGenerator, resourceOperatorSupplier, config);
            kafkaBridgeAssemblyOperator = new KafkaBridgeAssemblyOperator(vertx, pfa, certManager, passwordGenerator, resourceOperatorSupplier, config);
            kafkaRebalanceAssemblyOperator = new KafkaRebalanceAssemblyOperator(vertx, resourceOperatorSupplier, config);
        }

        @SuppressWarnings({ "rawtypes" })
        List<Future> futures = new ArrayList<>(config.getNamespaces().size());
        for (String namespace : config.getNamespaces()) {
            Promise<String> prom = Promise.promise();
            futures.add(prom.future());
            ClusterOperator operator = new ClusterOperator(namespace,
                    config,
                    client,
                    kafkaClusterOperations,
                    kafkaConnectClusterOperations,
                    kafkaMirrorMakerAssemblyOperator,
                    kafkaMirrorMaker2AssemblyOperator,
                    kafkaBridgeAssemblyOperator,
                    kafkaRebalanceAssemblyOperator,
                    resourceOperatorSupplier);
            vertx.deployVerticle(operator,
                res -> {
                    if (res.succeeded()) {
                        if (config.getCustomResourceSelector() != null) {
                            LOGGER.info("Cluster Operator verticle started in namespace {} with label selector {}", namespace, config.getCustomResourceSelector());
                        } else {
                            LOGGER.info("Cluster Operator verticle started in namespace {} without label selector", namespace);
                        }
                    } else {
                        LOGGER.error("Cluster Operator verticle in namespace {} failed to start", namespace, res.cause());
                    }
                    prom.handle(res);
                });
        }
        return CompositeFuture.join(futures);
    }

    /**
     * Utility method which waits until this instance of the operator is elected as a leader:
     *   - When it is not a leader, it will just wait
     *   - Once it is elected a leader, it will continue and start the ClusterOperator verticles
     *   - If it is removed as a leader, it will loop the operator container to start from the beginning
     *
     * When the leader election is disabled, it just completes the future without waiting for anything.
     *
     * @param client    Kubernetes client
     * @param config    Cluster Operator configuration
     */
    private static Future<Void> leaderElection(KubernetesClient client, ClusterOperatorConfig config)    {
        Promise<Void> leader = Promise.promise();

        if (config.getLeaderElectionConfig() != null) {
            LeaderElectionManager leaderElection = new LeaderElectionManager(
                    client, config.getLeaderElectionConfig(),
                    () -> {
                        // New leader => complete the future
                        LOGGER.info("I'm the new leader");
                        leader.complete();
                    },
                    () -> {
                        // Not a leader anymore
                        LOGGER.info("Stopped being a leader => exiting");
                        System.exit(0);
                    },
                    s -> {
                        // Do nothing
                    });

            LOGGER.info("Waiting to become a leader");
            leaderElection.start();
        } else {
            LOGGER.info("Leader election is not enabled");
            leader.complete();
        }

        return leader.future();
    }

    /**
     * If enabled in the configuration, it creates the cluster roles used by the operator
     *
     * @param vertx             Vertx instance
     * @param config            Cluster Operator configuration
     * @param client            Kubernetes client instance
     *
     * @return  Future which completes when the Cluster Roles are created
     *                  (or - if their creation is not enabled - it just completes without doing anything).
     */
    /*test*/ static Future<Void> maybeCreateClusterRoles(Vertx vertx, ClusterOperatorConfig config, KubernetesClient client)  {
        if (config.isCreateClusterRoles()) {
            @SuppressWarnings({ "rawtypes" })
            List<Future> futures = new ArrayList<>();
            ClusterRoleOperator cro = new ClusterRoleOperator(vertx, client);

            Map<String, String> clusterRoles = new HashMap<>(6);
            clusterRoles.put("strimzi-cluster-operator-namespaced", "020-ClusterRole-strimzi-cluster-operator-role.yaml");
            clusterRoles.put("strimzi-cluster-operator-global", "021-ClusterRole-strimzi-cluster-operator-role.yaml");
            clusterRoles.put("strimzi-kafka-broker", "030-ClusterRole-strimzi-kafka-broker.yaml");
            clusterRoles.put("strimzi-entity-operator", "031-ClusterRole-strimzi-entity-operator.yaml");
            clusterRoles.put("strimzi-kafka-client", "033-ClusterRole-strimzi-kafka-client.yaml");

            for (Map.Entry<String, String> clusterRole : clusterRoles.entrySet()) {
                LOGGER.info("Creating cluster role {}", clusterRole.getKey());

                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(Objects.requireNonNull(Main.class.getResourceAsStream("/cluster-roles/" + clusterRole.getValue())),
                                StandardCharsets.UTF_8))) {
                    String yaml = br.lines().collect(Collectors.joining(System.lineSeparator()));
                    ClusterRole role = ClusterRoleOperator.convertYamlToClusterRole(yaml);
                    @SuppressWarnings({ "rawtypes" })
                    Future fut = cro.reconcile(new Reconciliation("start-cluster-operator", "Deployment", config.getOperatorNamespace(), "cluster-operator"), role.getMetadata().getName(), role);
                    futures.add(fut);
                } catch (IOException e) {
                    LOGGER.error("Failed to create Cluster Roles.", e);
                    throw new RuntimeException(e);
                }

            }

            Promise<Void> returnPromise = Promise.promise();
            CompositeFuture.all(futures).onComplete(res -> {
                if (res.succeeded())    {
                    returnPromise.complete();
                } else  {
                    returnPromise.fail("Failed to create Cluster Roles.");
                }
            });

            return returnPromise.future();
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Start an HTTP health and metrics server
     *
     * @param vertx             Vertx instance
     * @param metricsProvider   Metrics Provider to get the metrics from
     *
     * @return Future which completes when the health and metrics webserver is started
     */
    private static Future<HttpServer> startHealthServer(Vertx vertx, MetricsProvider metricsProvider) {
        Promise<HttpServer> result = Promise.promise();

        vertx.createHttpServer()
                .requestHandler(request -> {
                    if (request.path().equals("/healthy")) {
                        request.response().setStatusCode(200).end();
                    } else if (request.path().equals("/ready")) {
                        request.response().setStatusCode(200).end();
                    } else if (request.path().equals("/metrics")) {
                        PrometheusMeterRegistry metrics = (PrometheusMeterRegistry) metricsProvider.meterRegistry();
                        request.response().setStatusCode(200)
                                .end(metrics.scrape());
                    }
                })
                .listen(HEALTH_SERVER_PORT, ar -> {
                    if (ar.succeeded()) {
                        LOGGER.info("Health and metrics server is ready on port {})", HEALTH_SERVER_PORT);
                    } else {
                        LOGGER.error("Failed to start health and metrics webserver on port {}", HEALTH_SERVER_PORT, ar.cause());
                    }
                    result.handle(ar);
                });

        return result.future();
    }
}
