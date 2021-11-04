/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.operator.assembly.KafkaAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaBridgeAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaConnectAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaMirrorMakerAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaMirrorMaker2AssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaRebalanceAssemblyOperator;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.resource.ClusterRoleOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.Security;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.vertx.core.VertxOptions;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SuppressFBWarnings("DM_EXIT")
public class Main {
    private static final Logger LOGGER = LogManager.getLogger(Main.class.getName());

    static {
        try {
            Crds.registerCustomKinds();
        } catch (Error | RuntimeException t) {
            t.printStackTrace();
        }
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:MethodLength"})
    public static void main(String[] args) {
        LOGGER.info("ClusterOperator {} is starting", Main.class.getPackage().getImplementationVersion());
        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(System.getenv());
        LOGGER.info("Cluster Operator configuration is {}", config);

        String dnsCacheTtl = System.getenv("STRIMZI_DNS_CACHE_TTL") == null ? "30" : System.getenv("STRIMZI_DNS_CACHE_TTL");
        Security.setProperty("networkaddress.cache.ttl", dnsCacheTtl);

        //Setup Micrometer metrics options
        VertxOptions options = new VertxOptions().setMetricsOptions(
                new MicrometerMetricsOptions()
                        .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                        .setJvmMetricsEnabled(true)
                        .setEnabled(true))
                        .setBlockedThreadCheckInterval(Duration.ofSeconds(121).toNanos());
        Vertx vertx = Vertx.vertx(options);
        
        KubernetesClient client = new DefaultKubernetesClient();

        maybeCreateClusterRoles(vertx, config, client).onComplete(crs -> {
            if (crs.succeeded())    {
                PlatformFeaturesAvailability.create(vertx, client).onComplete(pfa -> {
                    if (pfa.succeeded()) {
                        LOGGER.info("Environment facts gathered: {}", pfa.result());

                        PlatformFeaturesAvailability pfaRes = pfa.result();

                        Util.printEnvInfo();

                        ResourceOperatorSupplier resourceOperatorSupplier = new ResourceOperatorSupplier(vertx, client, pfaRes, config.featureGates(), config.getOperationTimeoutMs());

                        OpenSslCertManager certManager = new OpenSslCertManager();
                        PasswordGenerator passwordGenerator = new PasswordGenerator(12,
                            "abcdefghijklmnopqrstuvwxyz" +
                                "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
                            "abcdefghijklmnopqrstuvwxyz" +
                                "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                                "0123456789");
                        KafkaAssemblyOperator kafkaClusterOperations = new KafkaAssemblyOperator(vertx, pfaRes,
                            certManager, passwordGenerator, resourceOperatorSupplier, config);
                        KafkaConnectAssemblyOperator kafkaConnectClusterOperations = new KafkaConnectAssemblyOperator(vertx, pfaRes,
                            resourceOperatorSupplier, config);

                        KafkaMirrorMaker2AssemblyOperator kafkaMirrorMaker2AssemblyOperator =
                            new KafkaMirrorMaker2AssemblyOperator(vertx, pfaRes, resourceOperatorSupplier, config);

                        KafkaMirrorMakerAssemblyOperator kafkaMirrorMakerAssemblyOperator =
                            new KafkaMirrorMakerAssemblyOperator(vertx, pfaRes, certManager, passwordGenerator, resourceOperatorSupplier, config);

                        KafkaBridgeAssemblyOperator kafkaBridgeAssemblyOperator =
                            new KafkaBridgeAssemblyOperator(vertx, pfaRes, certManager, passwordGenerator, resourceOperatorSupplier, config);

                        KafkaRebalanceAssemblyOperator kafkaRebalanceAssemblyOperator =
                            new KafkaRebalanceAssemblyOperator(vertx, pfaRes, resourceOperatorSupplier, config);

                        // for each namespace create Cluster Operator verticle that watches resources
                        List<String> namespaces = new ArrayList<>(config.getNamespaces());

                        // always 3 in a row
                        if (namespaces.size() % 3 == 0) {
                            for (int i = 0; i < namespaces.size(); i++) {
                                // always run two cluster operator verticles at the time (where one waits on another) - elimination of CPU exhaustion
                                final String namespaceName = namespaces.get(i);
                                final String nextNamespaceName = namespaces.get(i + 1);
                                final String nextNextNamespaceName = namespaces.get(i + 2);
                                run(vertx, client, pfaRes, config, kafkaClusterOperations, kafkaConnectClusterOperations,
                                    kafkaMirrorMaker2AssemblyOperator, kafkaMirrorMakerAssemblyOperator, kafkaBridgeAssemblyOperator,
                                    kafkaRebalanceAssemblyOperator, resourceOperatorSupplier, namespaceName).onComplete(ar -> {
                                        if (ar.failed()) {
                                            LOGGER.error("Unable to start Cluster Operator Verticle for namespace: {}", namespaceName, ar.cause());
                                            System.exit(1);
                                        }
                                    }).compose(ignore ->
                                    run(vertx, client, pfaRes, config, kafkaClusterOperations, kafkaConnectClusterOperations,
                                        kafkaMirrorMaker2AssemblyOperator, kafkaMirrorMakerAssemblyOperator, kafkaBridgeAssemblyOperator,
                                        kafkaRebalanceAssemblyOperator, resourceOperatorSupplier, nextNamespaceName).onComplete(ar -> {
                                            if (ar.failed()) {
                                                LOGGER.error("Unable to start Cluster Operator Verticle for namespace: {}", nextNamespaceName, ar.cause());
                                                System.exit(1);
                                            }
                                        })).compose(ignore -> run(vertx, client, pfaRes, config, kafkaClusterOperations, kafkaConnectClusterOperations,
                                        kafkaMirrorMaker2AssemblyOperator, kafkaMirrorMakerAssemblyOperator, kafkaBridgeAssemblyOperator,
                                        kafkaRebalanceAssemblyOperator, resourceOperatorSupplier, nextNextNamespaceName).onComplete(ar -> {
                                            if (ar.failed()) {
                                                LOGGER.error("Unable to start Cluster Operator Verticle for namespace: {}", nextNextNamespaceName, ar.cause());
                                                System.exit(1);
                                            }
                                        })
                                        .onSuccess(handler -> LOGGER.info("Successfully deployed Cluster Operator verticles for namespaces:{},{}, {}", namespaceName, nextNamespaceName, nextNextNamespaceName))
                                        .onFailure(handler -> LOGGER.error("Some error occurred:{}", handler.getCause())));
                                // increment by 2 because we already compose two namespaces
                                i += 2;
                            }
                        // the last 2 only two in a row
                        } else if (namespaces.size() % 3 == 2) {
                            for (int i = 0; i < namespaces.size(); i++) {
                                // last iteration
                                if (i == namespaces.size() - 2) {
                                    final String namespaceName = namespaces.get(i);
                                    final String nextNamespaceName = namespaces.get(i + 1);

                                    run(vertx, client, pfaRes, config, kafkaClusterOperations, kafkaConnectClusterOperations,
                                        kafkaMirrorMaker2AssemblyOperator, kafkaMirrorMakerAssemblyOperator, kafkaBridgeAssemblyOperator,
                                        kafkaRebalanceAssemblyOperator, resourceOperatorSupplier, namespaceName).onComplete(ar -> {
                                            if (ar.failed()) {
                                                LOGGER.error("Unable to start Cluster Operator Verticle for namespace: {}", namespaceName, ar.cause());
                                                System.exit(1);
                                            }
                                        }).compose(ignore ->
                                        run(vertx, client, pfaRes, config, kafkaClusterOperations, kafkaConnectClusterOperations,
                                            kafkaMirrorMaker2AssemblyOperator, kafkaMirrorMakerAssemblyOperator, kafkaBridgeAssemblyOperator,
                                            kafkaRebalanceAssemblyOperator, resourceOperatorSupplier, nextNamespaceName).onComplete(ar -> {
                                                if (ar.failed()) {
                                                    LOGGER.error("Unable to start Cluster Operator Verticle for namespace: {}", nextNamespaceName, ar.cause());
                                                    System.exit(1);
                                                }
                                            }))
                                        .onSuccess(handler -> LOGGER.info("Successfully deployed Cluster Operator verticles for namespaces:{},{}", namespaceName, nextNamespaceName))
                                        .onFailure(handler -> LOGGER.error("Some error occurred:{}", handler.getCause()));
                                    // increment by 1 because we already compose one namespace
                                    i++;
                                } else {
                                    // always run two cluster operator verticles at the time (where one waits on another) - elimination of CPU exhaustion
                                    final String namespaceName = namespaces.get(i);
                                    final String nextNamespaceName = namespaces.get(i + 1);
                                    final String nextNextNamespaceName = namespaces.get(i + 2);
                                    run(vertx, client, pfaRes, config, kafkaClusterOperations, kafkaConnectClusterOperations,
                                        kafkaMirrorMaker2AssemblyOperator, kafkaMirrorMakerAssemblyOperator, kafkaBridgeAssemblyOperator,
                                        kafkaRebalanceAssemblyOperator, resourceOperatorSupplier, namespaceName).onComplete(ar -> {
                                            if (ar.failed()) {
                                                LOGGER.error("Unable to start Cluster Operator Verticle for namespace: {}", namespaceName, ar.cause());
                                                System.exit(1);
                                            }
                                        }).compose(ignore ->
                                        run(vertx, client, pfaRes, config, kafkaClusterOperations, kafkaConnectClusterOperations,
                                            kafkaMirrorMaker2AssemblyOperator, kafkaMirrorMakerAssemblyOperator, kafkaBridgeAssemblyOperator,
                                            kafkaRebalanceAssemblyOperator, resourceOperatorSupplier, nextNamespaceName).onComplete(ar -> {
                                                if (ar.failed()) {
                                                    LOGGER.error("Unable to start Cluster Operator Verticle for namespace: {}", nextNamespaceName, ar.cause());
                                                    System.exit(1);
                                                }
                                            })).compose(ignore -> run(vertx, client, pfaRes, config, kafkaClusterOperations, kafkaConnectClusterOperations,
                                        kafkaMirrorMaker2AssemblyOperator, kafkaMirrorMakerAssemblyOperator, kafkaBridgeAssemblyOperator,
                                        kafkaRebalanceAssemblyOperator, resourceOperatorSupplier, nextNextNamespaceName).onComplete(ar -> {
                                            if (ar.failed()) {
                                                LOGGER.error("Unable to start Cluster Operator Verticle for namespace: {}", nextNextNamespaceName, ar.cause());
                                                System.exit(1);
                                            }
                                        })
                                        .onSuccess(handler -> LOGGER.info("Successfully deployed Cluster Operator verticles for namespaces:{},{}, {}", namespaceName, nextNamespaceName, nextNextNamespaceName))
                                        .onFailure(handler -> LOGGER.error("Some error occurred:{}", handler.getCause())));
                                    // increment by 2 because we already compose two namespaces
                                    i += 2;
                                }
                            }
                        // the last 1 only in a row
                        } else {
                            for (int i = 0; i < namespaces.size(); i++) {
                                final String namespaceName = namespaces.get(i);
                                // last iteration
                                if (i == namespaces.size() - 1) {
                                    run(vertx, client, pfaRes, config, kafkaClusterOperations, kafkaConnectClusterOperations,
                                        kafkaMirrorMaker2AssemblyOperator, kafkaMirrorMakerAssemblyOperator, kafkaBridgeAssemblyOperator,
                                        kafkaRebalanceAssemblyOperator, resourceOperatorSupplier, namespaceName).onComplete(ar -> {
                                            if (ar.failed()) {
                                                LOGGER.error("Unable to start Cluster Operator Verticle for namespace: {}", namespaceName, ar.cause());
                                                System.exit(1);
                                            }
                                        })
                                        .onSuccess(handler -> LOGGER.info("Successfully deployed Cluster Operator verticle for namespaces:{}", namespaceName))
                                        .onFailure(handler -> LOGGER.error("Some error occurred:{}", handler.getCause()));
                                } else {
                                    // always run two cluster operator verticles at the time (where one waits on another) - elimination of CPU exhaustion
                                    final String nextNamespaceName = namespaces.get(i + 1);
                                    final String nextNextNamespaceName = namespaces.get(i + 2);
                                    run(vertx, client, pfaRes, config, kafkaClusterOperations, kafkaConnectClusterOperations,
                                        kafkaMirrorMaker2AssemblyOperator, kafkaMirrorMakerAssemblyOperator, kafkaBridgeAssemblyOperator,
                                        kafkaRebalanceAssemblyOperator, resourceOperatorSupplier, namespaceName).onComplete(ar -> {
                                            if (ar.failed()) {
                                                LOGGER.error("Unable to start Cluster Operator Verticle for namespace: {}", namespaceName, ar.cause());
                                                System.exit(1);
                                            }
                                        }).compose(ignore ->
                                        run(vertx, client, pfaRes, config, kafkaClusterOperations, kafkaConnectClusterOperations,
                                            kafkaMirrorMaker2AssemblyOperator, kafkaMirrorMakerAssemblyOperator, kafkaBridgeAssemblyOperator,
                                            kafkaRebalanceAssemblyOperator, resourceOperatorSupplier, nextNamespaceName).onComplete(ar -> {
                                                if (ar.failed()) {
                                                    LOGGER.error("Unable to start Cluster Operator Verticle for namespace: {}", nextNamespaceName, ar.cause());
                                                    System.exit(1);
                                                }
                                            })).compose(ignore -> run(vertx, client, pfaRes, config, kafkaClusterOperations, kafkaConnectClusterOperations,
                                        kafkaMirrorMaker2AssemblyOperator, kafkaMirrorMakerAssemblyOperator, kafkaBridgeAssemblyOperator,
                                        kafkaRebalanceAssemblyOperator, resourceOperatorSupplier, nextNextNamespaceName).onComplete(ar -> {
                                            if (ar.failed()) {
                                                LOGGER.error("Unable to start Cluster Operator Verticle for namespace: {}", nextNextNamespaceName, ar.cause());
                                                System.exit(1);
                                            }
                                        })
                                        .onSuccess(handler -> LOGGER.info("Successfully deployed Cluster Operator verticles for namespaces:{},{}, {}", namespaceName, nextNamespaceName, nextNextNamespaceName))
                                        .onFailure(handler -> LOGGER.error("Some error occurred:{}", handler.getCause())));
                                    // increment by 2 because we already compose two namespaces
                                    i += 2;
                                }
                            }
                        }
                    } else {
                        LOGGER.error("Failed to gather environment facts", pfa.cause());
                        System.exit(1);
                    }
                });
            } else  {
                LOGGER.error("Failed to create Cluster Roles", crs.cause());
                System.exit(1);
            }
        });
    }

    static Future<String> run(Vertx vertx, KubernetesClient client, PlatformFeaturesAvailability pfa, ClusterOperatorConfig config,
                    KafkaAssemblyOperator kafkaClusterOperations, KafkaConnectAssemblyOperator kafkaConnectClusterOperations,
                    KafkaMirrorMaker2AssemblyOperator kafkaMirrorMaker2AssemblyOperator, KafkaMirrorMakerAssemblyOperator kafkaMirrorMakerAssemblyOperator,
                    KafkaBridgeAssemblyOperator kafkaBridgeAssemblyOperator, KafkaRebalanceAssemblyOperator kafkaRebalanceAssemblyOperator,
                    ResourceOperatorSupplier resourceOperatorSupplier, String namespace) {
        Promise<String> prom = Promise.promise();
        Future<String> future = prom.future();

        ClusterOperator operator = new ClusterOperator(namespace,
                config,
                client,
                kafkaClusterOperations,
                kafkaConnectClusterOperations,
                kafkaMirrorMakerAssemblyOperator,
                kafkaMirrorMaker2AssemblyOperator,
                kafkaBridgeAssemblyOperator,
                kafkaRebalanceAssemblyOperator,
                resourceOperatorSupplier.metricsProvider);

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
                    prom.fail("Failed to create Cluster Operator verticle in namespace " + namespace + ".");
                    System.exit(1);
                }
                prom.handle(res);
            });

        return future;
    }

    /*test*/ static Future<Void> maybeCreateClusterRoles(Vertx vertx, ClusterOperatorConfig config, KubernetesClient client)  {
        if (config.isCreateClusterRoles()) {
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
                        new InputStreamReader(Main.class.getResourceAsStream("/cluster-roles/" + clusterRole.getValue()),
                                StandardCharsets.UTF_8))) {
                    String yaml = br.lines().collect(Collectors.joining(System.lineSeparator()));
                    ClusterRole role = ClusterRoleOperator.convertYamlToClusterRole(yaml);
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
}
