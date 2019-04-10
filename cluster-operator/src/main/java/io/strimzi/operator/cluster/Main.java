/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRole;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaConnectList;
import io.strimzi.api.kafka.KafkaConnectS2IList;
import io.strimzi.api.kafka.KafkaMirrorMakerList;
import io.strimzi.api.kafka.model.DoneableKafkaConnect;
import io.strimzi.api.kafka.model.DoneableKafkaConnectS2I;
import io.strimzi.api.kafka.model.DoneableKafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.operator.assembly.KafkaAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaConnectAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaConnectS2IAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaMirrorMakerAssemblyOperator;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.operator.resource.BuildConfigOperator;
import io.strimzi.operator.common.operator.resource.ClusterRoleOperator;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentConfigOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.ImageStreamOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.PodDisruptionBudgetOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Main {
    private static final Logger log = LogManager.getLogger(Main.class.getName());

    static {
        try {
            Crds.registerCustomKinds();
        } catch (Error | RuntimeException t) {
            t.printStackTrace();
        }
    }

    public static void main(String[] args) {
        log.info("ClusterOperator {} is starting", Main.class.getPackage().getImplementationVersion());
        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(System.getenv());
        Vertx vertx = Vertx.vertx();
        KubernetesClient client = new DefaultKubernetesClient();

        maybeCreateClusterRoles(vertx, config, client).setHandler(crs -> {
            if (crs.succeeded())    {
                PlatformFeaturesAvailability.create(vertx, client).setHandler(pfa -> {
                    if (pfa.succeeded()) {
                        log.info("Environment facts gathered: {}", pfa.result());

                        run(vertx, client, pfa.result(), config).setHandler(ar -> {
                            if (ar.failed()) {
                                log.error("Unable to start operator for 1 or more namespace", ar.cause());
                                System.exit(1);
                            }
                        });
                    } else {
                        log.error("Failed to gather environment facts", pfa.cause());
                        System.exit(1);
                    }
                });
            } else  {
                log.error("Failed to create Cluster Roles", crs.cause());
                System.exit(1);
            }
        });
    }

    static CompositeFuture run(Vertx vertx, KubernetesClient client, PlatformFeaturesAvailability pfa, ClusterOperatorConfig config) {
        printEnvInfo();
        ServiceOperator serviceOperations = new ServiceOperator(vertx, client);
        ConfigMapOperator configMapOperations = new ConfigMapOperator(vertx, client);
        DeploymentOperator deploymentOperations = new DeploymentOperator(vertx, client);
        SecretOperator secretOperations = new SecretOperator(vertx, client);
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectList, DoneableKafkaConnect> kco =
                new CrdOperator<>(vertx, client, KafkaConnect.class, KafkaConnectList.class, DoneableKafkaConnect.class);
        CrdOperator<KubernetesClient, KafkaMirrorMaker, KafkaMirrorMakerList, DoneableKafkaMirrorMaker> kmmo =
                new CrdOperator<>(vertx, client, KafkaMirrorMaker.class, KafkaMirrorMakerList.class, DoneableKafkaMirrorMaker.class);
        NetworkPolicyOperator networkPolicyOperator = new NetworkPolicyOperator(vertx, client);
        PodDisruptionBudgetOperator podDisruptionBudgetOperator = new PodDisruptionBudgetOperator(vertx, client);
        ResourceOperatorSupplier resourceOperatorSupplier = new ResourceOperatorSupplier(vertx, client, pfa, config.getOperationTimeoutMs());

        OpenSslCertManager certManager = new OpenSslCertManager();
        KafkaAssemblyOperator kafkaClusterOperations = new KafkaAssemblyOperator(vertx, pfa,
                config.getOperationTimeoutMs(), certManager, resourceOperatorSupplier,
                config.versions(), config.getImagePullPolicy());
        KafkaConnectAssemblyOperator kafkaConnectClusterOperations = new KafkaConnectAssemblyOperator(vertx, pfa,
                certManager, kco, configMapOperations, deploymentOperations, serviceOperations, secretOperations,
                networkPolicyOperator, podDisruptionBudgetOperator, resourceOperatorSupplier, config.versions(), config.getImagePullPolicy());

        KafkaConnectS2IAssemblyOperator kafkaConnectS2IClusterOperations = null;
        if (pfa.hasBuilds() && pfa.hasApps() && pfa.hasImages()) {
            kafkaConnectS2IClusterOperations = createS2iOperator(vertx, client, pfa, serviceOperations,
                    configMapOperations, secretOperations, certManager, resourceOperatorSupplier, config.versions(), config.getImagePullPolicy());
        } else {
            log.info("The KafkaConnectS2I custom resource definition can only be used in environment which supports OpenShift build, image and apps APIs. These APIs do not seem to be supported in this environment.");
        }

        KafkaMirrorMakerAssemblyOperator kafkaMirrorMakerAssemblyOperator =
                new KafkaMirrorMakerAssemblyOperator(vertx, pfa, certManager, kmmo, secretOperations,
                        configMapOperations, networkPolicyOperator, deploymentOperations, serviceOperations,
                        podDisruptionBudgetOperator, resourceOperatorSupplier, config.versions(), config.getImagePullPolicy());

        List<Future> futures = new ArrayList<>();
        for (String namespace : config.getNamespaces()) {
            Future<String> fut = Future.future();
            futures.add(fut);
            ClusterOperator operator = new ClusterOperator(namespace,
                    config.getReconciliationIntervalMs(),
                    client,
                    kafkaClusterOperations,
                    kafkaConnectClusterOperations,
                    kafkaConnectS2IClusterOperations,
                    kafkaMirrorMakerAssemblyOperator);
            vertx.deployVerticle(operator,
                res -> {
                    if (res.succeeded()) {
                        log.info("Cluster Operator verticle started in namespace {}", namespace);
                    } else {
                        log.error("Cluster Operator verticle in namespace {} failed to start", namespace, res.cause());
                        System.exit(1);
                    }
                    fut.completer().handle(res);
                });
        }
        return CompositeFuture.join(futures);
    }

    private static KafkaConnectS2IAssemblyOperator createS2iOperator(Vertx vertx, KubernetesClient client, PlatformFeaturesAvailability pfa, ServiceOperator serviceOperations, ConfigMapOperator configMapOperations, SecretOperator secretOperations, OpenSslCertManager certManager, ResourceOperatorSupplier resourceOperatorSupplier, KafkaVersion.Lookup versions, ImagePullPolicy imagePullPolicy) {
        ImageStreamOperator imagesStreamOperations;
        BuildConfigOperator buildConfigOperations;
        DeploymentConfigOperator deploymentConfigOperations;
        CrdOperator<OpenShiftClient, KafkaConnectS2I, KafkaConnectS2IList, DoneableKafkaConnectS2I> kafkaConnectS2iCrdOperator;
        NetworkPolicyOperator networkPolicyOperator;
        PodDisruptionBudgetOperator podDisruptionBudgetOperator;
        KafkaConnectS2IAssemblyOperator kafkaConnectS2IClusterOperations;
        OpenShiftClient osClient = client.adapt(OpenShiftClient.class);
        imagesStreamOperations = new ImageStreamOperator(vertx, osClient);
        buildConfigOperations = new BuildConfigOperator(vertx, osClient);
        deploymentConfigOperations = new DeploymentConfigOperator(vertx, osClient);
        kafkaConnectS2iCrdOperator = new CrdOperator<>(vertx, osClient, KafkaConnectS2I.class, KafkaConnectS2IList.class, DoneableKafkaConnectS2I.class);
        networkPolicyOperator = new NetworkPolicyOperator(vertx, client);
        podDisruptionBudgetOperator = new PodDisruptionBudgetOperator(vertx, client);
        kafkaConnectS2IClusterOperations = new KafkaConnectS2IAssemblyOperator(vertx, pfa,
                certManager,
                kafkaConnectS2iCrdOperator,
                 configMapOperations, deploymentConfigOperations,
                serviceOperations, imagesStreamOperations, buildConfigOperations, secretOperations, networkPolicyOperator, podDisruptionBudgetOperator, resourceOperatorSupplier, versions, imagePullPolicy);
        return kafkaConnectS2IClusterOperations;
    }

    private static Future<Void> maybeCreateClusterRoles(Vertx vertx, ClusterOperatorConfig config, KubernetesClient client)  {
        if (config.isCreateClusterRoles()) {
            List<Future> futures = new ArrayList<>();
            ClusterRoleOperator cro = new ClusterRoleOperator(vertx, client);

            Map<String, String> clusterRoles = new HashMap<String, String>() {
                {
                    put("strimzi-cluster-operator-namespaced", "020-ClusterRole-strimzi-cluster-operator-role.yaml");
                    put("strimzi-cluster-operator-global", "021-ClusterRole-strimzi-cluster-operator-role.yaml");
                    put("strimzi-kafka-broker", "030-ClusterRole-strimzi-kafka-broker.yaml");
                    put("strimzi-entity-operator", "031-ClusterRole-strimzi-entity-operator.yaml");
                    put("strimzi-topic-operator", "032-ClusterRole-strimzi-topic-operator.yaml");
                }
            };

            for (Map.Entry<String, String> clusterRole : clusterRoles.entrySet()) {
                log.info("Creating cluster role {}", clusterRole.getKey());

                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(Main.class.getResourceAsStream("/cluster-roles/" + clusterRole.getValue()),
                                StandardCharsets.UTF_8))) {
                    String yaml = br.lines().collect(Collectors.joining(System.lineSeparator()));
                    KubernetesClusterRole role = cro.convertYamlToClusterRole(yaml);
                    Future fut = cro.reconcile(role.getMetadata().getName(), role);
                    futures.add(fut);
                } catch (IOException e) {
                    log.error("Failed to create Cluster Roles.", e);
                    throw new RuntimeException(e);
                }

            }

            Future returnFuture = Future.future();
            CompositeFuture.all(futures).setHandler(res -> {
                if (res.succeeded())    {
                    returnFuture.complete();
                } else  {
                    returnFuture.fail("Failed to create Cluster Roles.");
                }
            });

            return returnFuture;
        } else {
            return Future.succeededFuture();
        }
    }

    static void printEnvInfo() {
        Map<String, String> m = new HashMap<>(System.getenv());
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry: m.entrySet()) {
            sb.append("\t").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        log.info("Using config:\n" + sb.toString());
    }
}
