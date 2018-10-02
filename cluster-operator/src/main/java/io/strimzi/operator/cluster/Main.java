/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaMirrorMakerList;
import io.strimzi.api.kafka.model.DoneableKafkaConnect;
import io.strimzi.api.kafka.model.DoneableKafkaConnectS2I;
import io.strimzi.api.kafka.KafkaConnectAssemblyList;
import io.strimzi.api.kafka.KafkaConnectS2IAssemblyList;
import io.strimzi.api.kafka.model.DoneableKafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.cluster.operator.assembly.KafkaAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaConnectAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaConnectS2IAssemblyOperator;
import io.strimzi.operator.cluster.operator.assembly.KafkaMirrorMakerAssemblyOperator;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.operator.resource.BuildConfigOperator;
import io.strimzi.operator.common.operator.resource.ConfigMapOperator;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.DeploymentConfigOperator;
import io.strimzi.operator.common.operator.resource.DeploymentOperator;
import io.strimzi.operator.common.operator.resource.ImageStreamOperator;
import io.strimzi.operator.common.operator.resource.NetworkPolicyOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        log.info("ClusterOperator is starting");
        ClusterOperatorConfig config = ClusterOperatorConfig.fromMap(System.getenv());
        Vertx vertx = Vertx.vertx();
        KubernetesClient client = new DefaultKubernetesClient();

        isOnOpenShift(vertx, client).setHandler(os -> {
            if (os.succeeded()) {
                run(vertx, client, os.result().booleanValue(), config).setHandler(ar -> {
                    if (ar.failed()) {
                        log.error("Unable to start operator for 1 or more namespace", ar.cause());
                        System.exit(1);
                    }
                });
            } else {
                log.error("Failed to distinguish between Kubernetes and OpenShift", os.cause());
                System.exit(1);
            }
        });
    }

    static CompositeFuture run(Vertx vertx, KubernetesClient client, boolean isOpenShift, ClusterOperatorConfig config) {
        printEnvInfo();
        ServiceOperator serviceOperations = new ServiceOperator(vertx, client);
        ConfigMapOperator configMapOperations = new ConfigMapOperator(vertx, client);
        DeploymentOperator deploymentOperations = new DeploymentOperator(vertx, client);
        SecretOperator secretOperations = new SecretOperator(vertx, client);
        CrdOperator<KubernetesClient, KafkaConnect, KafkaConnectAssemblyList, DoneableKafkaConnect> kco =
                new CrdOperator<>(vertx, client, KafkaConnect.class, KafkaConnectAssemblyList.class, DoneableKafkaConnect.class);
        CrdOperator<KubernetesClient, KafkaMirrorMaker, KafkaMirrorMakerList, DoneableKafkaMirrorMaker> kmmo =
                new CrdOperator<>(vertx, client, KafkaMirrorMaker.class, KafkaMirrorMakerList.class, DoneableKafkaMirrorMaker.class);
        NetworkPolicyOperator networkPolicyOperator = new NetworkPolicyOperator(vertx, client);

        OpenSslCertManager certManager = new OpenSslCertManager();
        KafkaAssemblyOperator kafkaClusterOperations = new KafkaAssemblyOperator(vertx, isOpenShift,
                config.getOperationTimeoutMs(), certManager, new ResourceOperatorSupplier(vertx, client, isOpenShift, config.getOperationTimeoutMs()));
        KafkaConnectAssemblyOperator kafkaConnectClusterOperations = new KafkaConnectAssemblyOperator(vertx, isOpenShift, certManager, kco, configMapOperations, deploymentOperations, serviceOperations, secretOperations, networkPolicyOperator);

        KafkaConnectS2IAssemblyOperator kafkaConnectS2IClusterOperations = null;
        CrdOperator<OpenShiftClient, KafkaConnectS2I, KafkaConnectS2IAssemblyList, DoneableKafkaConnectS2I> kafkaConnectS2iCrdOperator = null;
        if (isOpenShift) {
            kafkaConnectS2IClusterOperations = createS2iOperator(vertx, client, isOpenShift, serviceOperations, configMapOperations, secretOperations, certManager);
        } else {
            maybeLogS2iOnKubeWarning(vertx, client);
        }

        KafkaMirrorMakerAssemblyOperator kafkaMirrorMakerAssemblyOperator =
                new KafkaMirrorMakerAssemblyOperator(vertx, isOpenShift, certManager, kmmo, secretOperations, configMapOperations, networkPolicyOperator, deploymentOperations, serviceOperations);

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

    private static void maybeLogS2iOnKubeWarning(Vertx vertx, KubernetesClient client) {
        try {
            // Check the KafkaConnectS2I isn't installed and whinge if it is
            CustomResourceDefinition crd = client.customResourceDefinitions().withName(KafkaConnectS2I.CRD_NAME).get();
            if (crd != null) {
                log.warn("The KafkaConnectS2I custom resource definition can only be installed on OpenShift, because plain Kubernetes doesn't support S2I. " +
                        "Execute 'kubectl delete crd " + KafkaConnectS2I.CRD_NAME + "' to remove this warning.");
            }
        } catch (KubernetesClientException e) {

        }
    }

    private static KafkaConnectS2IAssemblyOperator createS2iOperator(Vertx vertx, KubernetesClient client, boolean isOpenShift, ServiceOperator serviceOperations, ConfigMapOperator configMapOperations, SecretOperator secretOperations, OpenSslCertManager certManager) {
        ImageStreamOperator imagesStreamOperations;
        BuildConfigOperator buildConfigOperations;
        DeploymentConfigOperator deploymentConfigOperations;
        CrdOperator<OpenShiftClient, KafkaConnectS2I, KafkaConnectS2IAssemblyList, DoneableKafkaConnectS2I> kafkaConnectS2iCrdOperator;
        NetworkPolicyOperator networkPolicyOperator;
        KafkaConnectS2IAssemblyOperator kafkaConnectS2IClusterOperations;
        OpenShiftClient osClient = client.adapt(OpenShiftClient.class);
        imagesStreamOperations = new ImageStreamOperator(vertx, osClient);
        buildConfigOperations = new BuildConfigOperator(vertx, osClient);
        deploymentConfigOperations = new DeploymentConfigOperator(vertx, osClient);
        kafkaConnectS2iCrdOperator = new CrdOperator<>(vertx, osClient, KafkaConnectS2I.class, KafkaConnectS2IAssemblyList.class, DoneableKafkaConnectS2I.class);
        networkPolicyOperator = new NetworkPolicyOperator(vertx, client);
        kafkaConnectS2IClusterOperations = new KafkaConnectS2IAssemblyOperator(vertx, isOpenShift,
                certManager,
                kafkaConnectS2iCrdOperator,
                 configMapOperations, deploymentConfigOperations,
                serviceOperations, imagesStreamOperations, buildConfigOperations, secretOperations, networkPolicyOperator);
        return kafkaConnectS2IClusterOperations;
    }

    static Future<Boolean> isOnOpenShift(Vertx vertx, KubernetesClient client)  {
        if (client.isAdaptable(OkHttpClient.class)) {
            OkHttpClient ok = client.adapt(OkHttpClient.class);
            Future<Boolean> fut = Future.future();

            vertx.executeBlocking(request -> {
                try (Response resp = ok.newCall(new Request.Builder().get().url(client.getMasterUrl().toString() + "oapi").build()).execute()) {
                    if (resp.code() == 200) {
                        log.debug("{} returned {}. We are on OpenShift.", resp.request().url(), resp.code());
                        // We should be on OpenShift based on the /oapi result. We can now safely try isAdaptable() to be 100% sure.
                        Boolean isOpenShift = Boolean.TRUE.equals(client.isAdaptable(OpenShiftClient.class));
                        request.complete(isOpenShift);
                    } else {
                        log.debug("{} returned {}. We are not on OpenShift.", resp.request().url(), resp.code());
                        request.complete(Boolean.FALSE);
                    }
                } catch (IOException e) {
                    log.error("OpenShift detection failed", e);
                    request.fail(e);
                }
            }, fut.completer());

            return fut;
        } else {
            log.error("Cannot adapt KubernetesClient to OkHttpClient");
            return Future.failedFuture("Cannot adapt KubernetesClient to OkHttpClient");
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
