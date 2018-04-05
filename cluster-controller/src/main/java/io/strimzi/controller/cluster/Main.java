/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.client.OpenShiftClient;

import io.strimzi.controller.cluster.operator.assembly.KafkaAssemblyOperator;
import io.strimzi.controller.cluster.operator.assembly.KafkaConnectAssemblyOperator;
import io.strimzi.controller.cluster.operator.assembly.KafkaConnectS2IAssemblyOperator;
import io.strimzi.controller.cluster.operator.resource.KafkaSetOperator;
import io.strimzi.controller.cluster.operator.resource.ZookeeperSetOperator;
import io.strimzi.controller.cluster.operator.resource.BuildConfigOperator;
import io.strimzi.controller.cluster.operator.resource.ConfigMapOperator;
import io.strimzi.controller.cluster.operator.resource.DeploymentConfigOperator;
import io.strimzi.controller.cluster.operator.resource.DeploymentOperator;
import io.strimzi.controller.cluster.operator.resource.ImageStreamOperator;
import io.strimzi.controller.cluster.operator.resource.PvcOperator;
import io.strimzi.controller.cluster.operator.resource.ServiceOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class.getName());

    public static void main(String[] args) {
        ClusterControllerConfig config = ClusterControllerConfig.fromMap(System.getenv());
        Vertx vertx = Vertx.vertx();
        KubernetesClient client = new DefaultKubernetesClient();

        isOnOpenShift(vertx, client).setHandler(os -> {
            if (os.succeeded()) {
                run(vertx, client, os.result().booleanValue(), config).setHandler(ar -> {
                    if (ar.failed()) {
                        log.error("Unable to start controller for 1 or more namespace", ar.cause());
                        System.exit(1);
                    }
                });
            } else {
                log.error("Failed to distinguish between Kubernetes and OpenShift", os.cause());
                System.exit(1);
            }
        });
    }

    static CompositeFuture run(Vertx vertx, KubernetesClient client, boolean isOpenShift, ClusterControllerConfig config) {
        ServiceOperator serviceOperations = new ServiceOperator(vertx, client);
        ZookeeperSetOperator zookeeperSetOperations = new ZookeeperSetOperator(vertx, client);
        KafkaSetOperator kafkaSetOperations = new KafkaSetOperator(vertx, client);
        ConfigMapOperator configMapOperations = new ConfigMapOperator(vertx, client);
        PvcOperator pvcOperations = new PvcOperator(vertx, client);
        DeploymentOperator deploymentOperations = new DeploymentOperator(vertx, client);

        KafkaAssemblyOperator kafkaClusterOperations = new KafkaAssemblyOperator(vertx, isOpenShift, config.getOperationTimeoutMs(), configMapOperations, serviceOperations, zookeeperSetOperations, kafkaSetOperations, pvcOperations, deploymentOperations);
        KafkaConnectAssemblyOperator kafkaConnectClusterOperations = new KafkaConnectAssemblyOperator(vertx, isOpenShift, configMapOperations, deploymentOperations, serviceOperations);

        DeploymentConfigOperator deploymentConfigOperations = null;
        ImageStreamOperator imagesStreamOperations = null;
        BuildConfigOperator buildConfigOperations = null;
        KafkaConnectS2IAssemblyOperator kafkaConnectS2IClusterOperations = null;
        if (isOpenShift) {
            imagesStreamOperations = new ImageStreamOperator(vertx, client.adapt(OpenShiftClient.class));
            buildConfigOperations = new BuildConfigOperator(vertx, client.adapt(OpenShiftClient.class));
            deploymentConfigOperations = new DeploymentConfigOperator(vertx, client.adapt(OpenShiftClient.class));
            kafkaConnectS2IClusterOperations = new KafkaConnectS2IAssemblyOperator(vertx, isOpenShift,
                    configMapOperations, deploymentConfigOperations,
                    serviceOperations, imagesStreamOperations, buildConfigOperations);
        }

        List<Future> futures = new ArrayList<>();
        for (String namespace : config.getNamespaces()) {
            Future<String> fut = Future.future();
            futures.add(fut);
            ClusterController controller = new ClusterController(namespace,
                    config.getReconciliationIntervalMs(),
                    client,
                    kafkaClusterOperations,
                    kafkaConnectClusterOperations,
                    kafkaConnectS2IClusterOperations);
            vertx.deployVerticle(controller,
                res -> {
                    if (res.succeeded()) {
                        log.info("Cluster Controller verticle started in namespace {}", namespace);
                    } else {
                        log.error("Cluster Controller verticle in namespace {} failed to start", namespace, res.cause());
                        System.exit(1);
                    }
                    fut.completer().handle(res);
                });
        }
        return CompositeFuture.join(futures);
    }

    static Future<Boolean> isOnOpenShift(Vertx vertx, KubernetesClient client)  {
        URL kubernetesApi = client.getMasterUrl();
        Future<Boolean> fut = Future.future();

        HttpClientOptions httpClientOptions = new HttpClientOptions();
        httpClientOptions.setDefaultHost(kubernetesApi.getHost());

        if (kubernetesApi.getPort() == -1) {
            httpClientOptions.setDefaultPort(kubernetesApi.getDefaultPort());
        } else {
            httpClientOptions.setDefaultPort(kubernetesApi.getPort());
        }

        if (kubernetesApi.getProtocol().equals("https")) {
            httpClientOptions.setSsl(true);
            httpClientOptions.setTrustAll(true);
        }

        HttpClient httpClient = vertx.createHttpClient(httpClientOptions);

        httpClient.getNow("/oapi", res -> {
            if (res.statusCode() == 200) {
                log.debug("{} returned {}. We are on OpenShift.", res.request().absoluteURI(), res.statusCode());
                // We should be on OpenShift based on the /oapi result. We can now safely try isAdaptable() to be 100% sure.
                Boolean isOpenShift = Boolean.TRUE.equals(client.isAdaptable(OpenShiftClient.class));
                fut.complete(isOpenShift);
            } else {
                log.debug("{} returned {}. We are not on OpenShift.", res.request().absoluteURI(), res.statusCode());
                fut.complete(Boolean.FALSE);
            }
        });

        return fut;
    }
}
