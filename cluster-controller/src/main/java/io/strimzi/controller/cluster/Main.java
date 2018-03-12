/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.client.OpenShiftClient;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.controller.cluster.operations.cluster.KafkaClusterOperations;
import io.strimzi.controller.cluster.operations.cluster.KafkaConnectClusterOperations;
import io.strimzi.controller.cluster.operations.cluster.KafkaConnectS2IClusterOperations;
import io.strimzi.controller.cluster.operations.resource.BuildConfigOperations;
import io.strimzi.controller.cluster.operations.resource.ConfigMapOperations;
import io.strimzi.controller.cluster.operations.resource.DeploymentConfigOperations;
import io.strimzi.controller.cluster.operations.resource.DeploymentOperations;
import io.strimzi.controller.cluster.operations.resource.EndpointOperations;
import io.strimzi.controller.cluster.operations.resource.ImageStreamOperations;
import io.strimzi.controller.cluster.operations.resource.PodOperations;
import io.strimzi.controller.cluster.operations.resource.PvcOperations;
import io.strimzi.controller.cluster.operations.resource.ServiceOperations;
import io.strimzi.controller.cluster.operations.resource.StatefulSetOperations;
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
        Vertx vertx = Vertx.vertx();
        KubernetesClient client = new DefaultKubernetesClient();

        isOnOpenShift(vertx, client).setHandler(os -> {
            if (os.succeeded()) {
                run(vertx, client, os.result().booleanValue(), System.getenv()).setHandler(ar -> {
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

    static CompositeFuture run(Vertx vertx, KubernetesClient client, boolean isOpenShift, Map<String, String> env) {
        ClusterControllerConfig config = ClusterControllerConfig.fromMap(env);

        ServiceOperations serviceOperations = new ServiceOperations(vertx, client);
        StatefulSetOperations statefulSetOperations = new StatefulSetOperations(vertx, client);
        ConfigMapOperations configMapOperations = new ConfigMapOperations(vertx, client);
        PvcOperations pvcOperations = new PvcOperations(vertx, client);
        DeploymentOperations deploymentOperations = new DeploymentOperations(vertx, client);
        PodOperations podOperations = new PodOperations(vertx, client);
        EndpointOperations endpointOperations = new EndpointOperations(vertx, client);

        KafkaClusterOperations kafkaClusterOperations = new KafkaClusterOperations(vertx, isOpenShift, config.getOperationTimeoutMs(), configMapOperations, serviceOperations, statefulSetOperations, pvcOperations, podOperations, endpointOperations, deploymentOperations);
        KafkaConnectClusterOperations kafkaConnectClusterOperations = new KafkaConnectClusterOperations(vertx, isOpenShift, configMapOperations, deploymentOperations, serviceOperations);

        DeploymentConfigOperations deploymentConfigOperations = null;
        ImageStreamOperations imagesStreamOperations = null;
        BuildConfigOperations buildConfigOperations = null;
        KafkaConnectS2IClusterOperations kafkaConnectS2IClusterOperations = null;
        if (isOpenShift) {
            imagesStreamOperations = new ImageStreamOperations(vertx, client.adapt(OpenShiftClient.class));
            buildConfigOperations = new BuildConfigOperations(vertx, client.adapt(OpenShiftClient.class));
            deploymentConfigOperations = new DeploymentConfigOperations(vertx, client.adapt(OpenShiftClient.class));
            kafkaConnectS2IClusterOperations = new KafkaConnectS2IClusterOperations(vertx, isOpenShift,
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
            if (res.statusCode() == HttpResponseStatus.OK.code()) {
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
