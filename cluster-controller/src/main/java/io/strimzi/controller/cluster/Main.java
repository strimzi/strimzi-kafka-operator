/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.client.OpenShiftClient;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class.getName());

    public static void main(String[] args) {
        run(Vertx.vertx(), new DefaultKubernetesClient(), System.getenv()).setHandler(ar -> {
            if (ar.failed()) {
                log.error("Unable to start controller for 1 or more namespace", ar.cause());
                System.exit(1);
            }
        });
    }

    static CompositeFuture run(Vertx vertx, KubernetesClient client, Map<String, String> env) {
        ServiceOperations serviceOperations = new ServiceOperations(vertx, client);
        StatefulSetOperations statefulSetOperations = new StatefulSetOperations(vertx, client);
        ConfigMapOperations configMapOperations = new ConfigMapOperations(vertx, client);
        PvcOperations pvcOperations = new PvcOperations(vertx, client);
        DeploymentOperations deploymentOperations = new DeploymentOperations(vertx, client);
        PodOperations podOperations = new PodOperations(vertx, client);
        EndpointOperations endpointOperations = new EndpointOperations(vertx, client);

        boolean isOpenShift = Boolean.TRUE.equals(client.isAdaptable(OpenShiftClient.class));
        KafkaClusterOperations kafkaClusterOperations = new KafkaClusterOperations(vertx, isOpenShift, configMapOperations, serviceOperations, statefulSetOperations, pvcOperations, podOperations, endpointOperations);
        KafkaConnectClusterOperations kafkaConnectClusterOperations = new KafkaConnectClusterOperations(vertx, isOpenShift, configMapOperations, deploymentOperations, serviceOperations);

        DeploymentConfigOperations deploymentConfigOperations = null;
        ImageStreamOperations imagesStreamOperations = null;
        BuildConfigOperations buildConfigOperations = null;
        if (isOpenShift) {
            imagesStreamOperations = new ImageStreamOperations(vertx, client.adapt(OpenShiftClient.class));
            buildConfigOperations = new BuildConfigOperations(vertx, client.adapt(OpenShiftClient.class));
            deploymentConfigOperations = new DeploymentConfigOperations(vertx, client.adapt(OpenShiftClient.class));
        }
        KafkaConnectS2IClusterOperations kafkaConnectS2IClusterOperations = new KafkaConnectS2IClusterOperations(vertx, isOpenShift,
                configMapOperations, deploymentConfigOperations,
                serviceOperations, imagesStreamOperations, buildConfigOperations);


        ClusterControllerConfig config = ClusterControllerConfig.fromMap(env);
        List<Future> futures = new ArrayList<>();
        for (String namespace : config.getNamespaces()) {
            Future<String> fut = Future.future();
            futures.add(fut);
            ClusterController controller = new ClusterController(namespace,
                    config.getLabels(),
                    config.getReconciliationInterval(),
                    client,
                    kafkaClusterOperations,
                    kafkaConnectClusterOperations,
                    kafkaConnectS2IClusterOperations);
            vertx.deployVerticle(controller,
                res -> {
                    if (res.succeeded()) {
                        log.info("Cluster Controller verticle started");
                    } else {
                        log.error("Cluster Controller verticle failed to start", res.cause());
                        System.exit(1);
                    }
                    fut.completer().handle(res);
                });
        }
        return CompositeFuture.join(futures);
    }
}
