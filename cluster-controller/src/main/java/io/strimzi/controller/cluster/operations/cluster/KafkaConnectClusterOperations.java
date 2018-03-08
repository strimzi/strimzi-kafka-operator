/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.strimzi.controller.cluster.operations.resource.ConfigMapOperations;
import io.strimzi.controller.cluster.operations.resource.DeploymentOperations;
import io.strimzi.controller.cluster.operations.resource.ServiceOperations;
import io.strimzi.controller.cluster.resources.ClusterDiffResult;
import io.strimzi.controller.cluster.resources.KafkaConnectCluster;
import io.strimzi.controller.cluster.resources.Labels;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Cluster operations for a Kafka Connect cluster
 */
public class KafkaConnectClusterOperations extends AbstractClusterOperations<KafkaConnectCluster, Deployment> {

    private static final Logger log = LoggerFactory.getLogger(KafkaConnectClusterOperations.class.getName());
    private static final String CLUSTER_TYPE_CONNECT = "kafka-connect";
    private final ServiceOperations serviceOperations;
    private final DeploymentOperations deploymentOperations;

    /**
     * @param vertx The Vertx instance
     * @param isOpenShift Whether we're running with OpenShift
     * @param configMapOperations For operating on ConfigMaps
     * @param deploymentOperations For operating on Deployments
     * @param serviceOperations For operating on Services
     */
    public KafkaConnectClusterOperations(Vertx vertx, boolean isOpenShift,
                                         ConfigMapOperations configMapOperations,
                                         DeploymentOperations deploymentOperations,
                                         ServiceOperations serviceOperations) {
        super(vertx, isOpenShift, "Kafka Connect", configMapOperations);
        this.serviceOperations = serviceOperations;
        this.deploymentOperations = deploymentOperations;
    }

    private final CompositeOperation<KafkaConnectCluster> create = new CompositeOperation<KafkaConnectCluster>() {
        @Override
        public String operationType() {
            return OP_CREATE;
        }

        @Override
        public String clusterType() {
            return CLUSTER_TYPE_CONNECT;
        }

        @Override
        public ClusterOperation<KafkaConnectCluster> getCluster(String namespace, String name) {
            return new ClusterOperation<>(KafkaConnectCluster.fromConfigMap(configMapOperations.get(namespace, name)), null);
        }

        @Override
        public Future<?> composite(String namespace, ClusterOperation<KafkaConnectCluster> clusterOp) {
            KafkaConnectCluster connect = clusterOp.cluster();
            List<Future> result = new ArrayList<>(3);
            result.add(serviceOperations.create(connect.generateService()));

            result.add(deploymentOperations.create(connect.generateDeployment()));

            return CompositeFuture.join(result);
        }
    };


    private final CompositeOperation<KafkaConnectCluster> delete = new CompositeOperation<KafkaConnectCluster>() {
        @Override
        public String operationType() {
            return OP_DELETE;
        }

        @Override
        public String clusterType() {
            return CLUSTER_TYPE_CONNECT;
        }
        @Override
        public Future<?> composite(String namespace, ClusterOperation<KafkaConnectCluster> clusterOp) {
            KafkaConnectCluster connect = clusterOp.cluster();
            List<Future> result = new ArrayList<>(3);

            result.add(serviceOperations.delete(namespace, connect.getName()));

            result.add(deploymentOperations.delete(namespace, connect.getName()));

            return CompositeFuture.join(result);
        }

        @Override
        public ClusterOperation<KafkaConnectCluster> getCluster(String namespace, String name) {
            Deployment dep = deploymentOperations.get(namespace, KafkaConnectCluster.kafkaConnectClusterName(name));
            return new ClusterOperation<>(KafkaConnectCluster.fromDeployment(namespace, name, dep), null);
        }
    };

    private final CompositeOperation<KafkaConnectCluster> update = new CompositeOperation<KafkaConnectCluster>() {
        @Override
        public String operationType() {
            return OP_UPDATE;
        }

        @Override
        public String clusterType() {
            return CLUSTER_TYPE_CONNECT;
        }
        @Override
        public Future<?> composite(String namespace, ClusterOperation<KafkaConnectCluster> operation) {
            KafkaConnectCluster connect = operation.cluster();
            ClusterDiffResult diff = operation.diff();
            Future<Void> chainFuture = Future.future();

            scaleDown(connect, namespace, diff)
                    .compose(i -> patchService(connect, namespace, diff))
                    .compose(i -> patchDeployment(connect, namespace, diff))
                    .compose(i -> scaleUp(connect, namespace, diff))
                    .compose(chainFuture::complete, chainFuture);

            return chainFuture;
        }

        @Override
        public ClusterOperation<KafkaConnectCluster> getCluster(String namespace, String name) {
            ClusterDiffResult diff;
            KafkaConnectCluster connect;
            ConfigMap connectConfigMap = configMapOperations.get(namespace, name);

            if (connectConfigMap != null)    {
                connect = KafkaConnectCluster.fromConfigMap(connectConfigMap);
                Deployment dep = deploymentOperations.get(namespace, connect.getName());
                log.info("Updating Kafka Connect cluster {} in namespace {}", connect.getName(), namespace);
                diff = connect.diff(dep);
            } else  {
                throw new IllegalStateException("ConfigMap " + name + " doesn't exist anymore in namespace " + namespace);
            }

            return new ClusterOperation<>(connect, diff);
        }
    };

    private Future<Void> scaleDown(KafkaConnectCluster connect, String namespace, ClusterDiffResult diff) {
        if (diff.isScaleDown())    {
            log.info("Scaling down deployment {} in namespace {}", connect.getName(), namespace);
            return deploymentOperations.scaleDown(namespace, connect.getName(), connect.getReplicas());
        } else {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchService(KafkaConnectCluster connect, String namespace, ClusterDiffResult diff) {
        if (diff.isDifferent()) {
            return serviceOperations.patch(namespace, connect.getName(),
                connect.patchService(serviceOperations.get(namespace, connect.getName())));
        } else {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchDeployment(KafkaConnectCluster connect, String namespace, ClusterDiffResult diff) {
        if (diff.isDifferent()) {
            return deploymentOperations.patch(namespace, connect.getName(),
                    connect.patchDeployment(deploymentOperations.get(namespace, connect.getName())));
        } else {
            return Future.succeededFuture();
        }
    }

    private Future<Void> scaleUp(KafkaConnectCluster connect, String namespace, ClusterDiffResult diff) {
        if (diff.isScaleUp()) {
            return deploymentOperations.scaleUp(namespace, connect.getName(), connect.getReplicas());
        } else {
            return Future.succeededFuture();
        }
    }

    @Override
    protected void create(String namespace, String name, Handler<AsyncResult<Void>> handler) {
        execute(namespace, name, create, handler);
    }

    @Override
    protected void delete(String namespace, String name, Handler<AsyncResult<Void>> handler) {
        execute(namespace, name, delete, handler);
    }

    @Override
    protected void update(String namespace, String name, Handler<AsyncResult<Void>> handler) {
        execute(namespace, name, update, handler);
    }

    @Override
    public String clusterType() {
        return CLUSTER_TYPE_CONNECT;
    }

    @Override
    protected List<Deployment> getResources(String namespace, Labels selector) {
        return deploymentOperations.list(namespace, selector);
    }
}
