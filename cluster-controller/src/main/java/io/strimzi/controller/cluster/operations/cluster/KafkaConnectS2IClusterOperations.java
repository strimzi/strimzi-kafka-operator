/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.api.model.ImageStream;
import io.strimzi.controller.cluster.operations.resource.BuildConfigOperations;
import io.strimzi.controller.cluster.operations.resource.ConfigMapOperations;
import io.strimzi.controller.cluster.operations.resource.DeploymentConfigOperations;
import io.strimzi.controller.cluster.operations.resource.ImageStreamOperations;
import io.strimzi.controller.cluster.operations.resource.ServiceOperations;
import io.strimzi.controller.cluster.resources.ClusterDiffResult;
import io.strimzi.controller.cluster.resources.KafkaConnectS2ICluster;
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
public class KafkaConnectS2IClusterOperations extends AbstractClusterOperations<KafkaConnectS2ICluster, DeploymentConfig> {

    private static final Logger log = LoggerFactory.getLogger(KafkaConnectS2IClusterOperations.class.getName());
    private static final String CLUSTER_TYPE_CONNECT_S2I = "kafka-connect-s2i";
    private final ServiceOperations serviceOperations;
    private final DeploymentConfigOperations deploymentConfigOperations;
    private final ImageStreamOperations imagesStreamOperations;
    private final BuildConfigOperations buildConfigOperations;

    /**
     * @param vertx                      The Vertx instance
     * @param isOpenShift                Whether we're running with OpenShift
     * @param configMapOperations        For operating on ConfigMaps
     * @param deploymentConfigOperations For operating on Deployments
     * @param serviceOperations          For operating on Services
     * @param imagesStreamOperations     For operating on ImageStreams, may be null
     * @param buildConfigOperations      For operating on BuildConfigs, may be null
     */
    public KafkaConnectS2IClusterOperations(Vertx vertx, boolean isOpenShift,
                                            ConfigMapOperations configMapOperations,
                                            DeploymentConfigOperations deploymentConfigOperations,
                                            ServiceOperations serviceOperations,
                                            ImageStreamOperations imagesStreamOperations,
                                            BuildConfigOperations buildConfigOperations) {
        super(vertx, isOpenShift, "Kafka Connect S2I", configMapOperations);
        this.serviceOperations = serviceOperations;
        this.deploymentConfigOperations = deploymentConfigOperations;
        this.imagesStreamOperations = imagesStreamOperations;
        this.buildConfigOperations = buildConfigOperations;
    }

    @Override
    public void create(String namespace, String name, Handler<AsyncResult<Void>> handler) {
        if (isOpenShift) {
            execute(namespace, name, create, handler);
        } else {
            handler.handle(Future.failedFuture("S2I only available on OpenShift"));
        }
    }

    private final CompositeOperation<KafkaConnectS2ICluster> create = new CompositeOperation<KafkaConnectS2ICluster>() {

        @Override
        public ClusterOperation<KafkaConnectS2ICluster> getCluster(String namespace, String name) {
            return new ClusterOperation<>(KafkaConnectS2ICluster.fromConfigMap(configMapOperations.get(namespace, name)), null);
        }

        @Override
        public String operationType() {
            return OP_CREATE;
        }

        @Override
        public String clusterType() {
            return CLUSTER_TYPE_CONNECT_S2I;
        }

        @Override
        public Future<?> composite(String namespace, ClusterOperation<KafkaConnectS2ICluster> clusterOp) {
            KafkaConnectS2ICluster connect = clusterOp.cluster();
            List<Future> result = new ArrayList<>(5);
            result.add(serviceOperations.create(connect.generateService()));
            result.add(deploymentConfigOperations.create(connect.generateDeploymentConfig()));
            result.add(imagesStreamOperations.create(connect.generateSourceImageStream()));
            result.add(imagesStreamOperations.create(connect.generateTargetImageStream()));
            result.add(buildConfigOperations.create(connect.generateBuildConfig()));

            return CompositeFuture.join(result);
        }
    };

    @Override
    protected void delete(String namespace, String name, Handler<AsyncResult<Void>> handler) {
        if (isOpenShift) {
            execute(namespace, name, delete, handler);
        } else {
            handler.handle(Future.failedFuture("S2I only available on OpenShift"));
        }
    }

    private final CompositeOperation<KafkaConnectS2ICluster> delete = new CompositeOperation<KafkaConnectS2ICluster>() {

        @Override
        public String operationType() {
            return OP_DELETE;
        }

        @Override
        public String clusterType() {
            return CLUSTER_TYPE_CONNECT_S2I;
        }

        @Override
        public Future<?> composite(String namespace, ClusterOperation<KafkaConnectS2ICluster> clusterOp) {
            KafkaConnectS2ICluster connect = clusterOp.cluster();
            List<Future> result = new ArrayList<>(5);
            result.add(serviceOperations.delete(namespace, connect.getName()));
            result.add(deploymentConfigOperations.delete(namespace, connect.getName()));
            result.add(imagesStreamOperations.delete(namespace, connect.getSourceImageStreamName()));
            result.add(imagesStreamOperations.delete(namespace, connect.getName()));
            result.add(buildConfigOperations.delete(namespace, connect.getName()));

            return CompositeFuture.join(result);
        }

        @Override
        public ClusterOperation<KafkaConnectS2ICluster> getCluster(String namespace, String name) {
            DeploymentConfig dep = deploymentConfigOperations.get(namespace, KafkaConnectS2ICluster.kafkaConnectClusterName(name));
            ImageStream sis = imagesStreamOperations.get(namespace, KafkaConnectS2ICluster.getSourceImageStreamName(KafkaConnectS2ICluster.kafkaConnectClusterName(name)));
            return new ClusterOperation<>(KafkaConnectS2ICluster.fromDeployment(namespace, name, dep, sis), null);
        }
    };

    @Override
    public void update(String namespace, String name, Handler<AsyncResult<Void>> handler) {
        if (isOpenShift) {
            execute(namespace, name, update, handler);
        } else {
            handler.handle(Future.failedFuture("S2I only available on OpenShift"));
        }
    }

    private final CompositeOperation<KafkaConnectS2ICluster> update = new CompositeOperation<KafkaConnectS2ICluster>() {
        @Override
        public String operationType() {
            return CLUSTER_TYPE_CONNECT_S2I;
        }

        @Override
        public String clusterType() {
            return OP_UPDATE;
        }

        @Override
        public Future<?> composite(String namespace, ClusterOperation<KafkaConnectS2ICluster> operation) {
            KafkaConnectS2ICluster connect = operation.cluster();
            ClusterDiffResult diff = operation.diff();
            Future<Void> chainFuture = Future.future();

            scaleDown(connect, namespace, diff)
                    .compose(i -> patchService(connect, namespace, diff))
                    .compose(i -> patchDeploymentConfig(connect, namespace, diff))
                    .compose(i -> patchSourceImageStream(connect, namespace, diff))
                    .compose(i -> patchTargetImageStream(connect, namespace, diff))
                    .compose(i -> patchBuildConfig(connect, namespace, diff))
                    .compose(i -> scaleUp(connect, namespace, diff))
                    .compose(chainFuture::complete, chainFuture);

            return chainFuture;
        }

        @Override
        public ClusterOperation<KafkaConnectS2ICluster> getCluster(String namespace, String name) {
            ClusterDiffResult diff;
            KafkaConnectS2ICluster connect;
            ConfigMap connectConfigMap = configMapOperations.get(namespace, name);

            if (connectConfigMap != null)    {
                connect = KafkaConnectS2ICluster.fromConfigMap(connectConfigMap);
                log.info("Updating {} cluster {} in namespace {}", clusterDescription, connect.getName(), namespace);
                DeploymentConfig dep = deploymentConfigOperations.get(namespace, connect.getName());
                ImageStream sis = imagesStreamOperations.get(namespace, connect.getSourceImageStreamName());
                ImageStream tis = imagesStreamOperations.get(namespace, connect.getName());
                BuildConfig bc = buildConfigOperations.get(namespace, connect.getName());
                diff = connect.diff(dep, sis, tis, bc);
            } else  {
                throw new IllegalStateException("ConfigMap " + name + " doesn't exist anymore in namespace " + namespace);
            }

            return new ClusterOperation<>(connect, diff);
        }

        private Future<Void> scaleDown(KafkaConnectS2ICluster connect, String namespace, ClusterDiffResult diff) {
            if (diff.isScaleDown())    {
                log.info("Scaling down {} deployment {} in namespace {}", clusterDescription, connect.getName(), namespace);
                return deploymentConfigOperations.scaleDown(namespace, connect.getName(), connect.getReplicas());
            } else {
                return Future.succeededFuture();
            }
        }

        private Future<Void> patchService(KafkaConnectS2ICluster connect, String namespace, ClusterDiffResult diff) {
            if (diff.isDifferent()) {
                return serviceOperations.patch(namespace, connect.getName(),
                        connect.patchService(serviceOperations.get(namespace, connect.getName())));
            } else {
                return Future.succeededFuture();
            }
        }

        private Future<Void> patchDeploymentConfig(KafkaConnectS2ICluster connect, String namespace, ClusterDiffResult diff) {
            if (diff.isDifferent()) {
                return deploymentConfigOperations.patch(namespace, connect.getName(),
                        connect.patchDeploymentConfig(deploymentConfigOperations.get(namespace, connect.getName())));
            } else {
                return Future.succeededFuture();
            }
        }

        private Future<Void> patchBuildConfig(KafkaConnectS2ICluster connect, String namespace, ClusterDiffResult diff) {
            if (diff.isDifferent()) {
                return buildConfigOperations.patch(namespace, connect.getName(),
                        connect.patchBuildConfig(buildConfigOperations.get(namespace, connect.getName())));
            } else {
                return Future.succeededFuture();
            }
        }

        private Future<Void> patchSourceImageStream(KafkaConnectS2ICluster connect, String namespace, ClusterDiffResult diff) {
            if (diff.isDifferent()) {
                return imagesStreamOperations.patch(namespace, connect.getSourceImageStreamName(),
                        connect.patchSourceImageStream(imagesStreamOperations.get(namespace, connect.getSourceImageStreamName())));
            } else {
                return Future.succeededFuture();
            }
        }

        private Future<Void> patchTargetImageStream(KafkaConnectS2ICluster connect, String namespace, ClusterDiffResult diff) {
            if (diff.isDifferent()) {
                return imagesStreamOperations.patch(namespace, connect.getName(),
                        connect.patchTargetImageStream(imagesStreamOperations.get(namespace, connect.getName())));
            } else {
                return Future.succeededFuture();
            }
        }

        private Future<Void> scaleUp(KafkaConnectS2ICluster connect, String namespace, ClusterDiffResult diff) {
            if (diff.isScaleUp()) {
                return deploymentConfigOperations.scaleUp(namespace, connect.getName(), connect.getReplicas());
            } else {
                return Future.succeededFuture();
            }
        }
    };

    @Override
    public String clusterType() {
        return CLUSTER_TYPE_CONNECT_S2I;
    }

    @Override
    protected List<DeploymentConfig> getResources(String namespace, Labels kafkaLabels) {
        return deploymentConfigOperations.list(namespace, kafkaLabels);
    }

}
