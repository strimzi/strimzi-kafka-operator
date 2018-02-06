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
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * CRUD-style operations on a Kafka Connect cluster
 */
public class KafkaConnectS2IClusterOperations extends AbstractClusterOperations<KafkaConnectS2ICluster> {

    private static final Logger log = LoggerFactory.getLogger(KafkaConnectS2IClusterOperations.class.getName());
    private final ServiceOperations serviceOperations;
    private final DeploymentConfigOperations deploymentConfigOperations;
    private final ConfigMapOperations configMapOperations;
    private final ImageStreamOperations imagesStreamOperations;
    private final BuildConfigOperations buildConfigOperations;

    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param isOpenShift Whether we're running with OpenShift
     * @param configMapOperations For operating on ConfigMaps
     * @param deploymentConfigOperations For operating on Deployments
     * @param serviceOperations For operating on Services
     * @param imagesStreamOperations For operating on ImageStreams, may be null
     * @param buildConfigOperations For operating on BuildConfigs, may be null
     */
    public KafkaConnectS2IClusterOperations(Vertx vertx, boolean isOpenShift,
                                            ConfigMapOperations configMapOperations,
                                            DeploymentConfigOperations deploymentConfigOperations,
                                            ServiceOperations serviceOperations,
                                            ImageStreamOperations imagesStreamOperations,
                                            BuildConfigOperations buildConfigOperations) {
        super(vertx, isOpenShift, "kafka-connect", "create");
        this.serviceOperations = serviceOperations;
        this.deploymentConfigOperations = deploymentConfigOperations;
        this.configMapOperations = configMapOperations;
        this.imagesStreamOperations = imagesStreamOperations;
        this.buildConfigOperations = buildConfigOperations;
    }

    private final CompositeOperation<KafkaConnectS2ICluster> create = new CompositeOperation<KafkaConnectS2ICluster>() {

        @Override
        public ClusterOperation<KafkaConnectS2ICluster> getCluster(String namespace, String name) {
            return new ClusterOperation<>(KafkaConnectS2ICluster.fromConfigMap(isOpenShift, configMapOperations.get(namespace, name)), null);
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
    protected CompositeOperation<KafkaConnectS2ICluster> createOp() {
        return create;
    }

    private final CompositeOperation<KafkaConnectS2ICluster> delete = new CompositeOperation<KafkaConnectS2ICluster>() {

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
    protected CompositeOperation<KafkaConnectS2ICluster> deleteOp() {
        return delete;
    }

    @Override
    protected CompositeOperation<KafkaConnectS2ICluster> updateOp() {
        return update;
    }

    private final CompositeOperation<KafkaConnectS2ICluster> update = new CompositeOperation<KafkaConnectS2ICluster>() {
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
                connect = KafkaConnectS2ICluster.fromConfigMap(isOpenShift, connectConfigMap);
                log.info("Updating Kafka Connect cluster {} in namespace {}", connect.getName(), namespace);
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
    };

    private Future<Void> scaleDown(KafkaConnectS2ICluster connect, String namespace, ClusterDiffResult diff) {
        Future<Void> scaleDown = Future.future();

        if (diff.isScaleDown())    {
            log.info("Scaling down deployment {} in namespace {}", connect.getName(), namespace);
            deploymentConfigOperations.scaleDown(namespace, connect.getName(), connect.getReplicas(), scaleDown.completer());
        }
        else {
            scaleDown.complete();
        }

        return scaleDown;
    }

    private Future<Void> patchService(KafkaConnectS2ICluster connect, String namespace, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            return serviceOperations.patch(namespace, connect.getName(),
                    connect.patchService(serviceOperations.get(namespace, connect.getName())));
        }
        else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchDeploymentConfig(KafkaConnectS2ICluster connect, String namespace, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            return deploymentConfigOperations.patch(namespace, connect.getName(),
                    connect.patchDeploymentConfig(deploymentConfigOperations.get(namespace, connect.getName())));
        }
        else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchBuildConfig(KafkaConnectS2ICluster connect, String namespace, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            return buildConfigOperations.patch(namespace, connect.getName(),
                    connect.patchBuildConfig(buildConfigOperations.get(namespace, connect.getName())));
        }
        else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchSourceImageStream(KafkaConnectS2ICluster connect, String namespace, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            return imagesStreamOperations.patch(namespace, connect.getSourceImageStreamName(),
                    connect.patchSourceImageStream(imagesStreamOperations.get(namespace, connect.getSourceImageStreamName())));
        }
        else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchTargetImageStream(KafkaConnectS2ICluster connect, String namespace, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            return imagesStreamOperations.patch(namespace, connect.getName(),
                    connect.patchTargetImageStream(imagesStreamOperations.get(namespace, connect.getName())));
        }
        else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> scaleUp(KafkaConnectS2ICluster connect, String namespace, ClusterDiffResult diff) {
        Future<Void> scaleUp = Future.future();

        if (diff.isScaleUp()) {
            deploymentConfigOperations.scaleUp(namespace, connect.getName(), connect.getReplicas(), scaleUp.completer());
        }
        else {
            scaleUp.complete();
        }

        return scaleUp;
    }
}
