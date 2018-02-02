package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.strimzi.controller.cluster.operations.resource.BuildConfigOperations;
import io.strimzi.controller.cluster.operations.resource.ConfigMapOperations;
import io.strimzi.controller.cluster.operations.resource.DeploymentOperations;
import io.strimzi.controller.cluster.operations.resource.ImageStreamOperations;
import io.strimzi.controller.cluster.operations.resource.S2IOperations;
import io.strimzi.controller.cluster.operations.resource.ServiceOperations;
import io.strimzi.controller.cluster.resources.ClusterDiffResult;
import io.strimzi.controller.cluster.resources.KafkaConnectCluster;
import io.strimzi.controller.cluster.resources.Source2Image;
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
public class KafkaConnectClusterOperations extends AbstractClusterOperations<KafkaConnectCluster> {

    private static final Logger log = LoggerFactory.getLogger(KafkaConnectClusterOperations.class.getName());
    private final ServiceOperations serviceOperations;
    private final DeploymentOperations deploymentOperations;
    private final ConfigMapOperations configMapOperations;
    private final ImageStreamOperations imagesStreamResources;
    private final BuildConfigOperations buildConfigOperations;
    private final S2IOperations s2iOperations;

    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param isOpenShift Whether we're running with OpenShift
     * @param configMapOperations For operating on ConfigMaps
     * @param deploymentOperations For operating on Deployments
     * @param serviceOperations For operating on Services
     * @param imagesStreamResources For operating on ImageStreams, may be null
     * @param buildConfigOperations For operating on BuildConfigs, may be null
     */
    public KafkaConnectClusterOperations(Vertx vertx, boolean isOpenShift,
                                         ConfigMapOperations configMapOperations,
                                         DeploymentOperations deploymentOperations,
                                         ServiceOperations serviceOperations,
                                         ImageStreamOperations imagesStreamResources,
                                         BuildConfigOperations buildConfigOperations) {
        super(vertx, isOpenShift, "kafka-connect", "create");
        this.serviceOperations = serviceOperations;
        this.deploymentOperations = deploymentOperations;
        this.configMapOperations = configMapOperations;
        this.imagesStreamResources = imagesStreamResources;
        this.buildConfigOperations = buildConfigOperations;
        if (imagesStreamResources != null && buildConfigOperations != null) {
            this.s2iOperations = new S2IOperations(vertx,
                    imagesStreamResources,
                    buildConfigOperations);
        } else {
            this.s2iOperations = null;
        }
    }

    private final CompositeOperation<KafkaConnectCluster> create = new CompositeOperation<KafkaConnectCluster>() {

        @Override
        public ClusterOperation<KafkaConnectCluster> getCluster(String namespace, String name) {
            return new ClusterOperation<>(KafkaConnectCluster.fromConfigMap(isOpenShift, configMapOperations.get(namespace, name)), null);
        }

        @Override
        public Future<?> composite(String namespace, ClusterOperation<KafkaConnectCluster> clusterOp) {
            KafkaConnectCluster connect = clusterOp.cluster();
            List<Future> result = new ArrayList<>(3);
            result.add(serviceOperations.create(connect.generateService()));

            result.add(deploymentOperations.create(connect.generateDeployment()));

            Future<Void> futureS2I;
            if (connect.getS2I() != null) {
                futureS2I = s2iOperations.create(connect.getS2I());
            } else {
                futureS2I = Future.succeededFuture();
            }
            result.add(futureS2I);

            return CompositeFuture.join(result);
        }
    };

    @Override
    protected CompositeOperation<KafkaConnectCluster> createOp() {
        return create;
    }

    private final CompositeOperation<KafkaConnectCluster> delete = new CompositeOperation<KafkaConnectCluster>() {

        @Override
        public Future<?> composite(String namespace, ClusterOperation<KafkaConnectCluster> clusterOp) {
            KafkaConnectCluster connect = clusterOp.cluster();
            List<Future> result = new ArrayList<>(3);

            result.add(serviceOperations.delete(namespace, connect.getName()));

            result.add(deploymentOperations.delete(namespace, connect.getName()));

            if (connect.getS2I() != null) {
                result.add(s2iOperations.delete(connect.getS2I()));
            }

            return CompositeFuture.join(result);
        }

        @Override
        public ClusterOperation<KafkaConnectCluster> getCluster(String namespace, String name) {
            Deployment dep = deploymentOperations.get(namespace, KafkaConnectCluster.kafkaConnectClusterName(name));
            return new ClusterOperation<>(KafkaConnectCluster.fromDeployment(namespace, name, dep, imagesStreamResources), null);
        }
    };

    @Override
    protected CompositeOperation<KafkaConnectCluster> deleteOp() {
        return delete;
    }

    @Override
    protected CompositeOperation<KafkaConnectCluster> updateOp() {
        return update;
    }

    private final CompositeOperation<KafkaConnectCluster> update = new CompositeOperation<KafkaConnectCluster>() {
        @Override
        public Future<?> composite(String namespace, ClusterOperation<KafkaConnectCluster> operation) {
            KafkaConnectCluster connect = operation.cluster();
            ClusterDiffResult diff = operation.diff();
            Future<Void> chainFuture = Future.future();

            scaleDown(connect, namespace, diff)
                    .compose(i -> patchService(connect, namespace, diff))
                    .compose(i -> patchDeployment(connect, namespace, diff))
                    .compose(i -> patchS2I(connect, namespace, diff))
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
                connect = KafkaConnectCluster.fromConfigMap(isOpenShift, connectConfigMap);
                log.info("Updating Kafka Connect cluster {} in namespace {}", connect.getName(), namespace);
                diff = connect.diff(namespace, deploymentOperations, imagesStreamResources, buildConfigOperations);
            } else  {
                throw new IllegalStateException("ConfigMap " + name + " doesn't exist anymore in namespace " + namespace);
            }

            return new ClusterOperation<>(connect, diff);
        }
    };

    private Future<Void> scaleDown(KafkaConnectCluster connect, String namespace, ClusterDiffResult diff) {
        Future<Void> scaleDown = Future.future();

        if (diff.getScaleDown())    {
            log.info("Scaling down deployment {} in namespace {}", connect.getName(), namespace);
            deploymentOperations.scaleDown(namespace, connect.getName(), connect.getReplicas(), scaleDown.completer());
        }
        else {
            scaleDown.complete();
        }

        return scaleDown;
    }

    private Future<Void> patchService(KafkaConnectCluster connect, String namespace, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            return serviceOperations.patch(namespace, connect.getName(),
                    connect.patchService(serviceOperations.get(namespace, connect.getName())));
        }
        else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchDeployment(KafkaConnectCluster connect, String namespace, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            return deploymentOperations.patch(namespace, connect.getName(),
                    connect.patchDeployment(deploymentOperations.get(namespace, connect.getName())));
        }
        else
        {
            return Future.succeededFuture();
        }
    }

    /**
     * Will check the Source2Image diff and add / delete / update resources when needed (S2I can be added / removed while
     * the cluster already exists)
     *
     * @param connect       KafkaConnectResource instance
     * @param namespace     The Kubernetes namespace
     * @param diff          ClusterDiffResult from KafkaConnectResource
     * @return A future for the patching
     */
    private Future<Void> patchS2I(KafkaConnectCluster connect, String namespace, ClusterDiffResult diff) {
        if (diff.getS2i() != Source2Image.Source2ImageDiff.NONE) {
            if (diff.getS2i() == Source2Image.Source2ImageDiff.CREATE) {
                log.info("Creating S2I deployment {} in namespace {}", connect.getName(), namespace);
                return s2iOperations.create(connect.getS2I());
            } else if (diff.getS2i() == Source2Image.Source2ImageDiff.DELETE) {
                log.info("Deleting S2I deployment {} in namespace {}", connect.getName(), namespace);
                return s2iOperations.delete(new Source2Image(namespace, connect.getName()));
            } else {
                log.info("Updating S2I deployment {} in namespace {}", connect.getName(), namespace);
                return s2iOperations.update(connect.getS2I());
            }
        } else {
            return Future.succeededFuture();
        }
    }

    private Future<Void> scaleUp(KafkaConnectCluster connect, String namespace, ClusterDiffResult diff) {
        Future<Void> scaleUp = Future.future();

        if (diff.getScaleUp()) {
            deploymentOperations.scaleUp(namespace, connect.getName(), connect.getReplicas(), scaleUp.completer());
        }
        else {
            scaleUp.complete();
        }

        return scaleUp;
    }
}
