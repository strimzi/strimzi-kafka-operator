package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.controller.cluster.operations.resource.BuildConfigOperations;
import io.strimzi.controller.cluster.operations.resource.ConfigMapOperations;
import io.strimzi.controller.cluster.operations.resource.DeploymentOperations;
import io.strimzi.controller.cluster.operations.resource.ImageStreamOperations;
import io.strimzi.controller.cluster.operations.resource.S2IOperations;
import io.strimzi.controller.cluster.operations.resource.ServiceOperations;
import io.strimzi.controller.cluster.resources.ClusterDiffResult;
import io.strimzi.controller.cluster.resources.KafkaConnectCluster;
import io.strimzi.controller.cluster.resources.Source2Image;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Lock;
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
     * @param client The kubernetes client
     * @param configMapOperations For operating on ConfigMaps
     * @param deploymentOperations For operating on Deployments
     * @param serviceOperations For operating on Services
     * @param imagesStreamResources For operating on ImageStreams, may be null
     * @param buildConfigOperations For operating on BuildConfigs, may be null
     */
    public KafkaConnectClusterOperations(Vertx vertx, KubernetesClient client,
                                         ConfigMapOperations configMapOperations,
                                         DeploymentOperations deploymentOperations,
                                         ServiceOperations serviceOperations,
                                         ImageStreamOperations imagesStreamResources,
                                         BuildConfigOperations buildConfigOperations) {
        super(vertx, client, "kafka-connect", "create");
        this.serviceOperations = serviceOperations;
        this.deploymentOperations = deploymentOperations;
        this.configMapOperations = configMapOperations;
        this.imagesStreamResources = imagesStreamResources;
        this.buildConfigOperations = buildConfigOperations;
        s2iOperations = new S2IOperations(vertx,
                imagesStreamResources,
                buildConfigOperations);
    }

    private final CompositeOperation<KafkaConnectCluster> create = new CompositeOperation<KafkaConnectCluster>() {

        @Override
        public KafkaConnectCluster getCluster(String namespace, String name) {
            return KafkaConnectCluster.fromConfigMap(client, configMapOperations.get(namespace, name));
        }

        @Override
        public Future<?> composite(String namespace, KafkaConnectCluster connect) {
            List<Future> result = new ArrayList<>(3);
            Future<Void> futureService = Future.future();
            serviceOperations.create(connect.generateService(), futureService.completer());
            result.add(futureService);

            Future<Void> futureDeployment = Future.future();
            deploymentOperations.create(connect.generateDeployment(), futureDeployment.completer());
            result.add(futureDeployment);

            Future<Void> futureS2I;
            if (connect.getS2I() != null) {
                futureS2I = Future.future();
                s2iOperations.create(connect.getS2I(), futureS2I.completer());
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
        public Future<?> composite(String namespace, KafkaConnectCluster connect) {
            List<Future> result = new ArrayList<>(3);

            Future<Void> futureService = Future.future();
            serviceOperations.delete(namespace, connect.getName(), futureService.completer());
            result.add(futureService);

            Future<Void> futureDeployment = Future.future();
            deploymentOperations.delete(namespace, connect.getName(), futureDeployment.completer());
            result.add(futureDeployment);

            if (connect.getS2I() != null) {
                Future<Void> futureS2I = Future.future();
                s2iOperations.delete(connect.getS2I(), futureS2I.completer());
                result.add(futureS2I);
            }

            return CompositeFuture.join(result);
        }

        @Override
        public KafkaConnectCluster getCluster(String namespace, String name) {
            return KafkaConnectCluster.fromDeployment(deploymentOperations, imagesStreamResources, namespace, name);
        }
    };

    @Override
    protected CompositeOperation<KafkaConnectCluster> deleteOp() {
        return delete;
    }

    public void update(String namespace, String name, Handler<AsyncResult<Void>> handler) {

        final String lockName = getLockName(namespace, name);
        vertx.sharedData().getLockWithTimeout(lockName, LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                Lock lock = res.result();

                ClusterDiffResult diff;
                KafkaConnectCluster connect;
                ConfigMap connectConfigMap = configMapOperations.get(namespace, name);

                if (connectConfigMap != null)    {
                    connect = KafkaConnectCluster.fromConfigMap(client, connectConfigMap);
                    log.info("Updating Kafka Connect cluster {} in namespace {}", connect.getName(), namespace);
                    diff = connect.diff(deploymentOperations, imagesStreamResources, buildConfigOperations, namespace);
                } else  {
                    log.error("ConfigMap {} doesn't exist anymore in namespace {}", name, namespace);
                    handler.handle(Future.failedFuture("ConfigMap doesn't exist anymore"));
                    lock.release();
                    return;
                }

                Future<Void> chainFuture = Future.future();

                scaleDown(connect, namespace, diff)
                        .compose(i -> patchService(connect, namespace, diff))
                        .compose(i -> patchDeployment(connect, namespace, diff))
                        .compose(i -> patchS2I(connect, namespace, diff))
                        .compose(i -> scaleUp(connect, namespace, diff))
                        .compose(chainFuture::complete, chainFuture);

                chainFuture.setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.info("Kafka Connect cluster {} successfully updated in namespace {}", connect.getName(), namespace);
                        handler.handle(Future.succeededFuture());
                        lock.release();
                    } else {
                        log.error("Kafka Connect cluster {} failed to update in namespace {}", connect.getName(), namespace);
                        handler.handle(Future.failedFuture("Failed to update Zookeeper cluster"));
                        lock.release();
                    }
                });
            } else {
                log.error("Failed to acquire lock to create Kafka Connect cluster {}", lockName);
                handler.handle(Future.failedFuture("Failed to acquire lock to create Kafka Connect cluster"));
            }
        });
    }

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
            Future<Void> patchService = Future.future();
            serviceOperations.patch(namespace, connect.getName(),
                    connect.patchService(serviceOperations.get(namespace, connect.getName())), patchService.completer());
            return patchService;
        }
        else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchDeployment(KafkaConnectCluster connect, String namespace, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            Future<Void> patchDeployment = Future.future();
            deploymentOperations.patch(namespace, connect.getName(),
                    connect.patchDeployment(deploymentOperations.get(namespace, connect.getName())), patchDeployment.completer());
            return patchDeployment;
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
                Future<Void> createS2I = Future.future();
                s2iOperations.create(connect.getS2I(), createS2I.completer());
                return createS2I;
            } else if (diff.getS2i() == Source2Image.Source2ImageDiff.DELETE) {
                log.info("Deleting S2I deployment {} in namespace {}", connect.getName(), namespace);
                Future<Void> deleteS2I = Future.future();
                s2iOperations.delete(new Source2Image(namespace, connect.getName()), deleteS2I.completer());
                return deleteS2I;
            } else {
                log.info("Updating S2I deployment {} in namespace {}", connect.getName(), namespace);
                Future<Void> patchS2I = Future.future();
                s2iOperations.update(connect.getS2I(), patchS2I.completer());
                return patchS2I;
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
