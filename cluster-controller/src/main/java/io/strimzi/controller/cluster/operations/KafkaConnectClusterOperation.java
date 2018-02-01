package io.strimzi.controller.cluster.operations;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.api.model.DoneableService;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DeploymentList;
import io.fabric8.kubernetes.api.model.extensions.DoneableDeployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import io.fabric8.openshift.api.model.Build;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.BuildConfigList;
import io.fabric8.openshift.api.model.DoneableBuildConfig;
import io.fabric8.openshift.api.model.DoneableImageStream;
import io.fabric8.openshift.api.model.ImageStream;
import io.fabric8.openshift.api.model.ImageStreamList;
import io.fabric8.openshift.client.OpenShiftClient;
import io.fabric8.openshift.client.dsl.BuildConfigResource;
import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.operations.kubernetes.ScaleDownOperation;
import io.strimzi.controller.cluster.operations.kubernetes.ScaleUpOperation;
import io.strimzi.controller.cluster.operations.openshift.CreateS2IOperation;
import io.strimzi.controller.cluster.operations.openshift.DeleteS2IOperation;
import io.strimzi.controller.cluster.operations.openshift.UpdateS2IOperation;
import io.strimzi.controller.cluster.resources.ClusterDiffResult;
import io.strimzi.controller.cluster.resources.KafkaConnectCluster;
import io.strimzi.controller.cluster.resources.Source2Image;
import io.vertx.core.*;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class KafkaConnectClusterOperation extends ClusterOperation<KafkaConnectCluster> {

    private static final Logger log = LoggerFactory.getLogger(KafkaConnectClusterOperation.class.getName());
    private final ResourceOperation<KubernetesClient, Service, ServiceList, DoneableService, Resource<Service, DoneableService>> serviceResources;
    private final ResourceOperation<KubernetesClient, Deployment, DeploymentList, DoneableDeployment, ScalableResource<Deployment, DoneableDeployment>> deploymentResources;
    private final ResourceOperation<KubernetesClient, ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> configMapResources;
    private final ResourceOperation<OpenShiftClient, ImageStream, ImageStreamList, DoneableImageStream, Resource<ImageStream, DoneableImageStream>> imagesStreamResources;
    private final ResourceOperation<OpenShiftClient, BuildConfig, BuildConfigList, DoneableBuildConfig, BuildConfigResource<BuildConfig, DoneableBuildConfig, Void, Build>> buildConfigResources;

    public KafkaConnectClusterOperation(Vertx vertx, K8SUtils k8s) {
        super(vertx, k8s, "kafka-connect","create");
        configMapResources = ResourceOperation.configMap(vertx, k8s.getKubernetesClient());
        serviceResources = ResourceOperation.service(vertx, k8s.getKubernetesClient());
        deploymentResources = ResourceOperation.deployment(vertx, k8s.getKubernetesClient());
        imagesStreamResources = ResourceOperation.imageStream(vertx, k8s.getKubernetesClient().adapt(OpenShiftClient.class));
        buildConfigResources = ResourceOperation.buildConfig(vertx, k8s.getKubernetesClient().adapt(OpenShiftClient.class));
    }

    private final Op<KafkaConnectCluster> create = new Op<KafkaConnectCluster>() {

        @Override
        public KafkaConnectCluster getCluster(K8SUtils k8s, String namespace, String name) {
            return KafkaConnectCluster.fromConfigMap(k8s, configMapResources.get(namespace, name));
        }

        @Override
        public List<Future> futures(K8SUtils k8s, String namespace, KafkaConnectCluster connect) {
            List<Future> result = new ArrayList<>(3);
            Future<Void> futureService = Future.future();
            ResourceOperation.service(vertx, k8s.getKubernetesClient()).create(connect.generateService(), futureService.completer());
            result.add(futureService);

            Future<Void> futureDeployment = Future.future();
            ResourceOperation.deployment(vertx, k8s.getKubernetesClient()).create(connect.generateDeployment(), futureDeployment.completer());
            result.add(futureDeployment);

            Future<Void> futureS2I;
            if (connect.getS2I() != null) {
                futureS2I = Future.future();
                new CreateS2IOperation(vertx, k8s.getKubernetesClient().adapt(OpenShiftClient.class)).execute(connect.getS2I(), futureS2I.completer());
            } else {
                futureS2I = Future.succeededFuture();
            }
            result.add(futureS2I);

            return result;
        }
    };

    @Override
    protected Op<KafkaConnectCluster> createOp() {
        return create;
    }

    private final Op<KafkaConnectCluster> delete = new Op<KafkaConnectCluster>() {

        @Override
        public List<Future> futures(K8SUtils k8s, String namespace, KafkaConnectCluster connect) {
            List<Future> result = new ArrayList<>(3);

            Future<Void> futureService = Future.future();
            ResourceOperation.service(vertx, k8s.getKubernetesClient()).delete(namespace, connect.getName(), futureService.completer());
            result.add(futureService);

            Future<Void> futureDeployment = Future.future();
            ResourceOperation.deployment(vertx, k8s.getKubernetesClient()).delete(namespace, connect.getName(), futureDeployment.completer());
            result.add(futureDeployment);

            if (connect.getS2I() != null) {
                Future<Void> futureS2I = Future.future();
                new DeleteS2IOperation(vertx, k8s.getKubernetesClient().adapt(OpenShiftClient.class)).execute(connect.getS2I(), futureS2I.completer());
                result.add(futureS2I);
            }

            return result;
        }

        @Override
        public KafkaConnectCluster getCluster(K8SUtils k8s, String namespace, String name) {
            return KafkaConnectCluster.fromDeployment(deploymentResources, imagesStreamResources, namespace, name);
        }
    };

    @Override
    protected Op<KafkaConnectCluster> deleteOp() {
        return delete;
    }

    public void update(String namespace, String name, Handler<AsyncResult<Void>> handler) {

        final String lockName = getLockName(namespace, name);
        vertx.sharedData().getLockWithTimeout(lockName, LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                Lock lock = res.result();

                ClusterDiffResult diff;
                KafkaConnectCluster connect;
                ConfigMap connectConfigMap = configMapResources.get(namespace, name);

                if (connectConfigMap != null)    {
                    connect = KafkaConnectCluster.fromConfigMap(k8s, connectConfigMap);
                    log.info("Updating Kafka Connect cluster {} in namespace {}", connect.getName(), namespace);
                    diff = connect.diff(deploymentResources, imagesStreamResources, buildConfigResources, namespace);
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
            new ScaleDownOperation(vertx, k8s).scaleDown(k8s.getDeploymentResource(namespace, connect.getName()), connect.getReplicas(), scaleDown.completer());
        }
        else {
            scaleDown.complete();
        }

        return scaleDown;
    }

    private Future<Void> patchService(KafkaConnectCluster connect, String namespace, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            Future<Void> patchService = Future.future();
            serviceResources.patch(namespace, connect.getName(),
                    connect.patchService(serviceResources.get(namespace, connect.getName())), patchService.completer());
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
            deploymentResources.patch(namespace, connect.getName(),
                    connect.patchDeployment(deploymentResources.get(namespace, connect.getName())), patchDeployment.completer());
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
     * @param diff          ClusterDiffResult from KafkaConnectResource
     * @return
     */
    private Future<Void> patchS2I(KafkaConnectCluster connect, String namespace, ClusterDiffResult diff) {
        if (diff.getS2i() != Source2Image.Source2ImageDiff.NONE) {
            if (diff.getS2i() == Source2Image.Source2ImageDiff.CREATE) {
                log.info("Creating S2I deployment {} in namespace {}", connect.getName(), namespace);
                Future<Void> createS2I = Future.future();
                new CreateS2IOperation(vertx, k8s.getKubernetesClient().adapt(OpenShiftClient.class)).execute(connect.getS2I(), createS2I.completer());
                return createS2I;
            } else if (diff.getS2i() == Source2Image.Source2ImageDiff.DELETE) {
                log.info("Deleting S2I deployment {} in namespace {}", connect.getName(), namespace);
                Future<Void> deleteS2I = Future.future();
                new DeleteS2IOperation(vertx, k8s.getKubernetesClient().adapt(OpenShiftClient.class)).execute(new Source2Image(namespace, connect.getName()), deleteS2I.completer());
                return deleteS2I;
            } else {
                log.info("Updating S2I deployment {} in namespace {}", connect.getName(), namespace);
                Future<Void> patchS2I = Future.future();
                new UpdateS2IOperation(vertx, k8s.getKubernetesClient().adapt(OpenShiftClient.class)).execute(connect.getS2I(), patchS2I.completer());
                return patchS2I;
            }
        } else {
            return Future.succeededFuture();
        }
    }

    private Future<Void> scaleUp(KafkaConnectCluster connect, String namespace, ClusterDiffResult diff) {
        Future<Void> scaleUp = Future.future();

        if (diff.getScaleUp()) {
            new ScaleUpOperation(vertx, k8s).scaleUp(k8s.getDeploymentResource(namespace, connect.getName()), connect.getReplicas(), scaleUp.completer());
        }
        else {
            scaleUp.complete();
        }

        return scaleUp;
    }
}
