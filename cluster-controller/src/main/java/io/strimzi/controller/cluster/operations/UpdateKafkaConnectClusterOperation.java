package io.strimzi.controller.cluster.operations;

import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.operations.kubernetes.PatchOperation;
import io.strimzi.controller.cluster.operations.kubernetes.ScaleDownOperation;
import io.strimzi.controller.cluster.operations.kubernetes.ScaleUpOperation;
import io.strimzi.controller.cluster.operations.openshift.CreateS2IOperation;
import io.strimzi.controller.cluster.operations.openshift.DeleteS2IOperation;
import io.strimzi.controller.cluster.operations.openshift.UpdateS2IOperation;
import io.strimzi.controller.cluster.resources.KafkaConnectCluster;
import io.strimzi.controller.cluster.resources.ClusterDiffResult;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.strimzi.controller.cluster.resources.Source2Image;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateKafkaConnectClusterOperation extends ClusterOperation {
    private static final Logger log = LoggerFactory.getLogger(UpdateKafkaConnectClusterOperation.class.getName());
    private final K8SUtils k8s;

    public UpdateKafkaConnectClusterOperation(Vertx vertx, K8SUtils k8s, String namespace, String name) {
        super(vertx, namespace, name);
        this.k8s = k8s;
    }

    protected String getLockName() {
        return "lock::kafka-connect::" + namespace + "::" + name;
    }

    public void execute(Handler<AsyncResult<Void>> handler) {

        vertx.sharedData().getLockWithTimeout(getLockName(), LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                Lock lock = res.result();

                ClusterDiffResult diff;
                KafkaConnectCluster connect;
                ConfigMap connectConfigMap = k8s.getConfigmap(namespace, name);

                if (connectConfigMap != null)    {
                    connect = KafkaConnectCluster.fromConfigMap(k8s, connectConfigMap);
                    log.info("Updating Kafka Connect cluster {} in namespace {}", connect.getName(), namespace);
                    diff = connect.diff(k8s, namespace);
                } else  {
                    log.error("ConfigMap {} doesn't exist anymore in namespace {}", name, namespace);
                    handler.handle(Future.failedFuture("ConfigMap doesn't exist anymore"));
                    lock.release();
                    return;
                }

                Future<Void> chainFuture = Future.future();

                scaleDown(connect, diff)
                        .compose(i -> patchService(connect, diff))
                        .compose(i -> patchDeployment(connect, diff))
                        .compose(i -> patchS2I(connect, diff))
                        .compose(i -> scaleUp(connect, diff))
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
                log.error("Failed to acquire lock to create Kafka Connect cluster {}", getLockName());
                handler.handle(Future.failedFuture("Failed to acquire lock to create Kafka Connect cluster"));
            }
        });
    }

    private Future<Void> scaleDown(KafkaConnectCluster connect, ClusterDiffResult diff) {
        Future<Void> scaleDown = Future.future();

        if (diff.getScaleDown())    {
            log.info("Scaling down deployment {} in namespace {}", connect.getName(), namespace);
            OperationExecutor.getInstance().executeK8s(new ScaleDownOperation(k8s.getDeploymentResource(namespace, connect.getName()), connect.getReplicas()), scaleDown.completer());
        }
        else {
            scaleDown.complete();
        }

        return scaleDown;
    }

    private Future<Void> patchService(KafkaConnectCluster connect, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            Future<Void> patchService = Future.future();
            OperationExecutor.getInstance().executeK8s(new PatchOperation(k8s.getServiceResource(namespace, connect.getName()), connect.patchService(k8s.getService(namespace, connect.getName()))), patchService.completer());
            return patchService;
        }
            else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchDeployment(KafkaConnectCluster connect, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            Future<Void> patchDeployment = Future.future();
            OperationExecutor.getInstance().executeK8s(new PatchOperation(k8s.getDeploymentResource(namespace, connect.getName()), connect.patchDeployment(k8s.getDeployment(namespace, connect.getName()))), patchDeployment.completer());
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
    private Future<Void> patchS2I(KafkaConnectCluster connect, ClusterDiffResult diff) {
        if (diff.getS2i() != Source2Image.Source2ImageDiff.NONE) {
            if (diff.getS2i() == Source2Image.Source2ImageDiff.CREATE) {
                log.info("Creating S2I deployment {} in namespace {}", connect.getName(), namespace);
                Future<Void> createS2I = Future.future();
                OperationExecutor.getInstance().executeOpenShift(new CreateS2IOperation(connect.getS2I()), createS2I.completer());
                return createS2I;
            } else if (diff.getS2i() == Source2Image.Source2ImageDiff.DELETE) {
                log.info("Deleting S2I deployment {} in namespace {}", connect.getName(), namespace);
                Future<Void> deleteS2I = Future.future();
                OperationExecutor.getInstance().executeOpenShift(new DeleteS2IOperation(new Source2Image(namespace, connect.getName())), deleteS2I.completer());
                return deleteS2I;
            } else {
                log.info("Updating S2I deployment {} in namespace {}", connect.getName(), namespace);
                Future<Void> patchS2I = Future.future();
                OperationExecutor.getInstance().executeOpenShift(new UpdateS2IOperation(connect.getS2I()), patchS2I.completer());
                return patchS2I;
            }
        } else {
            return Future.succeededFuture();
        }
    }

    private Future<Void> scaleUp(KafkaConnectCluster connect, ClusterDiffResult diff) {
        Future<Void> scaleUp = Future.future();

        if (diff.getScaleUp()) {
            OperationExecutor.getInstance().executeK8s(new ScaleUpOperation(k8s.getDeploymentResource(namespace, connect.getName()), connect.getReplicas()), scaleUp.completer());
        }
        else {
            scaleUp.complete();
        }

        return scaleUp;
    }
}
