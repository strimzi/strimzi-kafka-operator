package io.strimzi.controller.cluster.operations;

import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.operations.kubernetes.PatchOperation;
import io.strimzi.controller.cluster.operations.kubernetes.ScaleDownOperation;
import io.strimzi.controller.cluster.operations.kubernetes.ScaleUpOperation;
import io.strimzi.controller.cluster.resources.KafkaConnectCluster;
import io.strimzi.controller.cluster.resources.ClusterDiffResult;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateKafkaConnectClusterOperation extends KafkaConnectClusterOperation {
    private static final Logger log = LoggerFactory.getLogger(UpdateKafkaConnectClusterOperation.class.getName());

    private K8SUtils k8s;

    public UpdateKafkaConnectClusterOperation(String namespace, String name) {
        super(namespace, name);
    }

    @Override
    public void execute(Vertx vertx, K8SUtils k8s, Handler<AsyncResult<Void>> handler) {
        this.k8s = k8s;

        vertx.sharedData().getLockWithTimeout(getLockName(), LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                Lock lock = res.result();

                log.info("Updating Kafka Connect cluster {} in namespace {}", name, namespace);

                ClusterDiffResult diff;
                KafkaConnectCluster connect;
                ConfigMap connectConfigMap = k8s.getConfigmap(namespace, name);

                if (connectConfigMap != null)    {
                    connect = KafkaConnectCluster.fromConfigMap(connectConfigMap);
                    diff = connect.diff(k8s.getDeployment(namespace, name));
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
                        .compose(i -> scaleUp(connect, diff))
                        .compose(chainFuture::complete, chainFuture);

                chainFuture.setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.info("Kafka Connect cluster {} successfully updated in namespace {}", name, namespace);
                        handler.handle(Future.succeededFuture());
                        lock.release();
                    } else {
                        log.error("Kafka Connect cluster {} failed to update in namespace {}", name, namespace);
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
            log.info("Scaling down deployment {} in namespace {}", name, namespace);
            OperationExecutor.getInstance().execute(new ScaleDownOperation(k8s.getDeploymentResource(namespace, name), connect.getReplicas()), scaleDown.completer());
        }
        else {
            scaleDown.complete();
        }

        return scaleDown;
    }

    private Future<Void> patchService(KafkaConnectCluster connect, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            Future<Void> patchService = Future.future();
            OperationExecutor.getInstance().execute(new PatchOperation(k8s.getServiceResource(namespace, name), connect.patchService(k8s.getService(namespace, name))), patchService.completer());
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
            OperationExecutor.getInstance().execute(new PatchOperation(k8s.getDeploymentResource(namespace, name), connect.patchDeployment(k8s.getDeployment(namespace, name))), patchDeployment.completer());
            return patchDeployment;
        }
        else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> scaleUp(KafkaConnectCluster connect, ClusterDiffResult diff) {
        Future<Void> scaleUp = Future.future();

        if (diff.getScaleUp()) {
            OperationExecutor.getInstance().execute(new ScaleUpOperation(k8s.getDeploymentResource(namespace, name), connect.getReplicas()), scaleUp.completer());
        }
        else {
            scaleUp.complete();
        }

        return scaleUp;
    }
}
