package io.enmasse.barnabas.controller.cluster.operations.cluster;

import io.enmasse.barnabas.controller.cluster.K8SUtils;
import io.enmasse.barnabas.controller.cluster.operations.OperationExecutor;
import io.enmasse.barnabas.controller.cluster.operations.kubernetes.ManualRollingUpdateOperation;
import io.enmasse.barnabas.controller.cluster.operations.kubernetes.PatchOperation;
import io.enmasse.barnabas.controller.cluster.operations.kubernetes.ScaleDownOperation;
import io.enmasse.barnabas.controller.cluster.operations.kubernetes.ScaleUpOperation;
import io.enmasse.barnabas.controller.cluster.resources.KafkaResource;
import io.enmasse.barnabas.controller.cluster.resources.ResourceDiffResult;
import io.enmasse.barnabas.controller.cluster.resources.ZookeeperResource;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateKafkaClusterOperation extends KafkaClusterOperation {
    private static final Logger log = LoggerFactory.getLogger(UpdateKafkaClusterOperation.class.getName());

    private K8SUtils k8s;

    public UpdateKafkaClusterOperation(String namespace, String name) {
        super(namespace, name);
    }

    @Override
    public void execute(Vertx vertx, K8SUtils k8s, Handler<AsyncResult<Void>> handler) {
        this.k8s = k8s;

        vertx.sharedData().getLockWithTimeout(getLockName(), LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                Lock lock = res.result();

                log.info("Updating Kafka cluster {} in namespace {}", name, namespace);

                KafkaResource kafka = KafkaResource.fromConfigMap(k8s.getConfigmap(namespace, name));
                ResourceDiffResult diff = kafka.diff(k8s.getStatefulSet(namespace, name));

                Future<Void> chainFuture = Future.future();

                scaleDown(kafka, diff)
                        .compose(i -> patchService(kafka, diff))
                        .compose(i -> patchHeadlessService(kafka, diff))
                        .compose(i -> patchStatefulSet(kafka, diff))
                        .compose(i -> rollingUpdate(diff))
                        .compose(i -> scaleUp(kafka, diff))
                        .compose(chainFuture::complete, chainFuture);

                chainFuture.setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.info("Kafka cluster {} successfully updated in namespace {}", name, namespace);
                        handler.handle(Future.succeededFuture());
                        lock.release();
                    } else {
                        log.error("Kafka cluster {} failed to update in namespace {}", name, namespace);
                        handler.handle(Future.failedFuture("Failed to update Zookeeper cluster"));
                        lock.release();
                    }
                });
            } else {
                log.error("Failed to acquire lock to create Kafka cluster {}", getLockName());
                handler.handle(Future.failedFuture("Failed to acquire lock to create Kafka cluster"));
            }
        });
    }

    private Future<Void> scaleDown(KafkaResource kafka, ResourceDiffResult diff) {
        Future<Void> scaleDown = Future.future();

        if (diff.getScaleDown())    {
            log.info("Scaling down stateful set {} in namespace {}", name, namespace);
            OperationExecutor.getInstance().execute(new ScaleDownOperation(k8s.getStatefulSetResource(namespace, name), kafka.getReplicas()), scaleDown.completer());
        }
        else {
            scaleDown.complete();
        }

        return scaleDown;
    }

    private Future<Void> patchService(KafkaResource kafka, ResourceDiffResult diff) {
        if (diff.getDifferent()) {
            Future<Void> patchService = Future.future();
            OperationExecutor.getInstance().execute(new PatchOperation(k8s.getServiceResource(namespace, name), kafka.patchService(k8s.getService(namespace, name))), patchService.completer());
            return patchService;
        }
            else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchHeadlessService(KafkaResource kafka, ResourceDiffResult diff) {
        if (diff.getDifferent()) {
            Future<Void> patchService = Future.future();
            OperationExecutor.getInstance().execute(new PatchOperation(k8s.getServiceResource(namespace, kafka.getHeadlessName()), kafka.patchService(k8s.getService(namespace, kafka.getHeadlessName()))), patchService.completer());
            return patchService;
        }
            else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchStatefulSet(KafkaResource kafka, ResourceDiffResult diff) {
        if (diff.getDifferent()) {
            Future<Void> patchStatefulSet = Future.future();
            OperationExecutor.getInstance().execute(new PatchOperation(k8s.getStatefulSetResource(namespace, name).cascading(false), kafka.patchStatefulSet(k8s.getStatefulSet(namespace, name))), patchStatefulSet.completer());
            return patchStatefulSet;
        }
        else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> rollingUpdate(ResourceDiffResult diff) {
        Future<Void> rollingUpdate = Future.future();

        if (diff.getRollingUpdate()) {
            OperationExecutor.getInstance().execute(new ManualRollingUpdateOperation(namespace, name, k8s.getStatefulSet(namespace, name).getSpec().getReplicas()), rollingUpdate.completer());
        }
        else {
            rollingUpdate.complete();
        }

        return rollingUpdate;
    }

    private Future<Void> scaleUp(KafkaResource kafka, ResourceDiffResult diff) {
        Future<Void> scaleUp = Future.future();

        if (diff.getScaleUp()) {
            OperationExecutor.getInstance().execute(new ScaleUpOperation(k8s.getStatefulSetResource(namespace, name), kafka.getReplicas()), scaleUp.completer());
        }
        else {
            scaleUp.complete();
        }

        return scaleUp;
    }
}
