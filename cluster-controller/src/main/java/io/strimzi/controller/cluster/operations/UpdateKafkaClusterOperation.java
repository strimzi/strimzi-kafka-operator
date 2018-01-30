package io.strimzi.controller.cluster.operations;

import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.operations.kubernetes.ManualRollingUpdateOperation;
import io.strimzi.controller.cluster.operations.kubernetes.PatchOperation;
import io.strimzi.controller.cluster.operations.kubernetes.ScaleDownOperation;
import io.strimzi.controller.cluster.operations.kubernetes.ScaleUpOperation;
import io.strimzi.controller.cluster.resources.KafkaCluster;
import io.strimzi.controller.cluster.resources.ClusterDiffResult;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateKafkaClusterOperation extends ClusterOperation {
    private static final Logger log = LoggerFactory.getLogger(UpdateKafkaClusterOperation.class.getName());

    private final K8SUtils k8s;

    public UpdateKafkaClusterOperation(Vertx vertx, K8SUtils k8s) {
        super(vertx);
        this.k8s = k8s;
    }

    @Override
    protected String getLockName(String namespace, String name) {
        return "lock::kafka::" + namespace + "::" + name;
    }

    public void execute(String namespace, String name, Handler<AsyncResult<Void>> handler) {

        final String lockName = getLockName(namespace, name);
        vertx.sharedData().getLockWithTimeout(lockName, LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                Lock lock = res.result();

                ClusterDiffResult diff;
                KafkaCluster kafka;
                ConfigMap kafkaConfigMap = k8s.getConfigmap(namespace, name);

                if (kafkaConfigMap != null)    {

                    try {

                        kafka = KafkaCluster.fromConfigMap(kafkaConfigMap);
                        log.info("Updating Kafka cluster {} in namespace {}", kafka.getName(), namespace);
                        diff = kafka.diff(k8s, namespace);

                    } catch (Exception ex) {

                        log.error("Error while parsing cluster ConfigMap", ex);
                        handler.handle(Future.failedFuture("ConfigMap parsing error"));
                        lock.release();
                        return;
                    }

                } else {
                    log.error("ConfigMap {} doesn't exist anymore in namespace {}", name, namespace);
                    handler.handle(Future.failedFuture("ConfigMap doesn't exist anymore"));
                    lock.release();
                    return;
                }

                Future<Void> chainFuture = Future.future();

                scaleDown(kafka, namespace, diff)
                        .compose(i -> patchService(kafka, namespace, diff))
                        .compose(i -> patchHeadlessService(kafka, namespace, diff))
                        .compose(i -> patchStatefulSet(kafka, namespace, diff))
                        .compose(i -> patchMetricsConfigMap(kafka, namespace, diff))
                        .compose(i -> rollingUpdate(kafka, namespace, diff))
                        .compose(i -> scaleUp(kafka, namespace, diff))
                        .compose(chainFuture::complete, chainFuture);

                chainFuture.setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.info("Kafka cluster {} successfully updated in namespace {}", kafka.getName(), namespace);
                        handler.handle(Future.succeededFuture());
                        lock.release();
                    } else {
                        log.error("Kafka cluster {} failed to update in namespace {}", kafka.getName(), namespace);
                        handler.handle(Future.failedFuture("Failed to update Zookeeper cluster"));
                        lock.release();
                    }
                });
            } else {
                log.error("Failed to acquire lock to create Kafka cluster {}", lockName);
                handler.handle(Future.failedFuture("Failed to acquire lock to create Kafka cluster"));
            }
        });
    }

    private Future<Void> scaleDown(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
        Future<Void> scaleDown = Future.future();

        if (diff.getScaleDown())    {
            log.info("Scaling down stateful set {} in namespace {}", kafka.getName(), namespace);
            OperationExecutor.getInstance().executeK8s(new ScaleDownOperation(k8s.getStatefulSetResource(namespace, kafka.getName()), kafka.getReplicas()), scaleDown.completer());
        }
        else {
            scaleDown.complete();
        }

        return scaleDown;
    }

    private Future<Void> patchService(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            Future<Void> patchService = Future.future();
            OperationExecutor.getInstance().executeK8s(new PatchOperation(k8s.getServiceResource(namespace, kafka.getName()), kafka.patchService(k8s.getService(namespace, kafka.getName()))), patchService.completer());
            return patchService;
        }
            else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchHeadlessService(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            Future<Void> patchService = Future.future();
            OperationExecutor.getInstance().executeK8s(new PatchOperation(k8s.getServiceResource(namespace, kafka.getHeadlessName()), kafka.patchHeadlessService(k8s.getService(namespace, kafka.getHeadlessName()))), patchService.completer());
            return patchService;
        }
            else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchStatefulSet(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            Future<Void> patchStatefulSet = Future.future();
            OperationExecutor.getInstance().executeK8s(new PatchOperation(k8s.getStatefulSetResource(namespace, kafka.getName()).cascading(false), kafka.patchStatefulSet(k8s.getStatefulSet(namespace, kafka.getName()))), patchStatefulSet.completer());
            return patchStatefulSet;
        }
        else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchMetricsConfigMap(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
        if (diff.isMetricsChanged()) {
            Future<Void> patchConfigMap = Future.future();
            OperationExecutor.getInstance().executeK8s(new PatchOperation(k8s.getConfigmapResource(namespace, kafka.getMetricsConfigName()), kafka.patchMetricsConfigMap(k8s.getConfigmap(namespace, kafka.getMetricsConfigName()))), patchConfigMap.completer());
            return patchConfigMap;
        } else {
            return Future.succeededFuture();
        }
    }

    private Future<Void> rollingUpdate(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
        Future<Void> rollingUpdate = Future.future();

        if (diff.getRollingUpdate()) {
            OperationExecutor.getInstance().executeK8s(new ManualRollingUpdateOperation(namespace, kafka.getName(), k8s.getStatefulSet(namespace, kafka.getName()).getSpec().getReplicas()), rollingUpdate.completer());
        }
        else {
            rollingUpdate.complete();
        }

        return rollingUpdate;
    }

    private Future<Void> scaleUp(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
        Future<Void> scaleUp = Future.future();

        if (diff.getScaleUp()) {
            OperationExecutor.getInstance().executeK8s(new ScaleUpOperation(k8s.getStatefulSetResource(namespace, kafka.getName()), kafka.getReplicas()), scaleUp.completer());
        }
        else {
            scaleUp.complete();
        }

        return scaleUp;
    }
}
