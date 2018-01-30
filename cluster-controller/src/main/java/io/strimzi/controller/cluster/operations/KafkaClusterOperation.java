package io.strimzi.controller.cluster.operations;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.operations.kubernetes.ManualRollingUpdateOperation;
import io.strimzi.controller.cluster.operations.kubernetes.PatchOperation;
import io.strimzi.controller.cluster.operations.kubernetes.ScaleDownOperation;
import io.strimzi.controller.cluster.operations.kubernetes.ScaleUpOperation;
import io.strimzi.controller.cluster.resources.ClusterDiffResult;
import io.strimzi.controller.cluster.resources.KafkaCluster;
import io.strimzi.controller.cluster.resources.Storage;
import io.vertx.core.*;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class KafkaClusterOperation extends ClusterOperation<KafkaCluster> {
    private static final Logger log = LoggerFactory.getLogger(KafkaClusterOperation.class.getName());
    private final PatchOperation patchOperation;

    public KafkaClusterOperation(Vertx vertx, K8SUtils k8s) {
        super(vertx, k8s, "kafka", "create");
        patchOperation = new PatchOperation(vertx);
    }

    private final Op<KafkaCluster> create = new Op<KafkaCluster>() {

        @Override
        public KafkaCluster getCluster (K8SUtils k8s, String namespace, String name){
            return KafkaCluster.fromConfigMap(k8s.getConfigmap(namespace, name));
        }

        @Override
        public List<Future> futures (K8SUtils k8s, String namespace, KafkaCluster kafka){
            List<Future> result = new ArrayList<>(4);
            // start creating configMap operation only if metrics are enabled,
            // otherwise the future is already complete (for the "join")
            if (kafka.isMetricsEnabled()) {
                Future<Void> futureConfigMap = Future.future();
                CreateOperation.createConfigMap(vertx, k8s.getKubernetesClient()).create(kafka.generateMetricsConfigMap(), futureConfigMap.completer());
                result.add(futureConfigMap);
            }

            Future<Void> futureService = Future.future();
            CreateOperation.createService(vertx, k8s.getKubernetesClient()).create(kafka.generateService(), futureService.completer());
            result.add(futureService);

            Future<Void> futureHeadlessService = Future.future();
            CreateOperation.createService(vertx, k8s.getKubernetesClient()).create(kafka.generateHeadlessService(), futureHeadlessService.completer());
            result.add(futureHeadlessService);

            Future<Void> futureStatefulSet = Future.future();
            CreateOperation.createStatefulSet(vertx, k8s.getKubernetesClient()).create(kafka.generateStatefulSet(k8s.isOpenShift()), futureStatefulSet.completer());
            result.add(futureStatefulSet);

            return result;
        }
    };

    private final Op<KafkaCluster> delete = new Op<KafkaCluster>() {
        @Override
        public List<Future> futures(K8SUtils k8s, String namespace, KafkaCluster kafka) {
            boolean deleteClaims = kafka.getStorage().type() == Storage.StorageType.PERSISTENT_CLAIM
                    && kafka.getStorage().isDeleteClaim();
            List<Future> result = new ArrayList<>(4 + (deleteClaims ? kafka.getReplicas() : 0));

            if (kafka.isMetricsEnabled()) {
                Future<Void> futureConfigMap = Future.future();
                DeleteOperation.deleteConfigMap(namespace, kafka.getMetricsConfigName()).delete(vertx, k8s.getKubernetesClient(), futureConfigMap.completer());
                result.add(futureConfigMap);
            }

            Future<Void> futureService = Future.future();
            DeleteOperation.deleteService(namespace, kafka.getName()).delete(vertx, k8s.getKubernetesClient(), futureService.completer());
            result.add(futureService);

            Future<Void> futureHeadlessService = Future.future();
            DeleteOperation.deleteService(namespace, kafka.getHeadlessName()).delete(vertx, k8s.getKubernetesClient(), futureHeadlessService.completer());
            result.add(futureHeadlessService);

            Future<Void> futureStatefulSet = Future.future();
            DeleteOperation.deleteStatefulSet(namespace, kafka.getName()).delete(vertx, k8s.getKubernetesClient(), futureStatefulSet.completer());
            result.add(futureStatefulSet);

            if (deleteClaims) {
                for (int i = 0; i < kafka.getReplicas(); i++) {
                    Future<Void> f = Future.future();
                    DeleteOperation.deletePersistentVolumeClaim(namespace, kafka.getVolumeName() + "-" + kafka.getName() + "-" + i).delete(vertx, k8s.getKubernetesClient(), f.completer());
                    result.add(f);
                }
            }

            return result;
        }

        @Override
        public KafkaCluster getCluster(K8SUtils k8s, String namespace, String name) {
            return KafkaCluster.fromStatefulSet(k8s, namespace, name);
        }
    };

    @Override
    protected Op<KafkaCluster> createOp() {
        return create;
    }

    @Override
    protected Op<KafkaCluster> deleteOp() {
        return delete;
    }

    public void update(String namespace, String name, Handler<AsyncResult<Void>> handler) {

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
            new ScaleDownOperation(k8s.getStatefulSetResource(namespace, kafka.getName()), kafka.getReplicas()).scaleDown(vertx, k8s, scaleDown.completer());
        }
        else {
            scaleDown.complete();
        }

        return scaleDown;
    }

    private Future<Void> patchService(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            Future<Void> patchService = Future.future();
            patchOperation.patch(k8s.getServiceResource(namespace, kafka.getName()), kafka.patchService(k8s.getService(namespace, kafka.getName())), patchService.completer());
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
            patchOperation.patch(k8s.getServiceResource(namespace, kafka.getHeadlessName()), kafka.patchHeadlessService(k8s.getService(namespace, kafka.getHeadlessName())), patchService.completer());
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
            patchOperation.patch(k8s.getStatefulSetResource(namespace, kafka.getName()).cascading(false), kafka.patchStatefulSet(k8s.getStatefulSet(namespace, kafka.getName())), patchStatefulSet.completer());
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
            patchOperation.patch(k8s.getConfigmapResource(namespace, kafka.getMetricsConfigName()), kafka.patchMetricsConfigMap(k8s.getConfigmap(namespace, kafka.getMetricsConfigName())), patchConfigMap.completer());
            return patchConfigMap;
        } else {
            return Future.succeededFuture();
        }
    }

    private Future<Void> rollingUpdate(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
        Future<Void> rollingUpdate = Future.future();

        if (diff.getRollingUpdate()) {
            new ManualRollingUpdateOperation(namespace, kafka.getName(), k8s.getStatefulSet(namespace, kafka.getName()).getSpec().getReplicas()).rollingUpdate(vertx, k8s, rollingUpdate.completer());
        }
        else {
            rollingUpdate.complete();
        }

        return rollingUpdate;
    }

    private Future<Void> scaleUp(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
        Future<Void> scaleUp = Future.future();

        if (diff.getScaleUp()) {
            new ScaleUpOperation(k8s.getStatefulSetResource(namespace, kafka.getName()), kafka.getReplicas()).scaleUp(vertx, k8s, scaleUp.completer());
        }
        else {
            scaleUp.complete();
        }

        return scaleUp;
    }
}
