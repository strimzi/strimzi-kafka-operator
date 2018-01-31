package io.strimzi.controller.cluster.operations;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.controller.cluster.operations.resource.ConfigMapOperations;
import io.strimzi.controller.cluster.operations.resource.PvcOperations;
import io.strimzi.controller.cluster.operations.resource.ServiceOperations;
import io.strimzi.controller.cluster.operations.resource.StatefulSetOperations;
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
    private final ConfigMapOperations configMapOperations;
    private final StatefulSetOperations statefulSetOperations;
    private final ServiceOperations serviceOperations;
    private final PvcOperations pvcOperations;

    public KafkaClusterOperation(Vertx vertx, KubernetesClient client, ConfigMapOperations configMapOperations, StatefulSetOperations statefulSetOperations, ServiceOperations serviceOperations, PvcOperations pvcOperations) {
        super(vertx, client, "kafka", "create");
        this.configMapOperations = configMapOperations;
        this.statefulSetOperations = statefulSetOperations;
        this.serviceOperations = serviceOperations;
        this.pvcOperations = pvcOperations;
    }

    private final Op<KafkaCluster> create = new Op<KafkaCluster>() {

        @Override
        public KafkaCluster getCluster (KubernetesClient client, String namespace, String name){
            return KafkaCluster.fromConfigMap(configMapOperations.get(namespace, name));
        }

        @Override
        public List<Future> futures (KubernetesClient client, String namespace, KafkaCluster kafka){
            List<Future> result = new ArrayList<>(4);
            // start creating configMap operation only if metrics are enabled,
            // otherwise the future is already complete (for the "join")
            if (kafka.isMetricsEnabled()) {
                Future<Void> futureConfigMap = Future.future();
                configMapOperations.create(kafka.generateMetricsConfigMap(), futureConfigMap.completer());
                result.add(futureConfigMap);
            }

            Future<Void> futureService = Future.future();
            serviceOperations.create(kafka.generateService(), futureService.completer());
            result.add(futureService);

            Future<Void> futureHeadlessService = Future.future();
            serviceOperations.create(kafka.generateHeadlessService(), futureHeadlessService.completer());
            result.add(futureHeadlessService);

            Future<Void> futureStatefulSet = Future.future();
            statefulSetOperations.create(kafka.generateStatefulSet(client.isAdaptable(OpenShiftClient.class)), futureStatefulSet.completer());
            result.add(futureStatefulSet);

            return result;
        }
    };
    private final Op<KafkaCluster> delete = new Op<KafkaCluster>() {
        @Override
        public List<Future> futures(KubernetesClient client, String namespace, KafkaCluster kafka) {
            boolean deleteClaims = kafka.getStorage().type() == Storage.StorageType.PERSISTENT_CLAIM
                    && kafka.getStorage().isDeleteClaim();
            List<Future> result = new ArrayList<>(4 + (deleteClaims ? kafka.getReplicas() : 0));

            if (kafka.isMetricsEnabled()) {
                Future<Void> futureConfigMap = Future.future();
                configMapOperations.delete(namespace, kafka.getMetricsConfigName(), futureConfigMap.completer());
                result.add(futureConfigMap);
            }

            Future<Void> futureService = Future.future();
            serviceOperations.delete(namespace, kafka.getName(), futureService.completer());
            result.add(futureService);

            Future<Void> futureHeadlessService = Future.future();
            serviceOperations.delete(namespace, kafka.getHeadlessName(), futureHeadlessService.completer());
            result.add(futureHeadlessService);

            Future<Void> futureStatefulSet = Future.future();
            statefulSetOperations.delete(namespace, kafka.getName(), futureStatefulSet.completer());
            result.add(futureStatefulSet);

            if (deleteClaims) {
                for (int i = 0; i < kafka.getReplicas(); i++) {
                    Future<Void> f = Future.future();
                    pvcOperations.delete(namespace, kafka.getVolumeName() + "-" + kafka.getName() + "-" + i, f.completer());
                    result.add(f);
                }
            }

            return result;
        }

        @Override
        public KafkaCluster getCluster(KubernetesClient client, String namespace, String name) {
            return KafkaCluster.fromStatefulSet(statefulSetOperations, namespace, name);
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
                ConfigMap kafkaConfigMap = configMapOperations.get(namespace, name);

                if (kafkaConfigMap != null)    {

                    try {

                        kafka = KafkaCluster.fromConfigMap(kafkaConfigMap);
                        log.info("Updating Kafka cluster {} in namespace {}", kafka.getName(), namespace);
                        diff = kafka.diff(configMapOperations, statefulSetOperations, namespace);

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
            statefulSetOperations.scaleDown(namespace, kafka.getName(), kafka.getReplicas(), scaleDown.completer());
        }
        else {
            scaleDown.complete();
        }

        return scaleDown;
    }

    private Future<Void> patchService(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            Future<Void> patchService = Future.future();
            serviceOperations.patch(namespace, kafka.getName(), kafka.patchService(serviceOperations.get(namespace, kafka.getName())), patchService.completer());
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
            serviceOperations.patch(namespace, kafka.getHeadlessName(),
                    kafka.patchHeadlessService(serviceOperations.get(namespace, kafka.getHeadlessName())), patchService.completer());
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
            statefulSetOperations.patch(namespace, kafka.getName(), false,
                    kafka.patchStatefulSet(statefulSetOperations.get(namespace, kafka.getName())), patchStatefulSet.completer());
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
            configMapOperations.patch(namespace, kafka.getMetricsConfigName(),
                    kafka.patchMetricsConfigMap(configMapOperations.get(namespace, kafka.getMetricsConfigName())), patchConfigMap.completer());
            return patchConfigMap;
        } else {
            return Future.succeededFuture();
        }
    }

    private Future<Void> rollingUpdate(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
        Future<Void> rollingUpdate = Future.future();

        if (diff.getRollingUpdate()) {
            statefulSetOperations.rollingUpdate(namespace, kafka.getName(),
                    rollingUpdate.completer());
        }
        else {
            rollingUpdate.complete();
        }

        return rollingUpdate;
    }

    private Future<Void> scaleUp(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
        Future<Void> scaleUp = Future.future();

        if (diff.getScaleUp()) {
            statefulSetOperations.scaleUp(namespace, kafka.getName(), kafka.getReplicas(), scaleUp.completer());
        }
        else {
            scaleUp.complete();
        }

        return scaleUp;
    }
}
