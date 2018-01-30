package io.strimzi.controller.cluster.operations;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.api.model.DoneablePersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.DoneableService;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimList;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.api.model.extensions.DoneableStatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.operations.kubernetes.ManualRollingUpdateOperation;
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
    private final ResourceOperation<KubernetesClient, ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> configMapResources;
    private final ResourceOperation<KubernetesClient, StatefulSet, StatefulSetList, DoneableStatefulSet, RollableScalableResource<StatefulSet, DoneableStatefulSet>> statefulSetResources;
    private final ResourceOperation<KubernetesClient, Service, ServiceList, DoneableService, Resource<Service, DoneableService>> serviceResources;

    public KafkaClusterOperation(Vertx vertx, K8SUtils k8s) {
        super(vertx, k8s, "kafka", "create");
        configMapResources = ResourceOperation.configMap(vertx, k8s.getKubernetesClient());
        statefulSetResources = ResourceOperation.statefulSet(vertx, k8s.getKubernetesClient());
        serviceResources = ResourceOperation.service(vertx, k8s.getKubernetesClient());
        pvcResources = ResourceOperation.persistentVolumeClaim(vertx, k8s.getKubernetesClient());
    }

    private final Op<KafkaCluster> create = new Op<KafkaCluster>() {

        @Override
        public KafkaCluster getCluster (K8SUtils k8s, String namespace, String name){
            return KafkaCluster.fromConfigMap(configMapResources.get(namespace, name));
        }

        @Override
        public List<Future> futures (K8SUtils k8s, String namespace, KafkaCluster kafka){
            List<Future> result = new ArrayList<>(4);
            // start creating configMap operation only if metrics are enabled,
            // otherwise the future is already complete (for the "join")
            if (kafka.isMetricsEnabled()) {
                Future<Void> futureConfigMap = Future.future();
                configMapResources.create(kafka.generateMetricsConfigMap(), futureConfigMap.completer());
                result.add(futureConfigMap);
            }

            Future<Void> futureService = Future.future();
            serviceResources.create(kafka.generateService(), futureService.completer());
            result.add(futureService);

            Future<Void> futureHeadlessService = Future.future();
            serviceResources.create(kafka.generateHeadlessService(), futureHeadlessService.completer());
            result.add(futureHeadlessService);

            Future<Void> futureStatefulSet = Future.future();
            statefulSetResources.create(kafka.generateStatefulSet(k8s.isOpenShift()), futureStatefulSet.completer());
            result.add(futureStatefulSet);

            return result;
        }
    };

    private ResourceOperation<KubernetesClient, PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim, Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>> pvcResources;
    private final Op<KafkaCluster> delete = new Op<KafkaCluster>() {
        @Override
        public List<Future> futures(K8SUtils k8s, String namespace, KafkaCluster kafka) {
            boolean deleteClaims = kafka.getStorage().type() == Storage.StorageType.PERSISTENT_CLAIM
                    && kafka.getStorage().isDeleteClaim();
            List<Future> result = new ArrayList<>(4 + (deleteClaims ? kafka.getReplicas() : 0));

            if (kafka.isMetricsEnabled()) {
                Future<Void> futureConfigMap = Future.future();
                configMapResources.delete(namespace, kafka.getMetricsConfigName(), futureConfigMap.completer());
                result.add(futureConfigMap);
            }

            Future<Void> futureService = Future.future();
            serviceResources.delete(namespace, kafka.getName(), futureService.completer());
            result.add(futureService);

            Future<Void> futureHeadlessService = Future.future();
            serviceResources.delete(namespace, kafka.getHeadlessName(), futureHeadlessService.completer());
            result.add(futureHeadlessService);

            Future<Void> futureStatefulSet = Future.future();
            statefulSetResources.delete(namespace, kafka.getName(), futureStatefulSet.completer());
            result.add(futureStatefulSet);

            if (deleteClaims) {
                for (int i = 0; i < kafka.getReplicas(); i++) {
                    Future<Void> f = Future.future();
                    pvcResources.delete(namespace, kafka.getVolumeName() + "-" + kafka.getName() + "-" + i, f.completer());
                    result.add(f);
                }
            }

            return result;
        }

        @Override
        public KafkaCluster getCluster(K8SUtils k8s, String namespace, String name) {
            return KafkaCluster.fromStatefulSet(statefulSetResources, namespace, name);
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
                ConfigMap kafkaConfigMap = configMapResources.get(namespace, name);

                if (kafkaConfigMap != null)    {

                    try {

                        kafka = KafkaCluster.fromConfigMap(kafkaConfigMap);
                        log.info("Updating Kafka cluster {} in namespace {}", kafka.getName(), namespace);
                        diff = kafka.diff(configMapResources, statefulSetResources, namespace);

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
            new ScaleDownOperation(vertx, k8s).scaleDown(k8s.getStatefulSetResource(namespace, kafka.getName()), kafka.getReplicas(), scaleDown.completer());
        }
        else {
            scaleDown.complete();
        }

        return scaleDown;
    }

    private Future<Void> patchService(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            Future<Void> patchService = Future.future();
            serviceResources.patch(namespace, kafka.getName(), kafka.patchService(serviceResources.get(namespace, kafka.getName())), patchService.completer());
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
            serviceResources.patch(namespace, kafka.getHeadlessName(),
                    kafka.patchHeadlessService(serviceResources.get(namespace, kafka.getHeadlessName())), patchService.completer());
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
            statefulSetResources.patch(namespace, kafka.getName(), false,
                    kafka.patchStatefulSet(statefulSetResources.get(namespace, kafka.getName())), patchStatefulSet.completer());
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
            configMapResources.patch(namespace, kafka.getMetricsConfigName(),
                    kafka.patchMetricsConfigMap(configMapResources.get(namespace, kafka.getMetricsConfigName())), patchConfigMap.completer());
            return patchConfigMap;
        } else {
            return Future.succeededFuture();
        }
    }

    private Future<Void> rollingUpdate(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
        Future<Void> rollingUpdate = Future.future();

        if (diff.getRollingUpdate()) {
            new ManualRollingUpdateOperation(vertx, k8s).rollingUpdate(namespace, kafka.getName(), k8s.getStatefulSet(namespace, kafka.getName()).getSpec().getReplicas(), rollingUpdate.completer());
        }
        else {
            rollingUpdate.complete();
        }

        return rollingUpdate;
    }

    private Future<Void> scaleUp(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
        Future<Void> scaleUp = Future.future();

        if (diff.getScaleUp()) {
            new ScaleUpOperation(vertx, k8s).scaleUp(k8s.getStatefulSetResource(namespace, kafka.getName()), kafka.getReplicas(), scaleUp.completer());
        }
        else {
            scaleUp.complete();
        }

        return scaleUp;
    }
}
