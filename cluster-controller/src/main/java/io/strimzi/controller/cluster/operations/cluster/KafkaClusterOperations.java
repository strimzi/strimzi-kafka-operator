package io.strimzi.controller.cluster.operations.cluster;

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

/**
 * CRUD-style operations on a Kafka cluster
 */
public class KafkaClusterOperations extends AbstractClusterOperations<KafkaCluster> {
    private static final Logger log = LoggerFactory.getLogger(KafkaClusterOperations.class.getName());
    private final ConfigMapOperations configMapOperations;
    private final StatefulSetOperations statefulSetOperations;
    private final ServiceOperations serviceOperations;
    private final PvcOperations pvcOperations;

    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The kubernetes client
     * @param configMapOperations For operating on ConfigMaps
     * @param serviceOperations For operating on Services
     * @param statefulSetOperations For operating on StatefulSets
     * @param pvcOperations For operating on PersistentVolumeClaims
     */
    public KafkaClusterOperations(Vertx vertx, KubernetesClient client,
                                  ConfigMapOperations configMapOperations,
                                  ServiceOperations serviceOperations,
                                  StatefulSetOperations statefulSetOperations,
                                  PvcOperations pvcOperations) {
        super(vertx, client, "kafka", "create");
        this.configMapOperations = configMapOperations;
        this.statefulSetOperations = statefulSetOperations;
        this.serviceOperations = serviceOperations;
        this.pvcOperations = pvcOperations;
    }

    private final CompositeOperation<KafkaCluster> create = new CompositeOperation<KafkaCluster>() {

        @Override
        public ClusterOperation<KafkaCluster> getCluster(String namespace, String name){
            return new ClusterOperation<>(KafkaCluster.fromConfigMap(configMapOperations.get(namespace, name)), null);
        }

        @Override
        public Future<?> composite(String namespace, ClusterOperation<KafkaCluster> clusterOp){
            KafkaCluster kafka = clusterOp.cluster();
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

            return CompositeFuture.join(result);
        }
    };

    private final CompositeOperation<KafkaCluster> delete = new CompositeOperation<KafkaCluster>() {
        @Override
        public Future<?> composite(String namespace, ClusterOperation<KafkaCluster> clusterOp) {
            KafkaCluster kafka = clusterOp.cluster();
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

            return CompositeFuture.join(result);
        }

        @Override
        public ClusterOperation<KafkaCluster> getCluster(String namespace, String name) {
            return new ClusterOperation<>(KafkaCluster.fromStatefulSet(statefulSetOperations, namespace, name), null);
        }
    };

    @Override
    protected CompositeOperation<KafkaCluster> createOp() {
        return create;
    }

    @Override
    protected CompositeOperation<KafkaCluster> deleteOp() {
        return delete;
    }

    @Override
    protected CompositeOperation<KafkaCluster> updateOp() {
        return update;
    }

    private final CompositeOperation<KafkaCluster> update = new CompositeOperation<KafkaCluster>() {
        @Override
        public Future<?> composite(String namespace, ClusterOperation<KafkaCluster> clusterOp) {
            KafkaCluster kafka = clusterOp.cluster();
            ClusterDiffResult diff = clusterOp.diff();

            Future<Void> chainFuture = Future.future();
            scaleDown(kafka, namespace, diff)
                    .compose(i -> patchService(kafka, namespace, diff))
                    .compose(i -> patchHeadlessService(kafka, namespace, diff))
                    .compose(i -> patchStatefulSet(kafka, namespace, diff))
                    .compose(i -> patchMetricsConfigMap(kafka, namespace, diff))
                    .compose(i -> rollingUpdate(kafka, namespace, diff))
                    .compose(i -> scaleUp(kafka, namespace, diff))
                    .compose(chainFuture::complete, chainFuture);

            return chainFuture;
        }

        @Override
        public ClusterOperation<KafkaCluster> getCluster(String namespace, String name) {
            ClusterDiffResult diff;
            KafkaCluster kafka;
            ConfigMap kafkaConfigMap = configMapOperations.get(namespace, name);

            if (kafkaConfigMap != null)    {
                kafka = KafkaCluster.fromConfigMap(kafkaConfigMap);
                log.info("Updating Kafka cluster {} in namespace {}", kafka.getName(), namespace);
                diff = kafka.diff(configMapOperations, statefulSetOperations, namespace);
            } else {
                throw new IllegalStateException("ConfigMap " + name + " doesn't exist anymore in namespace " + namespace);
            }
            return new ClusterOperation<>(kafka, diff);
        }
    };

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
