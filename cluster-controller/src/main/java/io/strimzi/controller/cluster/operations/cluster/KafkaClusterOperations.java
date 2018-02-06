package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.strimzi.controller.cluster.operations.resource.ConfigMapOperations;
import io.strimzi.controller.cluster.operations.resource.PvcOperations;
import io.strimzi.controller.cluster.operations.resource.ServiceOperations;
import io.strimzi.controller.cluster.operations.resource.StatefulSetOperations;
import io.strimzi.controller.cluster.resources.ClusterDiffResult;
import io.strimzi.controller.cluster.resources.KafkaCluster;
import io.strimzi.controller.cluster.resources.Storage;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
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
     * @param isOpenShift Whether we're running with OpenShift
     * @param configMapOperations For operating on ConfigMaps
     * @param serviceOperations For operating on Services
     * @param statefulSetOperations For operating on StatefulSets
     * @param pvcOperations For operating on PersistentVolumeClaims
     */
    public KafkaClusterOperations(Vertx vertx, boolean isOpenShift,
                                  ConfigMapOperations configMapOperations,
                                  ServiceOperations serviceOperations,
                                  StatefulSetOperations statefulSetOperations,
                                  PvcOperations pvcOperations) {
        super(vertx, isOpenShift, "kafka", "create");
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
                result.add(configMapOperations.create(kafka.generateMetricsConfigMap()));
            }

            result.add(serviceOperations.create(kafka.generateService()));

            result.add(serviceOperations.create(kafka.generateHeadlessService()));

            result.add(statefulSetOperations.create(kafka.generateStatefulSet(isOpenShift)));

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
                result.add(configMapOperations.delete(namespace, kafka.getMetricsConfigName()));
            }

            result.add(serviceOperations.delete(namespace, kafka.getName()));

            result.add(serviceOperations.delete(namespace, kafka.getHeadlessName()));

            result.add(statefulSetOperations.delete(namespace, kafka.getName()));

            if (deleteClaims) {
                for (int i = 0; i < kafka.getReplicas(); i++) {
                    result.add(pvcOperations.delete(namespace, kafka.getVolumeName() + "-" + kafka.getName() + "-" + i));
                }
            }

            return CompositeFuture.join(result);
        }

        @Override
        public ClusterOperation<KafkaCluster> getCluster(String namespace, String name) {
            StatefulSet ss = statefulSetOperations.get(namespace, KafkaCluster.kafkaClusterName(name));
            return new ClusterOperation<>(KafkaCluster.fromStatefulSet(ss, namespace, name), null);
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
                StatefulSet ss = statefulSetOperations.get(namespace, kafka.getName());
                ConfigMap metricsConfigMap = configMapOperations.get(namespace, kafka.getMetricsConfigName());
                diff = kafka.diff(metricsConfigMap, ss);
            } else {
                throw new IllegalStateException("ConfigMap " + name + " doesn't exist anymore in namespace " + namespace);
            }
            return new ClusterOperation<>(kafka, diff);
        }
    };

    private Future<Void> scaleDown(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
        Future<Void> scaleDown = Future.future();

        if (diff.isScaleDown())    {
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
            return serviceOperations.patch(namespace, kafka.getName(), kafka.patchService(serviceOperations.get(namespace, kafka.getName())));
        }
        else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchHeadlessService(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            return serviceOperations.patch(namespace, kafka.getHeadlessName(),
                    kafka.patchHeadlessService(serviceOperations.get(namespace, kafka.getHeadlessName())));
        }
        else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchStatefulSet(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
        if (diff.getDifferent()) {
            return statefulSetOperations.patch(namespace, kafka.getName(), false,
                    kafka.patchStatefulSet(statefulSetOperations.get(namespace, kafka.getName())));
        }
        else
        {
            return Future.succeededFuture();
        }
    }

    private Future<Void> patchMetricsConfigMap(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
        if (diff.isMetricsChanged()) {
            return configMapOperations.patch(namespace, kafka.getMetricsConfigName(),
                    kafka.patchMetricsConfigMap(configMapOperations.get(namespace, kafka.getMetricsConfigName())));
        } else {
            return Future.succeededFuture();
        }
    }

    private Future<Void> rollingUpdate(KafkaCluster kafka, String namespace, ClusterDiffResult diff) {
        Future<Void> rollingUpdate = Future.future();

        if (diff.isRollingUpdate()) {
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

        if (diff.isScaleUp()) {
            statefulSetOperations.scaleUp(namespace, kafka.getName(), kafka.getReplicas(), scaleUp.completer());
        }
        else {
            scaleUp.complete();
        }

        return scaleUp;
    }
}
