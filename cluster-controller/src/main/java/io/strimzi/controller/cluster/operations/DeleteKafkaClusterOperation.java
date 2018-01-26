package io.strimzi.controller.cluster.operations;

import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.resources.KafkaCluster;
import io.strimzi.controller.cluster.resources.Storage;
import io.vertx.core.*;
import io.vertx.core.shareddata.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DeleteKafkaClusterOperation extends SimpleClusterOperation<KafkaCluster> {
    private static final Logger log = LoggerFactory.getLogger(DeleteKafkaClusterOperation.class.getName());

    public DeleteKafkaClusterOperation(String namespace, String name) {
        super("kafka", "delete", namespace, name);
    }

    @Override
    protected List<Future> futures(K8SUtils k8s, KafkaCluster kafka) {
        boolean deleteClaims = kafka.getStorage().type() == Storage.StorageType.PERSISTENT_CLAIM
                && kafka.getStorage().isDeleteClaim();
        List<Future> result = new ArrayList<>(4 + (deleteClaims ? kafka.getReplicas() : 0));

        if (kafka.isMetricsEnabled()) {
            Future<Void> futureConfigMap = Future.future();
            OperationExecutor.getInstance().executeK8s(DeleteOperation.deleteConfigMap(namespace, kafka.getMetricsConfigName()), futureConfigMap.completer());
            result.add(futureConfigMap);
        }

        Future<Void> futureService = Future.future();
        OperationExecutor.getInstance().executeFabric8(DeleteOperation.deleteService(namespace, kafka.getName()), futureService.completer());
        result.add(futureService);

        Future<Void> futureHeadlessService = Future.future();
        OperationExecutor.getInstance().executeFabric8(DeleteOperation.deleteService(namespace, kafka.getHeadlessName()), futureHeadlessService.completer());
        result.add(futureHeadlessService);

        Future<Void> futureStatefulSet = Future.future();
        OperationExecutor.getInstance().executeK8s(DeleteOperation.deleteStatefulSet(namespace, kafka.getName()), futureStatefulSet.completer());
        result.add(futureStatefulSet);

        if (deleteClaims) {
            for (int i = 0; i < kafka.getReplicas(); i++) {
                Future<Void> f = Future.future();
                OperationExecutor.getInstance().executeK8s(DeleteOperation.deletePersistentVolumeClaim(namespace, kafka.getVolumeName() + "-" + kafka.getName() + "-" + i), f.completer());
                result.add(f);
            }
        }

        return result;
    }

    @Override
    protected KafkaCluster getCluster(K8SUtils k8s, Handler<AsyncResult<Void>> handler, Lock lock) {
        return KafkaCluster.fromStatefulSet(k8s, namespace, name);
    }
}
