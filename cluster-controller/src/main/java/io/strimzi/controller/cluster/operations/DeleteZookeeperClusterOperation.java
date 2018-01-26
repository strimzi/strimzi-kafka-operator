package io.strimzi.controller.cluster.operations;

import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.operations.kubernetes.DeletePersistentVolumeClaimOperation;
import io.strimzi.controller.cluster.resources.Storage;
import io.strimzi.controller.cluster.resources.ZookeeperCluster;
import io.vertx.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.shareddata.Lock;

import java.util.ArrayList;
import java.util.List;

public class DeleteZookeeperClusterOperation extends SimpleClusterOperation<ZookeeperCluster> {
    private static final Logger log = LoggerFactory.getLogger(DeleteZookeeperClusterOperation.class.getName());

    public DeleteZookeeperClusterOperation(String namespace, String name) {
        super("zookeeper", "delete", namespace, name);
    }

    @Override
    protected List<Future> creationFutures(K8SUtils k8s, ZookeeperCluster zk) {
        boolean deleteClaims = zk.getStorage().type() == Storage.StorageType.PERSISTENT_CLAIM
                && zk.getStorage().isDeleteClaim();
        List<Future> result = new ArrayList<>(4 + (deleteClaims ? zk.getReplicas() : 0));

        // start deleting configMap operation only if metrics are enabled,
        // otherwise the future is already complete (for the "join")
        if (zk.isMetricsEnabled()) {
            Future<Void> futureConfigMap = Future.future();
            OperationExecutor.getInstance().executeK8s(DeleteOperation.deleteConfigMap(namespace, zk.getMetricsConfigName()), futureConfigMap.completer());
            result.add(futureConfigMap);
        }

        Future<Void> futureService = Future.future();
        OperationExecutor.getInstance().executeFabric8(DeleteOperation.deleteService(namespace, zk.getName()), futureService.completer());
        result.add(futureService);

        Future<Void> futureHeadlessService = Future.future();
        OperationExecutor.getInstance().executeFabric8(DeleteOperation.deleteService(namespace, zk.getHeadlessName()), futureHeadlessService.completer());
        result.add(futureHeadlessService);

        Future<Void> futureStatefulSet = Future.future();
        OperationExecutor.getInstance().executeK8s(DeleteOperation.deleteStatefulSet(namespace, zk.getName()), futureStatefulSet.completer());
        result.add(futureStatefulSet);


        if (deleteClaims) {
            for (int i = 0; i < zk.getReplicas(); i++) {
                Future<Void> f = Future.future();
                OperationExecutor.getInstance().executeK8s(new DeletePersistentVolumeClaimOperation(namespace, zk.getVolumeName() + "-" + zk.getName() + "-" + i), f.completer());
                result.add(f);
            }
        }
        return result;
    }

    @Override
    protected ZookeeperCluster getCluster(K8SUtils k8s, Handler<AsyncResult<Void>> handler, Lock lock) {
        return ZookeeperCluster.fromStatefulSet(k8s, namespace, name);
    }
}
