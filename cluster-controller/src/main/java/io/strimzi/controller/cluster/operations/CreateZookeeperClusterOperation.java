package io.strimzi.controller.cluster.operations;

import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.resources.ZookeeperCluster;
import io.vertx.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.shareddata.Lock;

import java.util.ArrayList;
import java.util.List;

public class CreateZookeeperClusterOperation extends CreateClusterOperation<ZookeeperCluster> {
    private static final Logger log = LoggerFactory.getLogger(CreateZookeeperClusterOperation.class.getName());

    public CreateZookeeperClusterOperation(String namespace, String name) {
        super("zookeeper", namespace, name);
    }

    @Override
    protected ZookeeperCluster getCluster(K8SUtils k8s, Handler<AsyncResult<Void>> handler, Lock lock) {
        return ZookeeperCluster.fromConfigMap(k8s.getConfigmap(namespace, name));
    }

    @Override
    protected List<Future> creationFutures(K8SUtils k8s, ZookeeperCluster zk) {
        List<Future> result = new ArrayList<>(4);
        // start creating configMap operation only if metrics are enabled,
        // otherwise the future is already complete (for the "join")
        Future<Void> futureConfigMap = Future.future();
        if (zk.isMetricsEnabled()) {
            OperationExecutor.getInstance().executeK8s(CreateOperation.createConfigMap(zk.generateMetricsConfigMap()), futureConfigMap.completer());
        } else {
            futureConfigMap.complete();
        }
        result.add(futureConfigMap);

        Future<Void> futureService = Future.future();
        OperationExecutor.getInstance().executeFabric8(CreateOperation.createService(zk.generateService()), futureService.completer());
        result.add(futureService);

        Future<Void> futureHeadlessService = Future.future();
        OperationExecutor.getInstance().executeFabric8(CreateOperation.createService(zk.generateHeadlessService()), futureHeadlessService.completer());
        result.add(futureHeadlessService);

        Future<Void> futureStatefulSet = Future.future();
        OperationExecutor.getInstance().executeK8s(CreateOperation.createStatefulSet(zk.generateStatefulSet(k8s.isOpenShift())), futureStatefulSet.completer());
        result.add(futureStatefulSet);

        return result;
    }
}
