package io.enmasse.barnabas.controller.cluster.operations;

import io.enmasse.barnabas.controller.cluster.K8SUtils;
import io.enmasse.barnabas.controller.cluster.operations.kubernetes.CreateConfigMapOperation;
import io.enmasse.barnabas.controller.cluster.operations.kubernetes.CreateServiceOperation;
import io.enmasse.barnabas.controller.cluster.operations.kubernetes.CreateStatefulSetOperation;
import io.enmasse.barnabas.controller.cluster.resources.ZookeeperCluster;
import io.vertx.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.shareddata.Lock;

public class CreateZookeeperClusterOperation extends ZookeeperClusterOperation {
    private static final Logger log = LoggerFactory.getLogger(CreateZookeeperClusterOperation.class.getName());

    public CreateZookeeperClusterOperation(String namespace, String name) {
        super(namespace, name);
    }

    @Override
    public void execute(Vertx vertx, K8SUtils k8s, Handler<AsyncResult<Void>> handler) {
        vertx.sharedData().getLockWithTimeout(getLockName(), LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                Lock lock = res.result();

                log.info("Creating Zookeeper cluster {} in namespace {}", name + "-zookeeper", namespace);

                ZookeeperCluster zk = ZookeeperCluster.fromConfigMap(k8s.getConfigmap(namespace, name));

                // start creating configMap operation only if metrics are enabled,
                // otherwise the future is already complete (for the "join")
                Future<Void> futureConfigMap = Future.future();
                if (zk.isMetricsEnabled()) {
                    OperationExecutor.getInstance().execute(new CreateConfigMapOperation(zk.generateMetricsConfigMap()), futureConfigMap.completer());
                } else {
                    futureConfigMap.complete();
                }

                Future<Void> futureService = Future.future();
                OperationExecutor.getInstance().execute(new CreateServiceOperation(zk.generateService()), futureService.completer());

                Future<Void> futureHeadlessService = Future.future();
                OperationExecutor.getInstance().execute(new CreateServiceOperation(zk.generateHeadlessService()), futureHeadlessService.completer());

                Future<Void> futureStatefulSet = Future.future();
                OperationExecutor.getInstance().execute(new CreateStatefulSetOperation(zk.generateStatefulSet(k8s.isOpenShift())), futureStatefulSet.completer());

                CompositeFuture.join(futureConfigMap, futureService, futureHeadlessService, futureStatefulSet).setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.info("Zookeeper cluster {} successfully created in namespace {}", name + "-zookeeper", namespace);
                        handler.handle(Future.succeededFuture());
                        lock.release();
                    } else {
                        log.error("Zookeeper cluster {} failed to create in namespace {}", name + "-zookeeper", namespace);
                        handler.handle(Future.failedFuture("Failed to create Zookeeper cluster"));
                        lock.release();
                    }
                });
            } else {
                log.error("Failed to acquire lock to create Zookeeper cluster {}", getLockName());
                handler.handle(Future.failedFuture("Failed to acquire lock to create Zookeeper cluster"));
            }
        });
    }
}
