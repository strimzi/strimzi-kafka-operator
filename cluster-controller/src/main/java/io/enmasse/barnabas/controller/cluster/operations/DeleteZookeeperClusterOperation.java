package io.enmasse.barnabas.controller.cluster.operations;

import io.enmasse.barnabas.controller.cluster.K8SUtils;
import io.enmasse.barnabas.controller.cluster.resources.ZookeeperResource;
import io.vertx.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.shareddata.Lock;

public class DeleteZookeeperClusterOperation extends ZookeeperClusterOperation {
    private static final Logger log = LoggerFactory.getLogger(DeleteZookeeperClusterOperation.class.getName());

    public DeleteZookeeperClusterOperation(Vertx vertx, K8SUtils k8s, String namespace, String name) {
        super(vertx, k8s, namespace, name);
    }

    @Override
    public void execute(Handler<AsyncResult<Void>> handler) {
        vertx.sharedData().getLockWithTimeout(getLockName(), LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                Lock lock = res.result();

                log.info("Deleting Zookeeper cluster {} from namespace {}", name + "-zookeeper", namespace);

                Future futureService = Future.future();
                new DeleteServiceOperation(vertx, k8s, namespace, name + "-zookeeper").execute(futureService.completer());

                Future futureHeadlessService = Future.future();
                new DeleteServiceOperation(vertx, k8s, namespace, name + "-zookeeper").execute(futureHeadlessService.completer());

                Future futureStatefulSet = Future.future();
                new DeleteStatefulSetOperation(vertx, k8s, namespace, name + "-zookeeper").execute(futureStatefulSet.completer());

                CompositeFuture.join(futureService, futureHeadlessService, futureStatefulSet).setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.info("Zookeeper cluster {} successfully deleted from namespace {}", name + "-zookeeper", namespace);
                        handler.handle(Future.succeededFuture());
                    } else {
                        log.error("Zookeeper cluster {} failed to delete from namespace {}", name + "-zookeeper", namespace);
                        handler.handle(Future.failedFuture("Failed to delete Zookeeper cluster"));
                    }
                });

                lock.release();
            } else {
                log.error("Failed to acquire lock to delete Zookeeper cluster {}", getLockName());
                handler.handle(Future.failedFuture("Failed to acquire lock to delete Zookeeper cluster"));
            }
        });
    }
}
