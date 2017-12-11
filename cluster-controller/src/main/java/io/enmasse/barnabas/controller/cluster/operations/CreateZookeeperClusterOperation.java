package io.enmasse.barnabas.controller.cluster.operations;

import io.enmasse.barnabas.controller.cluster.K8SUtils;
import io.enmasse.barnabas.controller.cluster.resources.ZookeeperResource;
import io.vertx.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.vertx.core.shareddata.Lock;

public class CreateZookeeperClusterOperation extends ZookeeperClusterOperation {
    private static final Logger log = LoggerFactory.getLogger(CreateZookeeperClusterOperation.class.getName());

    public CreateZookeeperClusterOperation(Vertx vertx, K8SUtils k8s, String namespace, String name) {
        super(vertx, k8s, namespace, name);
        log.info("Created Zookeeper cluster operation ... name: {}, namespace: {}", name + "-zookeeper", namespace);
    }

    @Override
    public void execute(Handler<AsyncResult<Void>> handler) {
        vertx.sharedData().getLockWithTimeout(getLockName(), LOCK_TIMEOUT, res -> {
            if (res.succeeded()) {
                Lock lock = res.result();

                log.info("Creating Zookeeper cluster {} in namespace {}", name + "-zookeeper", namespace);

                ZookeeperResource zk = ZookeeperResource.fromConfigMap(k8s.getConfigmap(namespace, name), vertx, k8s);

                Future futureService = Future.future();
                new CreateServiceOperation(vertx, k8s, zk.generateService()).execute(futureService.completer());

                Future futureHeadlessService = Future.future();
                new CreateServiceOperation(vertx, k8s, zk.generateHeadlessService()).execute(futureHeadlessService.completer());

                Future futureStatefulSet = Future.future();
                new CreateStatefulSetOperation(vertx, k8s, zk.generateStatefulSet()).execute(futureStatefulSet.completer());

                CompositeFuture.join(futureService, futureHeadlessService, futureStatefulSet).setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.info("Zookeeper cluster {} successfully created in namespace {}", name + "-zookeeper", namespace);
                        handler.handle(Future.succeededFuture());
                    } else {
                        log.error("Zookeeper cluster {} failed to create in namespace {}", name + "-zookeeper", namespace);
                        handler.handle(Future.failedFuture("Failed to create Zookeeper cluster"));
                    }
                });

                lock.release();
            } else {
                log.error("Failed to acquire lock to create Zookeeper cluster {}", getLockName());
                handler.handle(Future.failedFuture("Failed to acquire lock to create Zookeeper cluster"));
            }
        });
    }
}
