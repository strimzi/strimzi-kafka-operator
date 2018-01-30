package io.strimzi.controller.cluster.operations.resource;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ResourceOperation} that can be scaled up and down.
 * @param <C>
 * @param <T>
 * @param <L>
 * @param <D>
 * @param <R>
 */
public abstract class ScalableResourceOperation<C, T extends HasMetadata, L, D, R extends ScalableResource<T, D>>
        extends ResourceOperation<C, T, L, D, R> {

    private static final Logger log = LoggerFactory.getLogger(ScalableResourceOperation.class.getName());

    public ScalableResourceOperation(Vertx vertx, C client, String resourceKind) {
        super(vertx, client, resourceKind);
    }

    private R resource(String namespace, String name) {
        return operation().inNamespace(namespace).withName(name);
    }

    public void scaleUp(String namespace, String name, int scaleTo, Handler<AsyncResult<Void>> handler) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    try {
                        log.info("Scaling up to {} replicas", scaleTo);
                        resource(namespace, name).scale(scaleTo, true);
                        future.complete();
                    }
                    catch (Exception e) {
                        log.error("Caught exception while scaling up", e);
                        future.fail(e);
                    }
                },
                false,
                res -> {
                    if (res.succeeded()) {
                        log.info("Scaling up to {} replicas has been completed", scaleTo);
                        handler.handle(Future.succeededFuture());
                    }
                    else {
                        log.error("Scaling up has failed: {}", res.cause().toString());
                        handler.handle(Future.failedFuture(res.cause()));
                    }
                }
        );
    }

    public void scaleDown(String namespace, String name, int scaleTo, Handler<AsyncResult<Void>> handler) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    try {
                        Object gettable = resource(namespace, name).get();
                        int nextReplicas;

                        if (gettable instanceof StatefulSet) {
                            nextReplicas = ((StatefulSet) resource(namespace, name).get()).getSpec().getReplicas();
                        } else if (gettable instanceof Deployment) {
                            nextReplicas = ((Deployment) resource(namespace, name).get()).getSpec().getReplicas();
                        } else {
                            future.fail("Unknown resource type: " + gettable.getClass().getCanonicalName());
                            return;
                        }

                        while (nextReplicas > scaleTo) {
                            nextReplicas--;
                            log.info("Scaling down from {} to {}", nextReplicas+1, nextReplicas);
                            resource(namespace, name).scale(nextReplicas, true);
                        }

                        future.complete();
                    }
                    catch (Exception e) {
                        log.error("Caught exception while scaling down", e);
                        future.fail(e);
                    }
                },
                false,
                res -> {
                    if (res.succeeded()) {
                        log.info("Scaling down to {} replicas has been completed", scaleTo);
                        handler.handle(Future.succeededFuture());
                    }
                    else {
                        log.error("Scaling down has failed: {}", res.cause().toString());
                        handler.handle(Future.failedFuture(res.cause()));
                    }
                }
        );
    }
}
