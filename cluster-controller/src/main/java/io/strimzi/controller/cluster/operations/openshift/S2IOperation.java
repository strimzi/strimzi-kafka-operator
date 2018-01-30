package io.strimzi.controller.cluster.operations.openshift;

import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.controller.cluster.OpenShiftUtils;
import io.strimzi.controller.cluster.operations.Operation;
import io.strimzi.controller.cluster.resources.Source2Image;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Base Source2Image operation
 */
public abstract class S2IOperation<U> implements Operation<U> {
    private static final Logger log = LoggerFactory.getLogger(S2IOperation.class.getName());

    protected final Source2Image s2i;
    private final String operationType;
    private final OpenShiftClient client;
    protected final Vertx vertx;

    /**
     * Constructor
     *
     * @param s2i   Source2Image instance
     */
    protected S2IOperation(Vertx vertx, OpenShiftClient client, String operationType, Source2Image s2i) {
        this.vertx = vertx;
        this.client = client;
        this.operationType = operationType;
        this.s2i = s2i;
    }

    @Override
    public final void execute(Vertx vertx2, U os, Handler<AsyncResult<Void>> handler) {
        log.info("{} S2I {} in namespace {}", operationType, s2i.getName(), s2i.getNamespace());

        try {
            List<Future> futures = futures(client);

            CompositeFuture.join(futures).setHandler(ar -> {
                if (ar.succeeded()) {
                    log.info("S2I {} successfully updated in namespace {}", s2i.getName(), s2i.getNamespace());
                    handler.handle(Future.succeededFuture());
                } else {
                    log.error("S2I cluster {} failed to update in namespace {}", s2i.getName(), s2i.getNamespace());
                    handler.handle(Future.failedFuture("Failed to update S2I"));
                }
            });
        }
        catch (Exception e) {
            log.error("S2I cluster {} failed to update in namespace {}", s2i.getName(), s2i.getNamespace(), e);
            handler.handle(Future.failedFuture("Failed to update S2I"));
        }
    }

    protected abstract List<Future> futures(OpenShiftClient client);
}
