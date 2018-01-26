package io.strimzi.controller.cluster.operations.openshift;

import io.strimzi.controller.cluster.OpenShiftUtils;
import io.strimzi.controller.cluster.operations.DeleteOperation;
import io.strimzi.controller.cluster.operations.OperationExecutor;
import io.strimzi.controller.cluster.resources.Source2Image;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deletes all Soruce2Image resources
 */
public class DeleteS2IOperation extends S2IOperation {
    private static final Logger log = LoggerFactory.getLogger(DeleteS2IOperation.class.getName());

    /**
     * Constructor
     *
     * @param s2i   Source2Image instance which should be deleted
     */
    public DeleteS2IOperation(Source2Image s2i) {
        super(s2i);
    }

    /**
     * Delete the Source2Image instance
     *
     * @param vertx   Vert.x instance
     * @param os      OpenShiftUtils instance
     * @param handler Result handler
     */
    @Override
    public void execute(Vertx vertx, OpenShiftUtils os, Handler<AsyncResult<Void>> handler) {
        log.info("Deleting S2I {} in namespace {}", s2i.getName(), s2i.getNamespace());

        Future<Void> futureSourceImageStream = Future.future();
        OperationExecutor.getInstance().executeOpenShift(DeleteOperation.deleteImageStream(s2i.getNamespace(), s2i.getSourceImageStreamName()), futureSourceImageStream.completer());

        Future<Void> futureTargetImageStream = Future.future();
        OperationExecutor.getInstance().executeOpenShift(DeleteOperation.deleteImageStream(s2i.getNamespace(), s2i.getName()), futureTargetImageStream.completer());

        Future<Void> futureBuildConfig = Future.future();
        OperationExecutor.getInstance().executeOpenShift(DeleteOperation.deleteBuildConfig(s2i.getNamespace(), s2i.getName()), futureBuildConfig.completer());

        CompositeFuture.join(futureSourceImageStream, futureTargetImageStream, futureBuildConfig).setHandler(ar -> {
            if (ar.succeeded()) {
                log.info("S2I {} successfully deleted in namespace {}", s2i.getName(), s2i.getNamespace());
                handler.handle(Future.succeededFuture());
            } else {
                log.error("S2I cluster {} failed to deleted in namespace {}", s2i.getName(), s2i.getNamespace());
                handler.handle(Future.failedFuture("Failed to delete S2I"));
            }
        });
    }
}
