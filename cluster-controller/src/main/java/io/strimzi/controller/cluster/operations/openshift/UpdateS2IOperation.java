package io.strimzi.controller.cluster.operations.openshift;

import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.ImageStream;
import io.strimzi.controller.cluster.OpenShiftUtils;
import io.strimzi.controller.cluster.operations.OperationExecutor;
import io.strimzi.controller.cluster.operations.kubernetes.PatchOperation;
import io.strimzi.controller.cluster.resources.Source2Image;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Updates all Source2Image resources
 */
public class UpdateS2IOperation extends S2IOperation {
    private static final Logger log = LoggerFactory.getLogger(UpdateS2IOperation.class.getName());

    /**
     * Constructor
     *
     * @param s2i   Source2Image instance which should be updated
     */
    public UpdateS2IOperation(Source2Image s2i) {
        super(s2i);
    }

    /**
     * Updates Source2Image instance
     *
     * @param vertx   Vert.x instance
     * @param os      OpenShiftUtils instance
     * @param handler Result handler
     */
    @Override
    public void execute(Vertx vertx, OpenShiftUtils os, Handler<AsyncResult<Void>> handler) {
        log.info("Updating S2I {} in namespace {}", s2i.getName(), s2i.getNamespace());

        try {
            if (s2i.diff(os).getDifferent()) {
                Future<Void> futureSourceImageStream = Future.future();
                OperationExecutor.getInstance().executeOpenShift(new PatchOperation(os.getResource(s2i.getNamespace(), s2i.getSourceImageStreamName(), ImageStream.class), s2i.patchSourceImageStream((ImageStream) os.get(s2i.getNamespace(), s2i.getSourceImageStreamName(), ImageStream.class))), futureSourceImageStream.completer());

                Future<Void> futureTargetImageStream = Future.future();
                OperationExecutor.getInstance().executeOpenShift(new PatchOperation(os.getResource(s2i.getNamespace(), s2i.getName(), ImageStream.class), s2i.patchTargetImageStream((ImageStream) os.get(s2i.getNamespace(), s2i.getName(), ImageStream.class))), futureTargetImageStream.completer());

                Future<Void> futureBuildConfig = Future.future();
                OperationExecutor.getInstance().executeOpenShift(new PatchOperation(os.getResource(s2i.getNamespace(), s2i.getName(), BuildConfig.class), s2i.patchBuildConfig((BuildConfig) os.get(s2i.getNamespace(), s2i.getName(), BuildConfig.class))), futureBuildConfig.completer());

                CompositeFuture.join(futureSourceImageStream, futureTargetImageStream, futureBuildConfig).setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.info("S2I {} successfully updated in namespace {}", s2i.getName(), s2i.getNamespace());
                        handler.handle(Future.succeededFuture());
                    } else {
                        log.error("S2I cluster {} failed to update in namespace {}", s2i.getName(), s2i.getNamespace());
                        handler.handle(Future.failedFuture("Failed to update S2I"));
                    }
                });
            } else {
                log.info("No S2I {} differences found in namespace {}", s2i.getName(), s2i.getNamespace());
                handler.handle(Future.succeededFuture());
            }
        }
        catch (Exception e) {
            log.error("S2I cluster {} failed to update in namespace {}", s2i.getName(), s2i.getNamespace(), e);
            handler.handle(Future.failedFuture("Failed to update S2I"));
        }

    }
}
