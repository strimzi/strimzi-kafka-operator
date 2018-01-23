package io.strimzi.controller.cluster.operations.openshift;

import io.strimzi.controller.cluster.OpenShiftUtils;
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
 * Creates all Source2Image resources
 */
public class CreateS2IOperation extends S2IOperation {
    private static final Logger log = LoggerFactory.getLogger(CreateS2IOperation.class.getName());
    private final Source2Image s2i;

    /**
     * Constructor
     *
     * @param s2i   Source2Image resources which should be created
     */
    public CreateS2IOperation(Source2Image s2i) {
        super(s2i);
        this.s2i = s2i;
    }

    /**
     * Triggers the Soruce2Image resource (ImageStreams, BuildConfigs) creation
     *
     * @param vertx   Vert.x instance
     * @param os      OpenShiftUtils instance
     * @param handler Result handler
     */
    @Override
    public void execute(Vertx vertx, OpenShiftUtils os, Handler<AsyncResult<Void>> handler) {
        log.info("Creating S2I {} in namespace {}", s2i.getName(), s2i.getNamespace());

        Future<Void> futureSourceImageStream = Future.future();
        OperationExecutor.getInstance().execute(new CreateImageStreamOperation(s2i.generateSourceImageStream()), futureSourceImageStream.completer());

        Future<Void> futureTargetImageStream = Future.future();
        OperationExecutor.getInstance().execute(new CreateImageStreamOperation(s2i.generateTargetImageStream()), futureTargetImageStream.completer());

        Future<Void> futureBuildConfig = Future.future();
        OperationExecutor.getInstance().execute(new CreateBuildConfigOperation(s2i.generateBuildConfig()), futureBuildConfig.completer());

        CompositeFuture.join(futureSourceImageStream, futureTargetImageStream, futureBuildConfig).setHandler(ar -> {
            if (ar.succeeded()) {
                log.info("S2I {} successfully created in namespace {}", s2i.getName(), s2i.getNamespace());
                handler.handle(Future.succeededFuture());
            } else {
                log.error("S2I cluster {} failed to create in namespace {}", s2i.getName(), s2i.getNamespace());
                handler.handle(Future.failedFuture("Failed to create S2I"));
            }
        });
    }
}
