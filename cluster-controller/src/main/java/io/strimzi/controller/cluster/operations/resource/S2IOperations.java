package io.strimzi.controller.cluster.operations.resource;

import io.strimzi.controller.cluster.resources.Source2Image;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Base Source2Image operation
 */
public class S2IOperations {
    private static final Logger log = LoggerFactory.getLogger(S2IOperations.class.getName());

    protected final Vertx vertx;

    private final BuildConfigOperations buildConfigOperations;
    private final ImageStreamOperations imageStreamOperations;

    /**
     * Constructor
     *
     */
    public S2IOperations(Vertx vertx,
                            ImageStreamOperations imageStreamOperations,
                            BuildConfigOperations buildConfigOperations) {
        this.vertx = vertx;
        this.buildConfigOperations = buildConfigOperations;
        this.imageStreamOperations = imageStreamOperations;
    }

    abstract class S2IOperation {
        private final String operationType;

        S2IOperation(String operationType) {
            this.operationType = operationType;
        }
        public final void execute(Source2Image s2i, Handler<AsyncResult<Void>> handler) {
            log.info("{} S2I {} in namespace {}", operationType, s2i.getName(), s2i.getNamespace());

            try {
                List<Future> futures = futures(s2i);

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

        protected abstract List<Future> futures(Source2Image s2i);
    }

    private final S2IOperation createOp = new S2IOperation("create") {
        @Override
        protected List<Future> futures(Source2Image s2i) {
            List<Future> result = new ArrayList<>(3);

            Future<Void> futureSourceImageStream = Future.future();

            imageStreamOperations.create(s2i.generateSourceImageStream(), futureSourceImageStream.completer());
            result.add(futureSourceImageStream);

            Future<Void> futureTargetImageStream = Future.future();
            imageStreamOperations.create(s2i.generateTargetImageStream(), futureTargetImageStream.completer());
            result.add(futureTargetImageStream);

            Future<Void> futureBuildConfig = Future.future();
            buildConfigOperations.create(s2i.generateBuildConfig(), futureBuildConfig.completer());
            result.add(futureBuildConfig);

            return result;
        }
    };

    public void create(Source2Image s2i, Handler<AsyncResult<Void>> handler) {
        createOp.execute(s2i, handler);
    }

    private final S2IOperation updateOp = new S2IOperation("update") {
        @Override
        protected List<Future> futures(Source2Image s2i) {
            if (s2i.diff(imageStreamOperations, buildConfigOperations).getDifferent()) {
                List<Future> result = new ArrayList<>(3);
                Future<Void> futureSourceImageStream = Future.future();

                imageStreamOperations.patch(
                        s2i.getNamespace(), s2i.getSourceImageStreamName(),
                        s2i.patchSourceImageStream(
                                imageStreamOperations.get(s2i.getNamespace(), s2i.getSourceImageStreamName())),
                        futureSourceImageStream.completer());
                result.add(futureSourceImageStream);

                Future<Void> futureTargetImageStream = Future.future();
                imageStreamOperations
                        .patch(s2i.getNamespace(), s2i.getName(),
                                s2i.patchTargetImageStream(
                                        imageStreamOperations.get(s2i.getNamespace(), s2i.getName())),
                                futureTargetImageStream.completer());
                result.add(futureTargetImageStream);

                Future<Void> futureBuildConfig = Future.future();
                buildConfigOperations
                        .patch(s2i.getNamespace(), s2i.getName(),
                                s2i.patchBuildConfig(
                                        buildConfigOperations.get(s2i.getNamespace(), s2i.getName())),
                                futureBuildConfig.completer());
                result.add(futureBuildConfig);

                return result;
            } else {
                log.info("No S2I {} differences found in namespace {}", s2i.getName(), s2i.getNamespace());
                return Collections.emptyList();
            }
        }
    };

    public void update(Source2Image s2i, Handler<AsyncResult<Void>> handler) {
        updateOp.execute(s2i, handler);
    }

    private final S2IOperation deleteOp = new S2IOperation("delete") {
        @Override
        protected List<Future> futures(Source2Image s2i) {
            List<Future> result = new ArrayList<>(3);

            Future<Void> futureSourceImageStream = Future.future();

            imageStreamOperations.delete(s2i.getNamespace(), s2i.getSourceImageStreamName(), futureSourceImageStream.completer());
            result.add(futureSourceImageStream);

            Future<Void> futureTargetImageStream = Future.future();
            imageStreamOperations.delete(s2i.getNamespace(), s2i.getName(), futureTargetImageStream.completer());
            result.add(futureTargetImageStream);

            Future<Void> futureBuildConfig = Future.future();

            buildConfigOperations.delete(s2i.getNamespace(), s2i.getName(), futureBuildConfig.completer());
            result.add(futureBuildConfig);

            return result;
        }
    };

    public void delete(Source2Image s2i, Handler<AsyncResult<Void>> handler) {
        deleteOp.execute(s2i, handler);
    }

}
