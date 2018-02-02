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
     * @param vertx The Vertx instance
     * @param imageStreamOperations For accessing ImageStreams
     * @param buildConfigOperations For accessing BuildConfigs
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
        public final Future<Void> execute(Source2Image s2i) {
            Future<Void> fut = Future.future();
            log.info("{} S2I {} in namespace {}", operationType, s2i.getName(), s2i.getNamespace());

            try {
                Future<?> composite = composite(s2i);

                composite.setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.info("S2I {} successfully updated in namespace {}", s2i.getName(), s2i.getNamespace());
                        fut.complete();
                    } else {
                        log.error("S2I cluster {} failed to update in namespace {}", s2i.getName(), s2i.getNamespace());
                        fut.fail("Failed to update S2I");
                    }
                });
            }
            catch (Exception e) {
                log.error("S2I cluster {} failed to update in namespace {}", s2i.getName(), s2i.getNamespace(), e);
                fut.fail("Failed to update S2I");
            }
            return fut;
        }

        protected abstract Future<?> composite(Source2Image s2i);
    }

    private final S2IOperation createOp = new S2IOperation("create") {
        @Override
        protected Future<?> composite(Source2Image s2i) {
            List<Future> result = new ArrayList<>(3);

            result.add(imageStreamOperations.create(s2i.generateSourceImageStream()));

            result.add(imageStreamOperations.create(s2i.generateTargetImageStream()));

            result.add(buildConfigOperations.create(s2i.generateBuildConfig()));

            return CompositeFuture.join(result);
        }
    };

    public Future<Void> create(Source2Image s2i) {
        return createOp.execute(s2i);
    }

    private final S2IOperation updateOp = new S2IOperation("update") {
        @Override
        protected Future<?> composite(Source2Image s2i) {
            if (s2i.diff(imageStreamOperations, buildConfigOperations).getDifferent()) {
                List<Future> result = new ArrayList<>(3);
                result.add(imageStreamOperations.patch(
                        s2i.getNamespace(), s2i.getSourceImageStreamName(),
                        s2i.patchSourceImageStream(
                                imageStreamOperations.get(s2i.getNamespace(), s2i.getSourceImageStreamName()))));

                result.add(imageStreamOperations
                        .patch(s2i.getNamespace(), s2i.getName(),
                                s2i.patchTargetImageStream(
                                        imageStreamOperations.get(s2i.getNamespace(), s2i.getName()))));

                result.add(buildConfigOperations
                        .patch(s2i.getNamespace(), s2i.getName(),
                                s2i.patchBuildConfig(
                                        buildConfigOperations.get(s2i.getNamespace(), s2i.getName()))));

                return CompositeFuture.join(result);
            } else {
                log.info("No S2I {} differences found in namespace {}", s2i.getName(), s2i.getNamespace());
                return Future.succeededFuture();
            }
        }
    };

    public Future<Void> update(Source2Image s2i) {
        return updateOp.execute(s2i);
    }

    private final S2IOperation deleteOp = new S2IOperation("delete") {
        @Override
        protected Future<?> composite(Source2Image s2i) {
            List<Future> result = new ArrayList<>(3);

            result.add(imageStreamOperations.delete(s2i.getNamespace(), s2i.getSourceImageStreamName()));

            result.add(imageStreamOperations.delete(s2i.getNamespace(), s2i.getName()));

            result.add(buildConfigOperations.delete(s2i.getNamespace(), s2i.getName()));

            return CompositeFuture.join(result);
        }
    };

    public Future<Void> delete(Source2Image s2i) {
        return deleteOp.execute(s2i);
    }

}
