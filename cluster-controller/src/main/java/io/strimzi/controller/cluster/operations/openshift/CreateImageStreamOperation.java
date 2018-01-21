package io.strimzi.controller.cluster.operations.openshift;

import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.openshift.api.model.Image;
import io.fabric8.openshift.api.model.ImageStream;
import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.OpenShiftUtils;
import io.strimzi.controller.cluster.operations.kubernetes.K8sOperation;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateImageStreamOperation extends OpenShiftOperation {
    private static final Logger log = LoggerFactory.getLogger(CreateImageStreamOperation.class.getName());
    private final ImageStream imageStream;

    public CreateImageStreamOperation(ImageStream imageStream) {
        this.imageStream = imageStream;
    }

    @Override
    public void execute(Vertx vertx, OpenShiftUtils os, Handler<AsyncResult<Void>> handler) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    if (!os.exists(imageStream.getMetadata().getNamespace(), imageStream.getMetadata().getName(), ImageStream.class)) {
                        try {
                            log.info("Creating ImageStream {}", imageStream.getMetadata().getName());
                            os.create(imageStream);
                            future.complete();
                        } catch (Exception e) {
                            log.error("Caught exception while creating ImageStream", e);
                            future.fail(e);
                        }
                    } else {
                        log.warn("ImageStream {} already exists", imageStream.getMetadata().getName());
                        future.complete();
                    }

                },
                false,
                res -> {
                    if (res.succeeded()) {
                        log.info("ImageStream {} has been created", imageStream.getMetadata().getName());
                        handler.handle(Future.succeededFuture());
                    }
                    else {
                        log.error("ImageStream creation failed: {}", res.result());
                        handler.handle(Future.failedFuture((Exception)res.result()));
                    }
                }
        );
    }
}
