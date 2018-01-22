package io.strimzi.controller.cluster.operations.openshift;

import io.fabric8.openshift.api.model.BuildConfig;
import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.OpenShiftUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates BuildConfig resource
 */
public class CreateBuildConfigOperation extends OpenShiftOperation {
    private static final Logger log = LoggerFactory.getLogger(CreateBuildConfigOperation.class.getName());
    private final BuildConfig build;

    /**
     * Constructor
     *
     * @param build BuildConfig which should be created
     */
    public CreateBuildConfigOperation(BuildConfig build) {
        this.build = build;
    }

    /**
     * @param vertx   Vert.x instance
     * @param os      OpenShiftUtils instance
     * @param handler Result handler
     */
    @Override
    public void execute(Vertx vertx, OpenShiftUtils os, Handler<AsyncResult<Void>> handler) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    try {
                        if (!os.exists(build.getMetadata().getNamespace(), build.getMetadata().getName(), BuildConfig.class)) {
                                log.info("Creating BuildConfig {}", build.getMetadata().getName());
                                os.create(build);
                                future.complete();
                        }
                        else {
                            log.warn("BuildConfig {} already exists", build.getMetadata().getName());
                            future.complete();
                        }
                    } catch (Exception e) {
                        log.error("Caught exception while creating BuildConfig", e);
                        future.fail(e);
                    }
                },
                false,
                res -> {
                    if (res.succeeded()) {
                        log.info("BuildConfig {} has been created", build.getMetadata().getName());
                        handler.handle(Future.succeededFuture());
                    }
                    else {
                        log.error("BuildConfig creation failed: {}", res.result());
                        handler.handle(Future.failedFuture((Exception)res.result()));
                    }
                }
        );
    }
}
