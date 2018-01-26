package io.strimzi.controller.cluster.operations.openshift;

import io.strimzi.controller.cluster.OpenShiftUtils;
import io.strimzi.controller.cluster.operations.CreateOperation;
import io.strimzi.controller.cluster.operations.OperationExecutor;
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
 * Creates all Source2Image resources
 */
public class CreateS2IOperation extends S2IOperation {

    /**
     * Constructor
     *
     * @param s2i   Source2Image resources which should be created
     */
    public CreateS2IOperation(Source2Image s2i) {
        super("create", s2i);
    }

    @Override
    protected boolean guard(OpenShiftUtils os) {
        return true;
    }

    @Override
    protected List<Future> futures(OpenShiftUtils os) {
        List<Future> result = new ArrayList<>(3);

        Future<Void> futureSourceImageStream = Future.future();
        OperationExecutor.getInstance().executeOpenShift(CreateOperation.createImageStream(s2i.generateSourceImageStream()), futureSourceImageStream.completer());
        result.add(futureSourceImageStream);

        Future<Void> futureTargetImageStream = Future.future();
        OperationExecutor.getInstance().executeOpenShift(CreateOperation.createImageStream(s2i.generateTargetImageStream()), futureTargetImageStream.completer());
        result.add(futureTargetImageStream);

        Future<Void> futureBuildConfig = Future.future();
        OperationExecutor.getInstance().executeOpenShift(CreateOperation.createBuildConfig(s2i.generateBuildConfig()), futureBuildConfig.completer());
        result.add(futureBuildConfig);

        return result;
    }
}
