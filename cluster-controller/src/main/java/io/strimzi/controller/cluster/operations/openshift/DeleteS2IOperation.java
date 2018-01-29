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

import java.util.ArrayList;
import java.util.List;

/**
 * Deletes all Soruce2Image resources
 */
public class DeleteS2IOperation extends S2IOperation {

    /**
     * Constructor
     *
     * @param s2i   Source2Image instance which should be deleted
     */
    public DeleteS2IOperation(Source2Image s2i) {
        super("delete", s2i);
    }

    @Override
    protected List<Future> futures(OpenShiftUtils os) {
        List<Future> result = new ArrayList<>(3);

        Future<Void> futureSourceImageStream = Future.future();
        OperationExecutor.getInstance().executeOpenShiftClient(DeleteOperation.deleteImageStream(s2i.getNamespace(), s2i.getSourceImageStreamName()), futureSourceImageStream.completer());
        result.add(futureSourceImageStream);

        Future<Void> futureTargetImageStream = Future.future();
        OperationExecutor.getInstance().executeOpenShiftClient(DeleteOperation.deleteImageStream(s2i.getNamespace(), s2i.getName()), futureTargetImageStream.completer());
        result.add(futureTargetImageStream);

        Future<Void> futureBuildConfig = Future.future();
        OperationExecutor.getInstance().executeOpenShiftClient(DeleteOperation.deleteBuildConfig(s2i.getNamespace(), s2i.getName()), futureBuildConfig.completer());
        result.add(futureBuildConfig);

        return result;
    }
}
