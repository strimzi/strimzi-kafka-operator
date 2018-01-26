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

import java.util.ArrayList;
import java.util.List;

/**
 * Updates all Source2Image resources
 */
public class UpdateS2IOperation extends S2IOperation {

    /**
     * Constructor
     *
     * @param s2i   Source2Image instance which should be updated
     */
    public UpdateS2IOperation(Source2Image s2i) {
        super("update", s2i);
    }

    @Override
    protected boolean guard(OpenShiftUtils os) {
        return s2i.diff(os).getDifferent();
    }

    @Override
    protected List<Future> futures(OpenShiftUtils os) {
        List<Future> result = new ArrayList<>(3);
        Future<Void> futureSourceImageStream = Future.future();
        OperationExecutor.getInstance().executeOpenShift(new PatchOperation(os.getResource(s2i.getNamespace(), s2i.getSourceImageStreamName(), ImageStream.class), s2i.patchSourceImageStream((ImageStream) os.get(s2i.getNamespace(), s2i.getSourceImageStreamName(), ImageStream.class))), futureSourceImageStream.completer());
        result.add(futureSourceImageStream);

        Future<Void> futureTargetImageStream = Future.future();
        OperationExecutor.getInstance().executeOpenShift(new PatchOperation(os.getResource(s2i.getNamespace(), s2i.getName(), ImageStream.class), s2i.patchTargetImageStream((ImageStream) os.get(s2i.getNamespace(), s2i.getName(), ImageStream.class))), futureTargetImageStream.completer());
        result.add(futureTargetImageStream);

        Future<Void> futureBuildConfig = Future.future();
        OperationExecutor.getInstance().executeOpenShift(new PatchOperation(os.getResource(s2i.getNamespace(), s2i.getName(), BuildConfig.class), s2i.patchBuildConfig((BuildConfig) os.get(s2i.getNamespace(), s2i.getName(), BuildConfig.class))), futureBuildConfig.completer());
        result.add(futureBuildConfig);

        return result;
    }
}
