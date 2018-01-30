package io.strimzi.controller.cluster.operations.openshift;

import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.controller.cluster.operations.DeleteOperation;
import io.strimzi.controller.cluster.resources.Source2Image;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

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
    public DeleteS2IOperation(Vertx vertx, OpenShiftClient client, Source2Image s2i) {
        super(vertx, client, "delete", s2i);
    }

    @Override
    protected List<Future> futures(OpenShiftClient client) {
        List<Future> result = new ArrayList<>(3);

        Future<Void> futureSourceImageStream = Future.future();
        DeleteOperation.deleteImageStream(s2i.getNamespace(), s2i.getSourceImageStreamName()).execute(vertx, client, futureSourceImageStream.completer());
        result.add(futureSourceImageStream);

        Future<Void> futureTargetImageStream = Future.future();
        DeleteOperation.deleteImageStream(s2i.getNamespace(), s2i.getName()).execute(vertx, client, futureTargetImageStream.completer());
        result.add(futureTargetImageStream);

        Future<Void> futureBuildConfig = Future.future();
        DeleteOperation.deleteBuildConfig(s2i.getNamespace(), s2i.getName()).execute(vertx, client, futureBuildConfig.completer());
        result.add(futureBuildConfig);

        return result;
    }
}
