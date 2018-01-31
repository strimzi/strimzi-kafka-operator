package io.strimzi.controller.cluster.operations.openshift;

import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.controller.cluster.operations.resource.BuildConfigOperations;
import io.strimzi.controller.cluster.operations.resource.ImageStreamOperations;
import io.strimzi.controller.cluster.resources.Source2Image;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.ArrayList;
import java.util.List;

/**
 * Deletes all Soruce2Image resources
 */
public class DeleteS2IOperation extends S2IOperation {

    private final ImageStreamOperations imageStreamOperations;
    private final BuildConfigOperations buildConfigOperations;

    /**
     * Constructor
     */
    public DeleteS2IOperation(Vertx vertx, OpenShiftClient client) {
        super(vertx, "delete");
        imageStreamOperations = new ImageStreamOperations(vertx, client);
        buildConfigOperations = new BuildConfigOperations(vertx, client);
    }

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
}
