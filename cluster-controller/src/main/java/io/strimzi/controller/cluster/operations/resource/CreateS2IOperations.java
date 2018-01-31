package io.strimzi.controller.cluster.operations.resource;

import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.controller.cluster.resources.Source2Image;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.ArrayList;
import java.util.List;

/**
 * Creates all Source2Image resources
 */
public class CreateS2IOperations extends S2IOperations {

    private final ImageStreamOperations imageStreamOperations;
    private final BuildConfigOperations buildConfigOperations;

    /**
     * Constructor
     *
     */
    public CreateS2IOperations(Vertx vertx, OpenShiftClient client) {
        super(vertx, "create");
        imageStreamOperations = new ImageStreamOperations(vertx, client);
        buildConfigOperations = new BuildConfigOperations(vertx, client);
    }

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
}
