package io.strimzi.controller.cluster.operations.openshift;

import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.controller.cluster.operations.resource.BuildConfigResources;
import io.strimzi.controller.cluster.operations.resource.ImageStreamResources;
import io.strimzi.controller.cluster.resources.Source2Image;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.ArrayList;
import java.util.List;

/**
 * Creates all Source2Image resources
 */
public class CreateS2IOperation extends S2IOperation {

    private final ImageStreamResources imageStreamResources;
    private final BuildConfigResources buildConfigResources;

    /**
     * Constructor
     *
     */
    public CreateS2IOperation(Vertx vertx, OpenShiftClient client) {
        super(vertx, "create");
        imageStreamResources = new ImageStreamResources(vertx, client);
        buildConfigResources = new BuildConfigResources(vertx, client);
    }

    @Override
    protected List<Future> futures(Source2Image s2i) {
        List<Future> result = new ArrayList<>(3);

        Future<Void> futureSourceImageStream = Future.future();

        imageStreamResources.create(s2i.generateSourceImageStream(), futureSourceImageStream.completer());
        result.add(futureSourceImageStream);

        Future<Void> futureTargetImageStream = Future.future();
        imageStreamResources.create(s2i.generateTargetImageStream(), futureTargetImageStream.completer());
        result.add(futureTargetImageStream);

        Future<Void> futureBuildConfig = Future.future();
        buildConfigResources.create(s2i.generateBuildConfig(), futureBuildConfig.completer());
        result.add(futureBuildConfig);

        return result;
    }
}
