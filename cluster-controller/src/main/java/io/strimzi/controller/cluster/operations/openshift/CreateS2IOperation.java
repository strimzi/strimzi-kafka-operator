package io.strimzi.controller.cluster.operations.openshift;

import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.controller.cluster.operations.CreateOperation;
import io.strimzi.controller.cluster.resources.Source2Image;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

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
    public CreateS2IOperation(Vertx vertx, OpenShiftClient client, Source2Image s2i) {
        super(vertx, client, "create", s2i);
    }

    @Override
    protected List<Future> futures() {
        List<Future> result = new ArrayList<>(3);

        Future<Void> futureSourceImageStream = Future.future();
        CreateOperation.createImageStream(vertx, client).create(s2i.generateSourceImageStream(), futureSourceImageStream.completer());
        result.add(futureSourceImageStream);

        Future<Void> futureTargetImageStream = Future.future();
        CreateOperation.createImageStream(vertx, client).create(s2i.generateTargetImageStream(), futureTargetImageStream.completer());
        result.add(futureTargetImageStream);

        Future<Void> futureBuildConfig = Future.future();
        CreateOperation.createBuildConfig(vertx, client).create(s2i.generateBuildConfig(), futureBuildConfig.completer());
        result.add(futureBuildConfig);

        return result;
    }
}
