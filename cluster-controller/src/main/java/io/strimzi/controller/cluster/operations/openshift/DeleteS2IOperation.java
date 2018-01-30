package io.strimzi.controller.cluster.operations.openshift;

import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.api.model.Build;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.BuildConfigList;
import io.fabric8.openshift.api.model.DoneableBuildConfig;
import io.fabric8.openshift.api.model.DoneableImageStream;
import io.fabric8.openshift.api.model.ImageStream;
import io.fabric8.openshift.api.model.ImageStreamList;
import io.fabric8.openshift.client.OpenShiftClient;
import io.fabric8.openshift.client.dsl.BuildConfigResource;
import io.strimzi.controller.cluster.operations.ResourceOperation;
import io.strimzi.controller.cluster.resources.Source2Image;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.ArrayList;
import java.util.List;

/**
 * Deletes all Soruce2Image resources
 */
public class DeleteS2IOperation extends S2IOperation {

    private final ResourceOperation<OpenShiftClient, ImageStream, ImageStreamList, DoneableImageStream, Resource<ImageStream, DoneableImageStream>> imageStreamResources;
    private final ResourceOperation<OpenShiftClient, BuildConfig, BuildConfigList, DoneableBuildConfig, BuildConfigResource<BuildConfig, DoneableBuildConfig, Void, Build>> buildConfigResources;

    /**
     * Constructor
     */
    public DeleteS2IOperation(Vertx vertx, OpenShiftClient client) {
        super(vertx, "delete");
        imageStreamResources = ResourceOperation.imageStream(vertx, client);
        buildConfigResources = ResourceOperation.buildConfig(vertx, client);
    }

    @Override
    protected List<Future> futures(Source2Image s2i) {
        List<Future> result = new ArrayList<>(3);

        Future<Void> futureSourceImageStream = Future.future();

        imageStreamResources.delete(s2i.getNamespace(), s2i.getSourceImageStreamName(), futureSourceImageStream.completer());
        result.add(futureSourceImageStream);

        Future<Void> futureTargetImageStream = Future.future();
        imageStreamResources.delete(s2i.getNamespace(), s2i.getName(), futureTargetImageStream.completer());
        result.add(futureTargetImageStream);

        Future<Void> futureBuildConfig = Future.future();

        buildConfigResources.delete(s2i.getNamespace(), s2i.getName(), futureBuildConfig.completer());
        result.add(futureBuildConfig);

        return result;
    }
}
