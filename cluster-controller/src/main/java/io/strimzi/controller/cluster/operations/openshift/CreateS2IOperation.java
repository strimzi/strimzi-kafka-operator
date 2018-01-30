package io.strimzi.controller.cluster.operations.openshift;

import io.fabric8.kubernetes.client.KubernetesClient;
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
 * Creates all Source2Image resources
 */
public class CreateS2IOperation extends S2IOperation {

    private final ResourceOperation<OpenShiftClient, ImageStream, ImageStreamList, DoneableImageStream, Resource<ImageStream, DoneableImageStream>> imageStreamResources;
    private final ResourceOperation<OpenShiftClient, BuildConfig, BuildConfigList, DoneableBuildConfig, BuildConfigResource<BuildConfig, DoneableBuildConfig, Void, Build>> buildConfigResources;

    /**
     * Constructor
     *
     */
    public CreateS2IOperation(Vertx vertx, OpenShiftClient client) {
        super(vertx, "create");
        imageStreamResources = ResourceOperation.imageStream(vertx, client);
        buildConfigResources = ResourceOperation.buildConfig(vertx, client);
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
