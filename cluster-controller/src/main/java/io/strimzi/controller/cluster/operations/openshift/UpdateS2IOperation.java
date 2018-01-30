package io.strimzi.controller.cluster.operations.openshift;

import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.controller.cluster.operations.resource.BuildConfigResources;
import io.strimzi.controller.cluster.operations.resource.ImageStreamResources;
import io.strimzi.controller.cluster.resources.Source2Image;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Updates all Source2Image resources
 */
public class UpdateS2IOperation extends S2IOperation {

    private static final Logger log = LoggerFactory.getLogger(UpdateS2IOperation.class.getName());
    private final BuildConfigResources buildConfigResources;
    private final ImageStreamResources buildImageStreamResources;

    /**
     * Constructor
     */
    public UpdateS2IOperation(Vertx vertx, OpenShiftClient client) {
        super(vertx, "update");
        buildConfigResources = new BuildConfigResources(vertx, client);
        buildImageStreamResources = new ImageStreamResources(vertx, client);
    }


    @Override
    protected List<Future> futures(Source2Image s2i) {
        if (s2i.diff(buildImageStreamResources, buildConfigResources).getDifferent()) {
            List<Future> result = new ArrayList<>(3);
            Future<Void> futureSourceImageStream = Future.future();

            buildImageStreamResources.patch(
                    s2i.getNamespace(), s2i.getSourceImageStreamName(),
                    s2i.patchSourceImageStream(
                            buildImageStreamResources.get(s2i.getNamespace(), s2i.getSourceImageStreamName())),
                    futureSourceImageStream.completer());
            result.add(futureSourceImageStream);

            Future<Void> futureTargetImageStream = Future.future();
            buildImageStreamResources
                    .patch(s2i.getNamespace(), s2i.getName(),
                            s2i.patchTargetImageStream(
                                    buildImageStreamResources.get(s2i.getNamespace(), s2i.getName())),
                            futureTargetImageStream.completer());
            result.add(futureTargetImageStream);

            Future<Void> futureBuildConfig = Future.future();
            buildConfigResources
                    .patch(s2i.getNamespace(), s2i.getName(),
                            s2i.patchBuildConfig(
                                    buildConfigResources.get(s2i.getNamespace(), s2i.getName())),
                            futureBuildConfig.completer());
            result.add(futureBuildConfig);

            return result;
        } else {
            log.info("No S2I {} differences found in namespace {}", s2i.getName(), s2i.getNamespace());
            return Collections.emptyList();
        }
    }
}
