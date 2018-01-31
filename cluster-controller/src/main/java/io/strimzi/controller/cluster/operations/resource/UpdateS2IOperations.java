package io.strimzi.controller.cluster.operations.resource;

import io.fabric8.openshift.client.OpenShiftClient;
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
public class UpdateS2IOperations extends S2IOperations {

    private static final Logger log = LoggerFactory.getLogger(UpdateS2IOperations.class.getName());
    private final BuildConfigOperations buildConfigOperations;
    private final ImageStreamOperations buildImageStreamOperations;

    /**
     * Constructor
     */
    public UpdateS2IOperations(Vertx vertx, OpenShiftClient client) {
        super(vertx, "update");
        buildConfigOperations = new BuildConfigOperations(vertx, client);
        buildImageStreamOperations = new ImageStreamOperations(vertx, client);
    }


    @Override
    protected List<Future> futures(Source2Image s2i) {
        if (s2i.diff(buildImageStreamOperations, buildConfigOperations).getDifferent()) {
            List<Future> result = new ArrayList<>(3);
            Future<Void> futureSourceImageStream = Future.future();

            buildImageStreamOperations.patch(
                    s2i.getNamespace(), s2i.getSourceImageStreamName(),
                    s2i.patchSourceImageStream(
                            buildImageStreamOperations.get(s2i.getNamespace(), s2i.getSourceImageStreamName())),
                    futureSourceImageStream.completer());
            result.add(futureSourceImageStream);

            Future<Void> futureTargetImageStream = Future.future();
            buildImageStreamOperations
                    .patch(s2i.getNamespace(), s2i.getName(),
                            s2i.patchTargetImageStream(
                                    buildImageStreamOperations.get(s2i.getNamespace(), s2i.getName())),
                            futureTargetImageStream.completer());
            result.add(futureTargetImageStream);

            Future<Void> futureBuildConfig = Future.future();
            buildConfigOperations
                    .patch(s2i.getNamespace(), s2i.getName(),
                            s2i.patchBuildConfig(
                                    buildConfigOperations.get(s2i.getNamespace(), s2i.getName())),
                            futureBuildConfig.completer());
            result.add(futureBuildConfig);

            return result;
        } else {
            log.info("No S2I {} differences found in namespace {}", s2i.getName(), s2i.getNamespace());
            return Collections.emptyList();
        }
    }
}
