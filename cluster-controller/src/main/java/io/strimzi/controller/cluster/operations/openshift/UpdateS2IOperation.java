package io.strimzi.controller.cluster.operations.openshift;

import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.controller.cluster.operations.kubernetes.PatchOperation;
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

    private final PatchOperation patchOperation;

    /**
     * Constructor
     *
     * @param s2i   Source2Image instance which should be updated
     */
    public UpdateS2IOperation(Vertx vertx, OpenShiftClient client, Source2Image s2i) {
        super(vertx, client,"update", s2i);
        patchOperation = new PatchOperation(vertx);
    }


    @Override
    protected List<Future> futures() {
        if (s2i.diff(client).getDifferent()) {
            List<Future> result = new ArrayList<>(3);
            Future<Void> futureSourceImageStream = Future.future();

            patchOperation.patch(
                    client.imageStreams().inNamespace(s2i.getNamespace()).withName(s2i.getSourceImageStreamName()),
                    s2i.patchSourceImageStream(
                            client.imageStreams().inNamespace(s2i.getNamespace()).withName(s2i.getSourceImageStreamName()).get()),
                    futureSourceImageStream.completer());
            result.add(futureSourceImageStream);

            Future<Void> futureTargetImageStream = Future.future();
            patchOperation
                    .patch(client.imageStreams().inNamespace(s2i.getNamespace()).withName(s2i.getName()),
                            s2i.patchTargetImageStream(
                                    client.imageStreams().inNamespace(s2i.getNamespace()).withName(s2i.getName()).get()),
                            futureTargetImageStream.completer());
            result.add(futureTargetImageStream);

            Future<Void> futureBuildConfig = Future.future();
            patchOperation
                    .patch(client.buildConfigs().inNamespace(s2i.getNamespace()).withName(s2i.getName()),
                            s2i.patchBuildConfig(
                                    client.buildConfigs().inNamespace(s2i.getNamespace()).withName(s2i.getName()).get()),
                            futureBuildConfig.completer());
            result.add(futureBuildConfig);

            return result;
        } else {
            log.info("No S2I {} differences found in namespace {}", s2i.getName(), s2i.getNamespace());
            return Collections.emptyList();
        }
    }
}
