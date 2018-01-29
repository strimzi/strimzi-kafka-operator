package io.strimzi.controller.cluster.operations.openshift;

import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.ImageStream;
import io.strimzi.controller.cluster.OpenShiftUtils;
import io.strimzi.controller.cluster.operations.OperationExecutor;
import io.strimzi.controller.cluster.operations.kubernetes.PatchOperation;
import io.strimzi.controller.cluster.resources.Source2Image;
import io.vertx.core.Future;
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

    /**
     * Constructor
     *
     * @param s2i   Source2Image instance which should be updated
     */
    public UpdateS2IOperation(Source2Image s2i) {
        super("update", s2i);
    }


    @Override
    protected List<Future> futures(OpenShiftUtils os) {
        if (s2i.diff(os).getDifferent()) {
            List<Future> result = new ArrayList<>(3);
            Future<Void> futureSourceImageStream = Future.future();
            OperationExecutor.getInstance().executeOpenShift(new PatchOperation(os.getResource(s2i.getNamespace(), s2i.getSourceImageStreamName(), ImageStream.class), s2i.patchSourceImageStream((ImageStream) os.get(s2i.getNamespace(), s2i.getSourceImageStreamName(), ImageStream.class))), futureSourceImageStream.completer());
            result.add(futureSourceImageStream);

            Future<Void> futureTargetImageStream = Future.future();
            OperationExecutor.getInstance().executeOpenShift(new PatchOperation(os.getResource(s2i.getNamespace(), s2i.getName(), ImageStream.class), s2i.patchTargetImageStream((ImageStream) os.get(s2i.getNamespace(), s2i.getName(), ImageStream.class))), futureTargetImageStream.completer());
            result.add(futureTargetImageStream);

            Future<Void> futureBuildConfig = Future.future();
            OperationExecutor.getInstance().executeOpenShift(new PatchOperation(os.getResource(s2i.getNamespace(), s2i.getName(), BuildConfig.class), s2i.patchBuildConfig((BuildConfig) os.get(s2i.getNamespace(), s2i.getName(), BuildConfig.class))), futureBuildConfig.completer());
            result.add(futureBuildConfig);

            return result;
        } else {
            log.info("No S2I {} differences found in namespace {}", s2i.getName(), s2i.getNamespace());
            return Collections.emptyList();
        }
    }
}
