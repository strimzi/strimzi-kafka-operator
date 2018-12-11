/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;

import java.util.HashMap;
import java.util.Map;

public class Annotations {

    public static final String STRIMZI_DOMAIN = "strimzi.io";

    public static final String ANNO_DEP_KUBE_IO_REVISION = "deployment.kubernetes.io/revision";

    private static Map<String, String> putAnnotation(ObjectMeta metadata) {
        Map<String, String> annotations = metadata.getAnnotations();
        if (annotations == null) {
            annotations = new HashMap<>(3);
            metadata.setAnnotations(annotations);
        }
        return annotations;
    }

    public static Map<String, String> annotations(HasMetadata resource) {
        return putAnnotation(resource.getMetadata());
    }

    public static Map<String, String> annotations(PodTemplateSpec podSpec) {
        return putAnnotation(podSpec.getMetadata());
    }

}
