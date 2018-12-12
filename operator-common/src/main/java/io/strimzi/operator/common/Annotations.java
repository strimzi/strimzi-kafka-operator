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

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;

public class Annotations {

    public static final String STRIMZI_DOMAIN = "strimzi.io";

    public static final String ANNO_DEP_KUBE_IO_REVISION = "deployment.kubernetes.io/revision";

    private static Map<String, String> annotations(ObjectMeta metadata) {
        Map<String, String> annotations = metadata.getAnnotations();
        if (annotations == null) {
            annotations = new HashMap<>(3);
            metadata.setAnnotations(annotations);
        }
        return annotations;
    }

    public static Map<String, String> annotations(HasMetadata resource) {
        return annotations(resource.getMetadata());
    }

    public static Map<String, String> annotations(PodTemplateSpec podSpec) {
        return annotations(podSpec.getMetadata());
    }

    public static boolean booleanAnnotation(HasMetadata resource, String annotation, boolean defaultValue, String... deprecatedAnnotations) {
        ObjectMeta metadata = resource.getMetadata();
        String str = annotation(annotation, null, metadata, deprecatedAnnotations);
        return str != null ? parseBoolean(str) : defaultValue;
    }

    public static int intAnnotation(HasMetadata resource, String annotation, int defaultValue, String... deprecatedAnnotations) {
        ObjectMeta metadata = resource.getMetadata();
        String str = annotation(annotation, null, metadata, deprecatedAnnotations);
        return str != null ? parseInt(str) : defaultValue;
    }

    public static int intAnnotation(PodTemplateSpec podSpec, String annotation, int defaultValue, String... deprecatedAnnotations) {
        ObjectMeta metadata = podSpec.getMetadata();
        String str = annotation(annotation, null, metadata, deprecatedAnnotations);
        return str != null ? parseInt(str) : defaultValue;
    }

    private static String annotation(String annotation, String defaultValue, ObjectMeta metadata, String[] deprecatedAnnotations) {
        Map<String, String> annotations = annotations(metadata);
        String value = annotations.get(annotation);
        if (value == null) {
            for (String deprecated : deprecatedAnnotations) {
                value = annotations.get(deprecated);
                if (value != null) {
                    break;
                }
            }
            if (value == null) {
                value = defaultValue;
            }
        }
        return value;
    }

}
