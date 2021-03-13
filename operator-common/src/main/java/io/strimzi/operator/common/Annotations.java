/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.client.CustomResource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;

public class Annotations {

    public static final String STRIMZI_DOMAIN = "strimzi.io/";
    public static final String STRIMZI_LOGGING_ANNOTATION = STRIMZI_DOMAIN + "logging";
    /**
     * Annotations for rolling a cluster whenever the logging (or it's part) has changed.
     * By changing the annotation we force a restart since the pod will be out of date compared to the statefulset.
     */
    public static final String ANNO_STRIMZI_LOGGING_HASH = STRIMZI_DOMAIN + "logging-hash";
    public static final String ANNO_STRIMZI_LOGGING_APPENDERS_HASH = STRIMZI_DOMAIN + "logging-appenders-hash";
    public static final String ANNO_STRIMZI_LOGGING_DYNAMICALLY_UNCHANGEABLE_HASH = STRIMZI_DOMAIN + "logging-appenders-hash";

    public static final String STRIMZI_IO_USE_CONNECTOR_RESOURCES = STRIMZI_DOMAIN + "use-connector-resources";
    // Used to store the revision of the Kafka Connect build (hash of the Dockerfile)
    public static final String STRIMZI_IO_CONNECT_BUILD_REVISION = STRIMZI_DOMAIN + "connect-build-revision";
    // Use to force rebuild of the container image even if the dockerfile did not changed
    public static final String STRIMZI_IO_CONNECT_FORCE_REBUILD = STRIMZI_DOMAIN + "force-rebuild";
    // Use to pause resource reconciliation
    public static final String ANNO_STRIMZI_IO_PAUSE_RECONCILIATION = STRIMZI_DOMAIN + "pause-reconciliation";
    public static final String ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE = STRIMZI_DOMAIN + "manual-rolling-update";
    // This annotation with related possible values (approve, stop, refresh) is set by the user for interacting
    // with the rebalance operator in order to start, stop, or refresh rebalancing proposals and operations.
    public static final String ANNO_STRIMZI_IO_REBALANCE = STRIMZI_DOMAIN + "rebalance";

    /**
     * Annotations for restarting KafkaConnector and KafkaMirrorMaker2 connectors or tasks
     */
    public static final String ANNO_STRIMZI_IO_RESTART = STRIMZI_DOMAIN + "restart";
    public static final String ANNO_STRIMZI_IO_RESTART_CONNECTOR = STRIMZI_DOMAIN + "restart-connector";
    public static final String ANNO_STRIMZI_IO_RESTART_TASK = STRIMZI_DOMAIN + "restart-task";
    public static final String ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK = STRIMZI_DOMAIN + "restart-connector-task";
    public static final String ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK_PATTERN_CONNECTOR = "connector";
    public static final String ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK_PATTERN_TASK = "task";
    public static final Pattern ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK_PATTERN = Pattern.compile("^(?<" +
        ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK_PATTERN_CONNECTOR +
        ">.+):(?<" +
        ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK_PATTERN_TASK +
        ">\\d+)$");

    public static final String ANNO_DEP_KUBE_IO_REVISION = "deployment.kubernetes.io/revision";

    /**
     * Whitelist of predicates that allows existing load balancer service annotations to be retained while reconciling the resources.
     */
    public static final List<Predicate<String>> LOADBALANCER_ANNOTATION_WHITELIST = List.of(
        annotation -> annotation.startsWith("cattle.io/"),
        annotation -> annotation.startsWith("field.cattle.io")
    );

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

    public static boolean booleanAnnotation(ObjectMeta metadata, String annotation, boolean defaultValue, String... deprecatedAnnotations) {
        String str = annotation(annotation, null, metadata, deprecatedAnnotations);
        return str != null ? parseBoolean(str) : defaultValue;
    }

    public static int intAnnotation(HasMetadata resource, String annotation, int defaultValue, String... deprecatedAnnotations) {
        ObjectMeta metadata = resource.getMetadata();
        String str = annotation(annotation, null, metadata, deprecatedAnnotations);
        return str != null ? parseInt(str) : defaultValue;
    }

    public static String stringAnnotation(HasMetadata resource, String annotation, String defaultValue, String... deprecatedAnnotations) {
        ObjectMeta metadata = resource.getMetadata();
        String str = annotation(annotation, null, metadata, deprecatedAnnotations);
        return str != null ? str : defaultValue;
    }

    public static int intAnnotation(PodTemplateSpec podSpec, String annotation, int defaultValue, String... deprecatedAnnotations) {
        ObjectMeta metadata = podSpec.getMetadata();
        String str = annotation(annotation, null, metadata, deprecatedAnnotations);
        return str != null ? parseInt(str) : defaultValue;
    }

    public static String stringAnnotation(PodTemplateSpec podSpec, String annotation, String defaultValue, String... deprecatedAnnotations) {
        ObjectMeta metadata = podSpec.getMetadata();
        String str = annotation(annotation, null, metadata, deprecatedAnnotations);
        return str != null ? str : defaultValue;
    }

    public static boolean hasAnnotation(HasMetadata resource, String annotation) {
        ObjectMeta metadata = resource.getMetadata();
        String str = annotation(annotation, null, metadata, null);
        return str != null;
    }

    public static int incrementIntAnnotation(PodTemplateSpec podSpec, String annotation, int defaultValue, String... deprecatedAnnotations) {
        ObjectMeta metadata = podSpec.getMetadata();
        return incrementIntAnnotation(annotation, defaultValue, metadata, deprecatedAnnotations);
    }

    public static int incrementIntAnnotation(HasMetadata podSpec, String annotation, int defaultValue, String... deprecatedAnnotations) {
        ObjectMeta metadata = podSpec.getMetadata();
        return incrementIntAnnotation(annotation, defaultValue, metadata, deprecatedAnnotations);
    }

    private static int incrementIntAnnotation(String annotation, int defaultValue, ObjectMeta metadata, String... deprecatedAnnotations) {
        Map<String, String> annos = annotations(metadata);
        String str = annotation(annotation, null, annos, deprecatedAnnotations);
        int v = str != null ? parseInt(str) : defaultValue;
        v++;
        annos.put(annotation, Integer.toString(v));
        return v;
    }

    private static String annotation(String annotation, String defaultValue, ObjectMeta metadata, String... deprecatedAnnotations) {
        Map<String, String> annotations = annotations(metadata);
        return annotation(annotation, defaultValue, annotations, deprecatedAnnotations);
    }

    private static String annotation(String annotation, String defaultValue, Map<String, String> annotations, String... deprecatedAnnotations) {
        String value = annotations.get(annotation);
        if (value == null) {
            if (deprecatedAnnotations != null) {
                for (String deprecated : deprecatedAnnotations) {
                    value = annotations.get(deprecated);
                    if (value != null) {
                        break;
                    }
                }
            }

            if (value == null) {
                value = defaultValue;
            }
        }
        return value;
    }

    /**
     * Whether the provided resource instance is a KafkaConnector and has the strimzi.io/pause-reconciliation annotation
     *
     * @param resource resource instance to check
     * @return true if the provided resource instance has the strimzi.io/pause-reconciliation annotation; false otherwise
     */
    public static boolean isReconciliationPausedWithAnnotation(CustomResource resource) {
        return Annotations.booleanAnnotation(resource, ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, false);
    }

    public static boolean isReconciliationPausedWithAnnotation(ObjectMeta metadata) {
        return Annotations.booleanAnnotation(metadata, ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, false);
    }

}
