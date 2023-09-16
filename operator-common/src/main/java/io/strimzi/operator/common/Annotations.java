/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.api.ResourceAnnotations;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;

/**
 * Class for holding some annotation keys and utility methods for handling annotations
 */
public class Annotations extends ResourceAnnotations {

    /**
     * Annotation for keeping Kafka and ZooKeeper servers' certificate thumbprints.
     */
    public static final String ANNO_STRIMZI_SERVER_CERT_HASH = STRIMZI_DOMAIN + "server-cert-hash";

    /**
     * Strimzi logging annotation
     */
    public static final String STRIMZI_LOGGING_ANNOTATION = STRIMZI_DOMAIN + "logging";

    /**
     * Annotations for rolling a cluster whenever the logging (or it's part) has changed.
     * By changing the annotation we force a restart since the pod will be out of date compared to the StrimziPodSet.
     */
    public static final String ANNO_STRIMZI_LOGGING_HASH = STRIMZI_DOMAIN + "logging-hash";

    /**
     * Annotation for tracking changes to logging appenders which cannot be changed dynamically
     */
    public static final String ANNO_STRIMZI_LOGGING_APPENDERS_HASH = STRIMZI_DOMAIN + "logging-appenders-hash";

    /**
     * Annotation for tracking authentication changes
     */
    public static final String ANNO_STRIMZI_AUTH_HASH = STRIMZI_DOMAIN + "auth-hash";

    /**
     * Annotation which enabled the use of the connector operator
     */
    public static final String STRIMZI_IO_USE_CONNECTOR_RESOURCES = STRIMZI_DOMAIN + "use-connector-resources";

    /**
     * Annotation used to store the revision of the Kafka Connect build (hash of the Dockerfile)
     */
    public static final String STRIMZI_IO_CONNECT_BUILD_REVISION = STRIMZI_DOMAIN + "connect-build-revision";

    /**
     * Annotation used to store the container image created by Kafka Connect build (This is used to track what was the
     * result of previous build when it does not change)
     */
    public static final String STRIMZI_IO_CONNECT_BUILD_IMAGE = STRIMZI_DOMAIN + "connect-build-image";

    /**
     * Annotation for restarting KafkaConnector
     */
    public static final String ANNO_STRIMZI_IO_RESTART = STRIMZI_DOMAIN + "restart";

    /**
     * Pattern for validation of value which specifies which connector or task should be restarted
     */
    public static final Pattern ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK_PATTERN = Pattern.compile("^(?<" +
        ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK_PATTERN_CONNECTOR +
        ">.+):(?<" +
        ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK_PATTERN_TASK +
        ">\\d+)$");

    /**
     * Annotation on PVCs storing the original configuration. It is used to revert any illegal changes.
     */
    public static final String ANNO_STRIMZI_IO_STORAGE = STRIMZI_DOMAIN + "storage";

    /**
     * Annotation for storing the information whether the PVC should be deleted when the node is scaled down or when the cluster is deleted.
     */
    public static final String ANNO_STRIMZI_IO_DELETE_CLAIM = STRIMZI_DOMAIN + "delete-claim";

    /**
     * Annotation for tracking Deployment revisions
     */
    public static final String ANNO_DEP_KUBE_IO_REVISION = "deployment.kubernetes.io/revision";

    /**
     * List of predicates that allows existing load balancer service annotations to be retained while reconciling the resources.
     */
    public static final List<Predicate<String>> LOADBALANCER_ANNOTATION_IGNORELIST = List.of(
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

    /**
     * Gets annotations map from a Kubernetes resource
     *
     * @param resource  Resource from which we want to get the annotations
     *
     * @return  Map with annotations
     */
    public static Map<String, String> annotations(HasMetadata resource) {
        return annotations(resource.getMetadata());
    }

    /**
     * Gets annotations from Pod Template in Deployments.
     *
     * @param podSpec  Pod template from which we want to get the annotations
     *
     * @return  Map with annotations
     */
    public static Map<String, String> annotations(PodTemplateSpec podSpec) {
        return annotations(podSpec.getMetadata());
    }

    /**
     * Gets a boolean value of an annotation from a Kubernetes resource
     *
     * @param resource                  Resource from which the annotation should be extracted
     * @param annotation                Annotation key for which we want the value
     * @param defaultValue              Default value if the annotation is not present
     * @param deprecatedAnnotations     Alternative annotations which should be checked if the main annotation is not present
     *
     * @return  Boolean value form the annotation, the fallback annotations or the default value
     */
    public static boolean booleanAnnotation(HasMetadata resource, String annotation, boolean defaultValue, String... deprecatedAnnotations) {
        ObjectMeta metadata = resource.getMetadata();
        String str = annotation(annotation, null, metadata, deprecatedAnnotations);
        return str != null ? parseBoolean(str) : defaultValue;
    }

    /**
     * Gets a boolean value of an annotation from a Kubernetes metadata object
     *
     * @param metadata                  Metadata object from which the annotation should be extracted
     * @param annotation                Annotation key for which we want the value
     * @param defaultValue              Default value if the annotation is not present
     * @param deprecatedAnnotations     Alternative annotations which should be checked if the main annotation is not present
     *
     * @return  Boolean value form the annotation, the fallback annotations or the default value
     */
    private static boolean booleanAnnotation(ObjectMeta metadata, String annotation, boolean defaultValue, String... deprecatedAnnotations) {
        String str = annotation(annotation, null, metadata, deprecatedAnnotations);
        return str != null ? parseBoolean(str) : defaultValue;
    }

    /**
     * Gets an integer value of an annotation from a Kubernetes resource
     *
     * @param resource                  Resource from which the annotation should be extracted
     * @param annotation                Annotation key for which we want the value
     * @param defaultValue              Default value if the annotation is not present
     * @param deprecatedAnnotations     Alternative annotations which should be checked if the main annotation is not present
     *
     * @return  Integer value form the annotation, the fallback annotations or the default value
     */
    public static int intAnnotation(HasMetadata resource, String annotation, int defaultValue, String... deprecatedAnnotations) {
        ObjectMeta metadata = resource.getMetadata();
        String str = annotation(annotation, null, metadata, deprecatedAnnotations);
        return str != null ? parseInt(str) : defaultValue;
    }

    /**
     * Gets a string value of an annotation from a Kubernetes resource
     *
     * @param resource                  Resource from which the annotation should be extracted
     * @param annotation                Annotation key for which we want the value
     * @param defaultValue              Default value if the annotation is not present
     * @param deprecatedAnnotations     Alternative annotations which should be checked if the main annotation is not present
     *
     * @return  String value form the annotation, the fallback annotations or the default value
     */
    public static String stringAnnotation(HasMetadata resource, String annotation, String defaultValue, String... deprecatedAnnotations) {
        ObjectMeta metadata = resource.getMetadata();
        String str = annotation(annotation, null, metadata, deprecatedAnnotations);
        return str != null ? str : defaultValue;
    }

    /**
     * Gets an integer value of an annotation from a Por template
     *
     * @param podSpec                   Por template from which the annotation should be extracted
     * @param annotation                Annotation key for which we want the value
     * @param defaultValue              Default value if the annotation is not present
     * @param deprecatedAnnotations     Alternative annotations which should be checked if the main annotation is not present
     *
     * @return  Integer value form the annotation, the fallback annotations or the default value
     */
    public static int intAnnotation(PodTemplateSpec podSpec, String annotation, int defaultValue, String... deprecatedAnnotations) {
        ObjectMeta metadata = podSpec.getMetadata();
        String str = annotation(annotation, null, metadata, deprecatedAnnotations);
        return str != null ? parseInt(str) : defaultValue;
    }

    /**
     * Gets a string value of an annotation from a Pod template
     *
     * @param podSpec                   Por template from which the annotation should be extracted
     * @param annotation                Annotation key for which we want the value
     * @param defaultValue              Default value if the annotation is not present
     * @param deprecatedAnnotations     Alternative annotations which should be checked if the main annotation is not present
     *
     * @return  String value form the annotation, the fallback annotations or the default value
     */
    public static String stringAnnotation(PodTemplateSpec podSpec, String annotation, String defaultValue, String... deprecatedAnnotations) {
        ObjectMeta metadata = podSpec.getMetadata();
        String str = annotation(annotation, null, metadata, deprecatedAnnotations);
        return str != null ? str : defaultValue;
    }

    /**
     * Checks if Kubernetes resource has an annotation with given key
     *
     * @param resource      Kubernetes resource which should be checked for the annotations presence
     * @param annotation    Annotation key
     *
     * @return  True if the annotation exists. False otherwise.
     */
    public static boolean hasAnnotation(HasMetadata resource, String annotation) {
        ObjectMeta metadata = resource.getMetadata();
        String str = annotation(annotation, null, metadata, null);
        return str != null;
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
     * Checks if the custom resource has the paused-reconciliation annotation and returns the value
     *
     * @param resource  Kubernetes resource
     *
     * @return True if the provided resource instance has the strimzi.io/pause-reconciliation annotation and has it set to true. False otherwise.
     */
    public static boolean isReconciliationPausedWithAnnotation(CustomResource resource) {
        return Annotations.booleanAnnotation(resource, ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, false);
    }

    /**
     * Checks if the metadata has the paused-reconciliation annotation and returns the value
     *
     * @param metadata  Kubernetes resource
     *
     * @return True if the metadata instance has the strimzi.io/pause-reconciliation annotation and has it set to true. False otherwise.
     */
    public static boolean isReconciliationPausedWithAnnotation(ObjectMeta metadata) {
        return Annotations.booleanAnnotation(metadata, ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, false);
    }

}
