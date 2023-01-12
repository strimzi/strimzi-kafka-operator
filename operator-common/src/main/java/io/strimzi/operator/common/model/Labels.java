/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import io.fabric8.kubernetes.api.model.HasMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;

/**
 * An immutable set of labels
 */
public class Labels {
    /**
     * Strimzi domain used for the Strimzi labels
     */
    public static final String STRIMZI_DOMAIN = "strimzi.io/";

    /**
     * Kubernetes domain used for Kubernetes labels
     */
    public static final String KUBERNETES_DOMAIN = "app.kubernetes.io/";

    /**
     * The kind of a Kubernetes / OpenShift Resource. It contains the same value as the Kind of the corresponding
     * Custom Resource. It should have on of the following values:
     *
     * <ul>
     *   <li>Kafka</li>
     *   <li>KafkaConnect</li>
     *   <li>KafkaMirrorMaker</li>
     *   <li>KafkaBridge</li>
     *   <li>KafkaUser</li>
     *   <li>KafkaTopic</li>
     * </ul>
     */
    public static final String STRIMZI_KIND_LABEL = STRIMZI_DOMAIN + "kind";

    /**
     * The Strimzi cluster the resource is part of. This is typically the name of the custom resource.
     */
    public static final String STRIMZI_CLUSTER_LABEL = STRIMZI_DOMAIN + "cluster";

    /**
     * Type of the Strimzi component to which given resource belongs. E.g. Kafka or ZooKeeper. This label does not
     * depend on the name of the cluster. This is useful to identify resources which belong to the same component but
     * different clusters which is useful for example for scheduling (e.g. when you do not want this broker to be
     * scheduled on a node where any other Kafka broker is running).
     */
    public static final String STRIMZI_COMPONENT_TYPE_LABEL = STRIMZI_DOMAIN + "component-type";

    /**
     * Name of the component to which given resource belongs. This typically consists of the cluster name and component.
     */
    public static final String STRIMZI_NAME_LABEL = STRIMZI_DOMAIN + "name";

    /**
     * Identifies Pods managed directly by Strimzi and resources belonging directly to them such as per-node services,
     * config maps or secrets.
     */
    public static final String STRIMZI_POD_NAME_LABEL = STRIMZI_DOMAIN + "pod-name";

    /**
     * Indicates whether the resource should be controlled by a Strimzi operator instance and which StrimziPodSet
     * controls
     */
    public static final String STRIMZI_CONTROLLER_LABEL = STRIMZI_DOMAIN + "controller";

    /**
     * Indicates the name of the controller
     */
    public static final String STRIMZI_CONTROLLER_NAME_LABEL = STRIMZI_DOMAIN + "controller-name";

    /**
     * The name of the label used for Strimzi discovery.
     * This label should be set by Strimzi on services which are user interfaces when users are expected to connect.
     * Applications using Strimzi can use this label to find the services and connect to Strimzi created clusters.
     * This label should be used for example on the Kafka bootstrap service.
     */
    public static final String STRIMZI_DISCOVERY_LABEL = STRIMZI_DOMAIN + "discovery";

    /**
     * The name of the application
     * https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/#labels
     */
    public static final String KUBERNETES_NAME_LABEL = KUBERNETES_DOMAIN + "name";

    /**
     * A unique name identifying the instance of an application
     * https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/#labels
     */
    public static final String KUBERNETES_INSTANCE_LABEL = KUBERNETES_DOMAIN + "instance";

    /**
     * The name of a higher level application this one is part of
     * https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/#labels
     */
    public static final String KUBERNETES_PART_OF_LABEL = KUBERNETES_DOMAIN + "part-of";

    /**
     * Indicates the application name
     */
    public static final String APPLICATION_NAME = "strimzi";

    /**
     * The tool being used to manage the operation of an application
     * https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/#labels
     */
    public static final String KUBERNETES_MANAGED_BY_LABEL = KUBERNETES_DOMAIN + "managed-by";

    /**
     * Used to identify individual pods
     */
    public static final String KUBERNETES_STATEFULSET_POD_LABEL = "statefulset.kubernetes.io/pod-name";

    /**
     * Used to exclude parent CR's labels from being assigned to provisioned subresources
     */
    public static final Pattern STRIMZI_LABELS_EXCLUSION_PATTERN = Pattern.compile(System.getenv()
            .getOrDefault("STRIMZI_LABELS_EXCLUSION_PATTERN", "(^app.kubernetes.io/(?!part-of).*|^kustomize.toolkit.fluxcd.io.*)"));

    /**
     * The empty set of labels.
     */
    public static final Labels EMPTY = new Labels(emptyMap());

    private final Map<String, String> labels;

    /**
     * @param additionalLabels The labels
     * @return A {@code Labels} instance from the given map
     */
    private static Labels additionalLabels(Map<String, String> additionalLabels) {
        if (additionalLabels == null || additionalLabels.isEmpty()) {
            return EMPTY;
        } else {
            List<String> invalidLabels = additionalLabels
                    .keySet()
                    .stream()
                    .filter(key -> key.startsWith(Labels.STRIMZI_DOMAIN) && !key.startsWith(Labels.STRIMZI_CLUSTER_LABEL))
                    .collect(Collectors.toList());
            if (invalidLabels.size() > 0) {
                throw new IllegalArgumentException("Labels starting with " + STRIMZI_DOMAIN + " are not allowed in Custom Resources, such labels should be removed.");
            }

            return new Labels(additionalLabels);
        }
    }

    /**
     * @param additionalLabels The labels to add.
     * @return A new instances with the given {@code additionalLabels} added to the labels in this instance.
     */
    public Labels withAdditionalLabels(Map<String, String> additionalLabels) {
        if (additionalLabels == null || additionalLabels.isEmpty()) {
            return this;
        } else {
            Map<String, String> newLabels = new HashMap<>(labels.size());
            newLabels.putAll(labels);
            newLabels.putAll(Labels.additionalLabels(additionalLabels).toMap());

            return new Labels(newLabels);
        }
    }

    /**
     * @param resource The resource to get the labels of.
     * @return A new instance with filtered labels added from the given {@code resource}.
     */
    public static Labels fromResource(HasMetadata resource) {
        Map<String, String> additionalLabels = resource.getMetadata().getLabels();

        if (additionalLabels != null) {
            additionalLabels = additionalLabels
                    .entrySet()
                    .stream()
                    .filter(entryset -> !STRIMZI_LABELS_EXCLUSION_PATTERN.matcher(entryset.getKey()).matches())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        return additionalLabels(additionalLabels);
    }

    /**
     * @param labels The map of labels.
     * @return A labels instance from Map.
     */
    public static Labels fromMap(Map<String, String> labels) {
        if (labels != null) {
            return new Labels(labels);
        }

        return EMPTY;
    }

    /**
     * Parse Labels from String into Labels object. The expected format of the String with labels is `key1=value1,key2=value2`
     *
     * @param stringLabels String with labels
     * @return Labels object with parsed labels
     * @throws IllegalArgumentException The string could not be parsed.
     */
    public static Labels fromString(String stringLabels) throws IllegalArgumentException {
        Map<String, String> labels = new HashMap<>();

        try {
            if (stringLabels != null && !stringLabels.isEmpty()) {
                String[] labelsArray = stringLabels.split(",");
                for (String label : labelsArray) {
                    String[] fields = label.split("=");
                    labels.put(fields[0].trim(), fields[1].trim());
                }
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse labels from string " + stringLabels, e);
        }

        return new Labels(labels);
    }

    private Labels(Map<String, String> labels) {
        this.labels = unmodifiableMap(new HashMap<>(labels));
    }

    private Labels with(String label, String value) {
        Map<String, String> newLabels = new HashMap<>(labels.size() + 1);
        newLabels.putAll(labels);
        newLabels.put(label, value);
        return new Labels(newLabels);
    }

    /**
     * The same labels as this instance, but with the given {@code kind} for the {@code strimzi.io/kind} key.
     *
     * @param kind The kind to add.
     * @return A new instance with the given kind added.
     */
    public Labels withStrimziKind(String kind) {
        return with(STRIMZI_KIND_LABEL, kind);
    }


    /**
     * The same labels as this instance, but with the given {@code cluster} for the {@code strimzi.io/cluster} key.
     *
     * @param cluster The cluster to add.
     * @return A new instance with the given cluster added.
     */
    public Labels withStrimziCluster(String cluster) {
        return with(STRIMZI_CLUSTER_LABEL, cluster);
    }

    /**
     * The same labels as this instance, but with the application name {@code strimzi} for the {@code app.kubernetes.io/name} key.
     *
     * @param name The kubernetes name to add.
     * @return A new instance with the given kubernetes application name added.
     */
    public Labels withKubernetesName(String name) {
        return with(Labels.KUBERNETES_NAME_LABEL, name);
    }

    /**
     * The same labels as this instance, but with the given {@code instance} for the {@code app.kubernetes.io/instance} key.
     *
     * @param instanceName The instance to add.
     * @return A new instance with the given kubernetes application instance added.
     */
    public Labels withKubernetesInstance(String instanceName) {
        return with(Labels.KUBERNETES_INSTANCE_LABEL, getOrValidInstanceLabelValue(instanceName));
    }

    /**
     * The same labels as this instance, but with the given {@code part-of} for the {@code app.kubernetes.io/part-of} key.
     *
     * @param instanceName The instance used to generate the unique label to add composed with the application name.
     * @return A new instance with the given kubernetes application part-of label added.
     */
    public Labels withKubernetesPartOf(String instanceName) {
        return with(Labels.KUBERNETES_PART_OF_LABEL, getOrValidInstanceLabelValue(APPLICATION_NAME + "-" + instanceName));
    }

    /**
     * Validates the instance name and if needed modifies it to make it a valid Label value:
     * - (([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?
     * - 63 characters max
     * This method is written to handle instance names which are valid resource names, since they are derived from a
     * custom resource. It does not modify arbitrary names as label values.
     *
     * @param instance Theoriginal name of the instance
     * @return Either the original instance name or a modified version to match label value criteria
     */
    /*test*/ static String getOrValidInstanceLabelValue(String instance) {
        if (instance == null) {
            return "";
        }

        int i = Math.min(instance.length(), 63);

        while (i > 0) {
            char lastChar = instance.charAt(i - 1);

            if (lastChar == '.' || lastChar == '-' || lastChar == '_') {
                i--;
            } else {
                break;
            }
        }

        return instance.substring(0, i);
    }

    /**
     * The same labels as this instance, but with the given {@code operatorName} for the {@code app.kubernetes.io/managed-by} key.
     *
     * @param operatorName The name of the operator managing this resource.
     * @return A new instance with the given operator that is managing this resource.
     */
    public Labels withKubernetesManagedBy(String operatorName) {
        return with(Labels.KUBERNETES_MANAGED_BY_LABEL, operatorName);
    }

    /**
     * The same labels as this instance, but with the given {@code name} for the {@code strimzi.io/name} key.
     *
     * @param name The name to add
     * @return A new instance with the given name added.
     */
    public Labels withStrimziName(String name) {
        return with(STRIMZI_NAME_LABEL, name);
    }

    /**
     * The same labels as this instance, but with the given {@code type} for the {@code strimzi.io/component-type} key.
     *
     * @param type The type to add
     *
     * @return A new instance with the given type added.
     */
    public Labels withStrimziComponentType(String type) {
        return with(STRIMZI_COMPONENT_TYPE_LABEL, type);
    }

    /**
     * The same labels as this instance, but with the given {@code name} for the {@code strimzi.io/pod-name} key.
     *
     * @param name The name to add
     * @return A new instance with the given name added.
     */
    public Labels withStrimziPodName(String name) {
        return with(STRIMZI_POD_NAME_LABEL, name);
    }

    /**
     * The same labels as this instance, but with "true" for the {@code strimzi.io/discovery} key.
     *
     * @return A new instance with the given name added.
     */
    public Labels withStrimziDiscovery() {
        return with(STRIMZI_DISCOVERY_LABEL, "true");
    }

    /**
     * The same labels as this instance, but with the given {@code name} for the {@code statefulset.kubernetes.io/pod-name} key.
     *
     * @param name The pod name to add
     * @return A new instance with the given pod name added.
     */
    public Labels withStatefulSetPod(String name) {
        return with(KUBERNETES_STATEFULSET_POD_LABEL, name);
    }

    /**
     * Sets the Strimzi controller label to strimzipodset
     *
     * @param controllerName Name of the controlling StrimziPodSet
     *
     * @return A new instance with the given pod name added.
     */
    public Labels withStrimziPodSetController(String controllerName) {
        return with(STRIMZI_CONTROLLER_LABEL, "strimzipodset").with(STRIMZI_CONTROLLER_NAME_LABEL, controllerName);
    }

    /**
     * @return an unmodifiable map of the labels.
     */
    public Map<String, String> toMap() {
        return labels;
    }

    /**
     * @return A string which can be used as the Kubernetes label selector (e.g. key1=value1,key2=value2).
     */
    public String toSelectorString() {
        return labels.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue()).collect(Collectors.joining(","));
    }

    /**
     * @param cluster The cluster.
     * @return A singleton instance with the given {@code cluster} for the {@code strimzi.io/cluster} key.
     */
    public static Labels forStrimziCluster(String cluster) {
        return new Labels(singletonMap(STRIMZI_CLUSTER_LABEL, cluster));
    }

    /**
     * @param kind The kind.
     * @return A singleton instance with the given {@code kind} for the {@code strimzi.io/kind} key.
     */
    public static Labels forStrimziKind(String kind) {
        return new Labels(singletonMap(STRIMZI_KIND_LABEL, kind));
    }

    /**
     * @return A new instances containing just the strimzi.io selector labels present in this instance.
     */
    public Labels strimziSelectorLabels() {
        Map<String, String> newLabels = new HashMap<>(3);

        List<String> strimziSelectorLabels = new ArrayList<>(3);
        strimziSelectorLabels.add(STRIMZI_CLUSTER_LABEL);
        strimziSelectorLabels.add(STRIMZI_NAME_LABEL);
        strimziSelectorLabels.add(STRIMZI_KIND_LABEL);

        strimziSelectorLabels.forEach(key -> {
            if (labels.containsKey(key)) newLabels.put(key, labels.get(key));
        });

        return new Labels(newLabels);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Labels labels1 = (Labels) o;
        return Objects.equals(labels, labels1.labels);
    }

    @Override
    public int hashCode() {

        return Objects.hash(labels);
    }

    @Override
    public String toString() {
        return "Labels" + labels;
    }

    /**
     *
     * When adding new labels, ensure the names of any resources are truncated for sanitization purposes to be compatible with Kubernetes
     * Note: Valid label values must be a maximum length of 63 characters
     * https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
     *
     * @param resource              Kubernetes resource with metadata. It is used to get the resource name as well as copy
     *                              its labels. This is typically a custom resource which owns the whole deployment.
     * @param strimziComponentName  Name of the component used for the strimzi.io/name label
     * @param strimziComponentType  Type of the component (e.g. kafka, zookeeper, etc.) used for the strimzi.io/component-type label
     * @param managedBy             Name of the component managing this resource (e.g. strimzi-cluster-operator)
     *
     * @return  The default set of labels used for the Kubernetes resources
     */
    public static Labels generateDefaultLabels(HasMetadata resource, String strimziComponentName, String strimziComponentType, String managedBy) {
        String customResourceName = resource.getMetadata().getName();

        return Labels.fromResource(resource)
                // Strimzi labels
                .withStrimziKind(resource.getKind())
                .withStrimziName(strimziComponentName)
                .withStrimziCluster(customResourceName)
                .withStrimziComponentType(strimziComponentType)
                // Kubernetes labels
                .withKubernetesName(strimziComponentType)
                .withKubernetesInstance(customResourceName)
                .withKubernetesPartOf(customResourceName)
                .withKubernetesManagedBy(managedBy);
    }
}
