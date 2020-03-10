/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.HasMetadata;

/**
 * An immutable set of labels
 */
public class Labels {

    public static final String STRIMZI_DOMAIN = "strimzi.io/";
    public static final String KUBERNETES_DOMAIN = "app.kubernetes.io/";

    /**
     * The kind of a Kubernetes / OpenShift Resource. It contains the same value as the Kind of the corresponding
     * Custom Resource. It should have on of the following values:
     *
     * <ul>
     *   <li>Kafka</li>
     *   <li>KafkaConnect</li>
     *   <li>KafkaConnectS2I</li>
     *   <li>KafkaMirrorMaker</li>
     *   <li>KafkaBridge</li>
     *   <li>KafkaUser</li>
     *   <li>KafkaTopic</li>
     * </ul>
     */
    public static final String STRIMZI_KIND_LABEL = STRIMZI_DOMAIN + "kind";

    /**
     * The Strimzi cluster the resource is part of.
     * The value is the cluster name (i.e. the name of the cluster CM)
     */
    public static final String STRIMZI_CLUSTER_LABEL = STRIMZI_DOMAIN + "cluster";

    /**
     * The name of the K8S resource.
     * This is often the same name as the cluster
     * (i.e. the same as {@code strimzi.io/cluster})
     * but is different in some cases (e.g. headful and headless services)
     */
    public static final String STRIMZI_NAME_LABEL = STRIMZI_DOMAIN + "name";

    /**
     * The name of the label used for Strimzi discovery.
     * This label should be set by Strimzi on services which are user interfaces when users are expected to connect.
     * Applications using Strimzi can use this label to find the services and connect to Strimzi created clusters.
     * This label should be used for example on the Kafka bootstrap service.
     */
    public static final String STRIMZI_DISCOVERY_LABEL = STRIMZI_DOMAIN + "discovery";

    public static final String KUBERNETES_NAME_LABEL = KUBERNETES_DOMAIN + "name";
    public static final String KUBERNETES_INSTANCE_LABEL = KUBERNETES_DOMAIN + "instance";
    public static final String KUBERNETES_PART_OF_LABEL = KUBERNETES_DOMAIN + "part-of";
    public static final String KUBERNETES_MANAGED_BY_LABEL = KUBERNETES_DOMAIN + "managed-by";

    public static final String KUBERNETES_NAME = "strimzi";

    /**
     * Used to identify individual pods
     */
    public static final String KUBERNETES_STATEFULSET_POD_LABEL = "statefulset.kubernetes.io/pod-name";

    /**
     * The empty set of labels.
     */
    public static final Labels EMPTY = new Labels(emptyMap());

    private final Map<String, String> labels;

    /**
     * @return The value of the {@code strimzi.io/cluster} label of the given {@code resource}.
     * @param resource The resource.
     */
    public static String cluster(HasMetadata resource) {
        return resource.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
    }

    /**
     * @return the value of the {@code strimzi.io/name} label of the given {@code resource}.
     * @param resource The resource.
     */
    public static String name(HasMetadata resource) {
        return resource.getMetadata().getLabels().get(Labels.STRIMZI_NAME_LABEL);
    }

    /**
     * @return A {@code Labels} instance from the given map
     * @param userLabels The labels
     */
    public static Labels userLabels(Map<String, String> userLabels) {

        if (userLabels == null) {
            return EMPTY;
        }

        List<String> invalidLabels = userLabels
                .keySet()
                .stream()
                .filter(key -> key.startsWith(Labels.STRIMZI_DOMAIN) && !key.startsWith(Labels.STRIMZI_CLUSTER_LABEL))
                .collect(Collectors.toList());
        if (invalidLabels.size() > 0) {
            throw new IllegalArgumentException("Labels starting with " + STRIMZI_DOMAIN + " are not allowed in Custom Resources, such labels should be removed.");
        }

        // Remove Kubernetes Domain specific labels
        Map<String, String> filteredLabels = userLabels
                .entrySet()
                .stream()
                .filter(entryset -> !entryset.getKey().startsWith(Labels.KUBERNETES_DOMAIN))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return new Labels(filteredLabels);
    }

    /**
     * @param userLabels The labels to add.
     * @return A new instances with the given {@code userLabels} added to the labels in this instance.
     */
    public Labels withUserLabels(Map<String, String> userLabels) {
        Map<String, String> newLabels = new HashMap<>(labels.size());
        newLabels.putAll(labels);
        newLabels.putAll(Labels.userLabels(userLabels).toMap());

        return new Labels(newLabels);
    }

    /**
     * @param resource The resource to get the labels of
     * @return the labels of the given {@code resource}.
     */
    public static Labels fromResource(HasMetadata resource) {
        return resource.getMetadata().getLabels() != null ? userLabels(resource.getMetadata().getLabels()) : EMPTY;
    }

    /**
     * @return A labels instance from Map.
     * @param labels The map of labels.
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
     * @param stringLabels  String with labels
     * @return  Labels object with parsed labels
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
     * @param kind The kind to add.
     * @return A new instance with the given kind added.
     */
    public Labels withKind(String kind) {
        return with(STRIMZI_KIND_LABEL, kind);
    }


    /**
     * The same labels as this instance, but with the given {@code cluster} for the {@code strimzi.io/cluster} key.
     * @param cluster The cluster to add.
     * @return A new instance with the given cluster added.
     */
    public Labels withCluster(String cluster) {
        return with(STRIMZI_CLUSTER_LABEL, cluster);
    }

    /**
     * The same labels as this instance, but with the application name {@code strimzi} for the {@code app.kubernetes.io/name} key.
     * @return A new instance with the given kubernetes application name added.
     */
    public Labels withKubernetesName() {
        return with(Labels.KUBERNETES_NAME_LABEL, Labels.KUBERNETES_NAME);
    }

    /**
     * The same labels as this instance, but with the given {@code instance} for the {@code app.kubernetes.io/instance} key.
     * @param instance The instance to add.
     * @return A new instance with the given kubernetes application instance added.
     */
    public Labels withKubernetesInstance(String instance) {
        return with(Labels.KUBERNETES_INSTANCE_LABEL, getOrValidInstanceLabelValue(instance));
    }

    /**
     * The same labels as this instance, but with the given {@code part-of} for the {@code app.kubernetes.io/part-of} key.
     * @param partof The partof label to add.
     * @return A new instance with the given kubernetes application part-of label added.
     */
    public Labels withKubernetesPartOf(String partof) {
        return with(Labels.KUBERNETES_PART_OF_LABEL, getOrValidInstanceLabelValue(partof));
    }

    /**
     * Validates the instance name and if needed modifies it to make it a valid Label value:
     *   - (([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?
     *   - 63 characters max
     * This method is written to handle instance names which are valid resource names, since they are derived from a
     * custom resource. It does not modify arbitrary names as label values.
     *
     *
     * @param instance Theoriginal name of the instance
     * @return Either the original instance name or a modified version to match label value criteria
     */
    /*test*/ static String getOrValidInstanceLabelValue(String instance) {
        if (instance == null)   {
            return "";
        }

        int i = Math.min(instance.length(), 63);

        while (i > 0)   {
            char lastChar = instance.charAt(i - 1);

            if (lastChar == '.' || lastChar == '-') {
                i--;
            } else {
                break;
            }
        }

        return instance.substring(0, i);
    }

    /**
     * The same labels as this instance, but with the given {@code operatorName} for the {@code app.kubernetes.io/managed-by} key.
     * @param operatorName The name of the operator managing this resource.
     * @return A new instance with the given operator that is managing this resourse.
     */
    public Labels withKubernetesManagedBy(String operatorName) {
        return with(Labels.KUBERNETES_MANAGED_BY_LABEL, operatorName);
    }

    /**
     * The same labels as this instance, but with the given {@code name} for the {@code strimzi.io/name} key.
     * @param name The name to add
     * @return A new instance with the given name added.
     */
    public Labels withName(String name) {
        return with(STRIMZI_NAME_LABEL, name);
    }

    /**
     * The same labels as this instance, but with "true" for the {@code strimzi.io/discovery} key.
     * @return A new instance with the given name added.
     */
    public Labels withDiscovery() {
        return with(STRIMZI_DISCOVERY_LABEL, "true");
    }

    /**
     * The same labels as this instance, but with the given {@code name} for the {@code statefulset.kubernetes.io/pod-name} key.
     * @param name The pod name to add
     * @return A new instance with the given pod name added.
     */
    public Labels withStatefulSetPod(String name) {
        return with(KUBERNETES_STATEFULSET_POD_LABEL, name);
    }

    /**
     * @return an unmodifiable map of the labels.
     */
    public Map<String, String> toMap() {
        return labels;
    }

    /**
     * @param cluster The cluster.
     * @return A singleton instance with the given {@code cluster} for the {@code strimzi.io/cluster} key.
     */
    public static Labels forCluster(String cluster) {
        return new Labels(singletonMap(STRIMZI_CLUSTER_LABEL, cluster));
    }

    /**
     * @param kind The kind.
     * @return A singleton instance with the given {@code kind} for the {@code strimzi.io/kind} key.
     */
    public static Labels forKind(String kind) {
        return new Labels(singletonMap(STRIMZI_KIND_LABEL, kind));
    }

    /**
     * @return An instances containing just the strimzi.io labels present in this instance.
     */
    public Labels strimziSelectorLabels() {
        Map<String, String> newLabels = new HashMap<>(3);

        for (Map.Entry<String, String> entry : labels.entrySet()) {
            if (entry.getKey().startsWith(STRIMZI_DOMAIN) && !entry.getKey().equals(STRIMZI_DISCOVERY_LABEL)) {
                newLabels.put(entry.getKey(), entry.getValue());
            }
        }

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
}
