/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api;

/**
 * An immutable set of labels
 */
public class ResourceLabels {
    /**
     * Strimzi domain used for the Strimzi labels
     */
    public static final String STRIMZI_DOMAIN = "strimzi.io/";
    
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
}
