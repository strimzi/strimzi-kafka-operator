/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter.conversions;

import io.fabric8.kubernetes.client.KubernetesClient;

import java.util.function.BiConsumer;

/**
 * A holder for a resource created during the conversion and the method for its creation in a Kubernetes cluster
 *
 * @param name          Name of the resource
 * @param resource      The resource
 * @param k8sConsumer   Function for creating the resource
 */
public record MultipartResource(String name, Object resource, BiConsumer<KubernetesClient, String> k8sConsumer) {
}