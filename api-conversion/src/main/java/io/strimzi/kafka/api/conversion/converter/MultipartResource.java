/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.converter;

import io.fabric8.kubernetes.client.KubernetesClient;

import java.util.function.BiConsumer;

public class MultipartResource {
    private String name;
    private Object resource;
    private BiConsumer<KubernetesClient, String> k8sConsumer;

    public MultipartResource(String name, Object resource, BiConsumer<KubernetesClient, String> k8sConsumer) {
        this.name = name;
        this.resource = resource;
        this.k8sConsumer = k8sConsumer;
    }

    public String getName() {
        return name;
    }

    public Object getResource() {
        return resource;
    }

    public BiConsumer<KubernetesClient, String> getK8sConsumer() {
        return k8sConsumer;
    }
}

