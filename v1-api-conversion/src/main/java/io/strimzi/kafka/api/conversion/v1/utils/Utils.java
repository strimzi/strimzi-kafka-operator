/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.utils;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.annotations.ApiVersion;

/**
 * Various utility methods useful during the conversion
 */
public class Utils {
    /**
     * Returns Generic Kubernetes Resource Operation for a given resource
     *
     * @param client        Kubernetes client
     * @param kind          Kind
     * @param group         Group
     * @param apiVersion    API version
     *
     * @return  Client for working with given Kubernetes resource
     */
    public static MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> versionedOperation(KubernetesClient client, String kind, String group, ApiVersion apiVersion) {
        return client.genericKubernetesResources(group + "/" + apiVersion, kind);
    }
}
