/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.utils;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceList;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.annotations.ApiVersion;

import java.util.Arrays;
import java.util.Collection;

/**
 * Various utility methods useful during the conversion
 */
public class Utils {

    private Utils() {
        // Private constructor to prevent instantiation
    }
    /**
     * Returns Generic Kubernetes Resource Operation for a given resource
     *
     * @param client        Kubernetes client
     * @param kind          Kind
     * @param group         Group
     * @param apiVersion    API version
     *
     * @return  Client for working with a given Kubernetes resource
     */
    public static MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> versionedOperation(KubernetesClient client, String kind, String group, ApiVersion apiVersion) {
        return client.genericKubernetesResources(group + "/" + apiVersion, kind);
    }

    /**
     * Checks if the CRDs are installed and have the correct API versions. This is used as a pre-check to avoid running
     * the conversion tool against older Strimzi versions missing the `v1` CRD API or missing the CRDs entirely.
     *
     * @param client        Kubernetes client
     * @param crdNames      Collection of the CRD names
     * @param apiVersions   Array of required API versions
     */
    public static void checkCrdsHaveApiVersions(KubernetesClient client, Collection<String> crdNames, ApiVersion... apiVersions)   {
        for (String crdName : crdNames) {
            CustomResourceDefinition crd = client.apiextensions().v1().customResourceDefinitions().withName(crdName).get();

            if (crd == null) {
                throw new RuntimeException("CRD " + crdName + " is missing");
            }

            for (ApiVersion apiVersion : apiVersions) {
                if (crd.getSpec().getVersions().stream().noneMatch(v -> apiVersion.toString().equals(v.getName()))) {
                    throw new RuntimeException("CRD " + crdName + " is missing at least one of the required API versions " + Arrays.toString(apiVersions));
                }
            }
        }
    }

    private Utils() {
        // Private constructor to prevent instantiation
    }
}
