/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.crdgenerator.annotations.Crd;

@Crd(
    apiVersion = "apiextensions.k8s.io/v1beta1",
    spec = @Crd.Spec(
        group = "crdgenerator.strimzi.io",
        names = @Crd.Spec.Names(
            kind = "ExampleWithSubresources",
            plural = "exampleswithsubresources",
            categories = {"strimzi"}),
        scope = "Namespaced",
        version = "v1alpha1",
    versions = {
        @Crd.Spec.Version(name = "v1alpha1", served = true, storage = true),
        @Crd.Spec.Version(name = "v1beta1", served = true, storage = false)
    },
    subresources = @Crd.Spec.Subresources(
            status = @Crd.Spec.Subresources.Status(),
            scale = @Crd.Spec.Subresources.Scale(
                    specReplicasPath = ".spec.replicas",
                    statusReplicasPath = ".status.replicas",
                    labelSelectorPath = ".status.selector"
            )
    ),
    additionalPrinterColumns = {
        @Crd.Spec.AdditionalPrinterColumn(
            name = "Foo",
            description = "The foo",
            jsonPath = "...",
            type = "integer"
        )
    }
    ))
public class ExampleWithSubresourcesCrd<T, U extends Number, V extends U> extends CustomResource {
    private String replicas;

    public String getReplicas() {
        return replicas;
    }

    public void setReplicas(String replicas) {
        this.replicas = replicas;
    }
}
