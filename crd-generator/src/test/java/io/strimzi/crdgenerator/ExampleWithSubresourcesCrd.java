/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.crdgenerator.annotations.Crd;

@Crd(
    spec = @Crd.Spec(
        group = "crdgenerator.strimzi.io",
        names = @Crd.Spec.Names(
            kind = "ExampleWithSubresources",
            plural = "exampleswithsubresources",
            categories = {"strimzi"}),
        scope = "Namespaced",
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
                type = "integer")
        }
    ))
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"replicas", "spec", "status"})
public class ExampleWithSubresourcesCrd<T, U extends Number, V extends U> extends CustomResource {
    private String replicas;

    public String getReplicas() {
        return replicas;
    }

    public void setReplicas(String replicas) {
        this.replicas = replicas;
    }
}
