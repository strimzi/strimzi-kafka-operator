/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.CustomResource;
import io.sundr.builder.annotations.Buildable;
import io.sundr.builder.annotations.BuildableReference;
import io.sundr.builder.annotations.Inline;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder",
        inline = @Inline(type = Doneable.class, prefix = "Doneable", value = "done"),
        refs = {@BuildableReference(ObjectMeta.class)}
)
@JsonPropertyOrder({"apiVersion", "kind", "metadata", "spec", "status"})
public class KafkaConnector extends CustomResource {
    public static final String V1ALPHA1 = "v1alpha1";
    public static final List<String> VERSIONS = unmodifiableList(asList(V1ALPHA1));
    public static final String SCOPE = "Namespaced";
    public static final String CRD_API_VERSION = "apiextensions.k8s.io/v1alpha1";
    public static final String RESOURCE_PLURAL = "kafkaconnectors";
    public static final String RESOURCE_SINGULAR = "kafkaconnectors";
    public static final String RESOURCE_GROUP = "kafka.strimzi.io";
    public static final String RESOURCE_KIND = "KafkaConnector";
    public static final String RESOURCE_LIST_KIND = RESOURCE_KIND + "List";


    private KafkaConnectorSpec spec;

    public KafkaConnectorSpec getSpec() {
        return spec;
    }
}

