/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.crdgenerator.annotations.Crd;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import io.sundr.builder.annotations.BuildableReference;
import io.sundr.builder.annotations.Inline;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;

@Crd(
        apiVersion = KafkaConnector.CRD_API_VERSION,
        spec = @Crd.Spec(
                names = @Crd.Spec.Names(
                        kind = KafkaConnector.RESOURCE_KIND,
                        plural = KafkaConnector.RESOURCE_PLURAL,
                        shortNames = {KafkaConnector.SHORT_NAME}
                ),
                group = KafkaConnector.RESOURCE_GROUP,
                scope = KafkaConnector.SCOPE,
                version = KafkaConnector.V1ALPHA1,
                versions = {
                        @Crd.Spec.Version(name = KafkaConnector.V1ALPHA1, served = true, storage = false)
                }
        )
)
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder",
        inline = @Inline(type = Doneable.class, prefix = "Doneable", value = "done"),
        refs = {@BuildableReference(ObjectMeta.class)}
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"apiVersion", "kind", "metadata", "spec", "status"})
@EqualsAndHashCode
public class KafkaConnector extends CustomResource implements UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;
    public static final String V1ALPHA1 = "v1alpha1";
    public static final List<String> VERSIONS = unmodifiableList(asList(V1ALPHA1));
    public static final String SCOPE = "Namespaced";
    public static final String CRD_API_VERSION = "apiextensions.k8s.io/v1alpha1";
    public static final String RESOURCE_PLURAL = "kafkaconnectors";
    public static final String RESOURCE_SINGULAR = "kafkaconnector";
    public static final String RESOURCE_GROUP = "kafka.strimzi.io";
    public static final String RESOURCE_KIND = "KafkaConnector";
    public static final String RESOURCE_LIST_KIND = RESOURCE_KIND + "List";
    public static final String SHORT_NAME = "kctr";

    private KafkaConnectorSpec spec;
    private Map<String, Object> additionalProperties;

    @Description("The specification of the Kafka Connector.")
    public KafkaConnectorSpec getSpec() {
        return spec;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : emptyMap();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>();
        }
        this.additionalProperties.put(name, value);
    }
}

