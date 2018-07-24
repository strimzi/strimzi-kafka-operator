/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.crdgenerator.annotations.Crd;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import io.sundr.builder.annotations.BuildableReference;

import java.util.HashMap;
import java.util.Map;

@JsonDeserialize(
        using = JsonDeserializer.None.class
)
@Crd(
        apiVersion = KafkaConnectAssembly.CRD_API_VERSION,
        spec = @Crd.Spec(
                names = @Crd.Spec.Names(
                        kind = KafkaConnectAssembly.RESOURCE_KIND,
                        plural = KafkaConnectAssembly.RESOURCE_PLURAL
                ),
                group = KafkaConnectAssembly.RESOURCE_GROUP,
                scope = "Namespaced",
                version = KafkaConnectAssembly.VERSION
        )
)
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = true,
        builderPackage = "io.strimzi.api.kafka.model",
        refs = {
                @BuildableReference(io.fabric8.kubernetes.api.model.ObjectMeta.class)
        }
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"apiVersion", "kind", "metadata", "spec"})
public class KafkaConnectAssembly extends CustomResource {

    private static final long serialVersionUID = 1L;

    public static final String VERSION = "v1alpha1";
    public static final String RESOURCE_KIND = "KafkaConnect";
    public static final String RESOURCE_LIST_KIND = RESOURCE_KIND + "List";
    public static final String RESOURCE_GROUP = "kafka.strimzi.io";
    public static final String RESOURCE_PLURAL = "kafkaconnects";
    public static final String RESOURCE_SINGULAR = "kafkaconnect";
    public static final String CRD_API_VERSION = "apiextensions.k8s.io/v1beta1";
    public static final String CRD_NAME = RESOURCE_PLURAL + "." + RESOURCE_GROUP;

    private String apiVersion;
    private KafkaConnectAssemblySpec spec;
    private ObjectMeta metadata;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Override
    public String getApiVersion() {
        return apiVersion;
    }

    @Override
    public void setApiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
    }

    @JsonProperty("kind")
    @Override
    public String getKind() {
        return RESOURCE_KIND;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Override
    public ObjectMeta getMetadata() {
        return super.getMetadata();
    }

    @Override
    public void setMetadata(ObjectMeta metadata) {
        super.setMetadata(metadata);
    }

    @Description("The specification of the Kafka Connect deployment.")
    public KafkaConnectAssemblySpec getSpec() {
        return spec;
    }

    public void setSpec(KafkaConnectAssemblySpec spec) {
        this.spec = spec;
    }

    @Override
    public String toString() {
        YAMLMapper mapper = new YAMLMapper().disable(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID);
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}
