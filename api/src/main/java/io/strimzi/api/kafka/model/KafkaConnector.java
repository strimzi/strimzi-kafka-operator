/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.api.kafka.model.status.KafkaConnectorStatus;
import io.strimzi.crdgenerator.annotations.Crd;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import io.sundr.builder.annotations.Inline;
import lombok.EqualsAndHashCode;
import lombok.ToString;

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
                        @Crd.Spec.Version(
                                name = KafkaConnector.V1ALPHA1,
                                served = true,
                                storage = true)
                }
        )
)
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder",
        inline = @Inline(type = Doneable.class, prefix = "Doneable", value = "done")
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"apiVersion", "kind", "metadata", "spec"})
@EqualsAndHashCode
@ToString
public class KafkaConnector extends CustomResource implements UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;
    public static final String V1ALPHA1 = "v1alpha1";
    public static final List<String> VERSIONS = unmodifiableList(asList(V1ALPHA1));
    public static final String SCOPE = "Namespaced";
    public static final String CRD_API_VERSION = "apiextensions.k8s.io/v1beta1";
    public static final String RESOURCE_PLURAL = "kafkaconnectors";
    public static final String RESOURCE_SINGULAR = "kafkaconnector";
    public static final String RESOURCE_GROUP = "kafka.strimzi.io";
    public static final String RESOURCE_KIND = "KafkaConnector";
    public static final String RESOURCE_LIST_KIND = RESOURCE_KIND + "List";
    public static final String SHORT_NAME = "kctr";

    private KafkaConnectorSpec spec;
    private KafkaConnectorStatus status;
    private Map<String, Object> additionalProperties;
    private ObjectMeta metadata;
    private String apiVersion;

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

    @Description("The specification of the Kafka Connector.")
    public KafkaConnectorSpec getSpec() {
        return spec;
    }

    public void setSpec(KafkaConnectorSpec spec) {
        this.spec = spec;
    }

    @Description("The status of the Kafka Connector.")
    public KafkaConnectorStatus getStatus() {
        return status;
    }

    public void setStatus(KafkaConnectorStatus status) {
        this.status = status;
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

