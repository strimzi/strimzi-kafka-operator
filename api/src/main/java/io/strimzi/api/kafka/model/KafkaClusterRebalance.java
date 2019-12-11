/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.api.kafka.model.status.HasStatus;
import io.strimzi.api.kafka.model.status.KafkaClusterRebalanceStatus;
import io.strimzi.crdgenerator.annotations.Crd;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import io.sundr.builder.annotations.Inline;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

@JsonDeserialize
@Crd(
        apiVersion = KafkaClusterRebalance.CRD_API_VERSION,
        spec = @Crd.Spec(
                names = @Crd.Spec.Names(
                        kind = KafkaClusterRebalance.RESOURCE_KIND,
                        plural = KafkaClusterRebalance.RESOURCE_PLURAL,
                        shortNames = {KafkaClusterRebalance.SHORT_NAME}
                ),
                group = KafkaClusterRebalance.RESOURCE_GROUP,
                scope = KafkaClusterRebalance.SCOPE,
                version = KafkaClusterRebalance.V1ALPHA1,
                versions = {
                        @Crd.Spec.Version(
                                name = KafkaClusterRebalance.V1ALPHA1,
                                served = true,
                                storage = true
                        )
                },
                subresources = @Crd.Spec.Subresources(
                        status = @Crd.Spec.Subresources.Status()
                )
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
public class KafkaClusterRebalance extends CustomResource implements UnknownPropertyPreserving, HasStatus<KafkaClusterRebalanceStatus> {

    private static final long serialVersionUID = 1L;

    public static final String SCOPE = "Namespaced";
    public static final String V1ALPHA1 = "v1alpha1";
    public static final List<String> VERSIONS = unmodifiableList(asList(V1ALPHA1));
    public static final String RESOURCE_KIND = "KafkaClusterRebalance";
    public static final String RESOURCE_LIST_KIND = RESOURCE_KIND + "List";
    public static final String RESOURCE_GROUP = "kafka.strimzi.io";
    public static final String RESOURCE_PLURAL = "kafkaclusterrebalances";
    public static final String RESOURCE_SINGULAR = "kafkaclusterrebalance";
    public static final String CRD_API_VERSION = "apiextensions.k8s.io/v1beta1";
    public static final String CRD_NAME = RESOURCE_PLURAL + "." + RESOURCE_GROUP;
    public static final String SHORT_NAME = "kcr";
    public static final List<String> RESOURCE_SHORTNAMES = singletonList(SHORT_NAME);

    private String apiVersion;
    private ObjectMeta metadata;
    private KafkaClusterRebalanceSpec spec;
    private KafkaClusterRebalanceStatus status;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @JsonProperty("kind")
    @Override
    public String getKind() {
        return RESOURCE_KIND;
    }

    @Override
    public String getApiVersion() {
        return apiVersion;
    }

    @Override
    public void setApiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
    }

    @Override
    public ObjectMeta getMetadata() {
        return super.getMetadata();
    }

    @Override
    public void setMetadata(ObjectMeta metadata) {
        super.setMetadata(metadata);
    }

    @Description("The specification of the Kafka Cluster Rebalance.")
    public KafkaClusterRebalanceSpec getSpec() {
        return spec;
    }

    public void setSpec(KafkaClusterRebalanceSpec spec) {
        this.spec = spec;
    }

    @Override
    @Description("The status of the Kafka Cluster Rebalance.")
    public KafkaClusterRebalanceStatus getStatus() {
        return status;
    }

    public void setStatus(KafkaClusterRebalanceStatus status) {
        this.status = status;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    @Override
    public String toString() {
        YAMLMapper mapper = new YAMLMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
