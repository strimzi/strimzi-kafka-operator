/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;
import io.strimzi.api.annotations.DeprecatedType;
import io.strimzi.api.kafka.model.status.KafkaConnectS2IStatus;
import io.strimzi.crdgenerator.annotations.Crd;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import io.sundr.builder.annotations.BuildableReference;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

@JsonDeserialize(
        using = JsonDeserializer.None.class
)
@Crd(
        spec = @Crd.Spec(
                names = @Crd.Spec.Names(
                        kind = KafkaConnectS2I.RESOURCE_KIND,
                        plural = KafkaConnectS2I.RESOURCE_PLURAL,
                        shortNames = {KafkaConnectS2I.SHORT_NAME},
                        categories = {Constants.STRIMZI_CATEGORY}
                ),
                group = KafkaConnectS2I.RESOURCE_GROUP,
                scope = KafkaConnectS2I.SCOPE,
                versions = {
                        @Crd.Spec.Version(name = KafkaConnectS2I.V1BETA2, served = true, storage = false),
                        @Crd.Spec.Version(name = KafkaConnectS2I.V1BETA1, served = true, storage = true),
                        @Crd.Spec.Version(name = KafkaConnectS2I.V1ALPHA1, served = true, storage = false)
                },
                subresources = @Crd.Spec.Subresources(
                        status = @Crd.Spec.Subresources.Status(),
                        scale = @Crd.Spec.Subresources.Scale(
                                specReplicasPath = KafkaConnectS2I.SPEC_REPLICAS_PATH,
                                statusReplicasPath = KafkaConnectS2I.STATUS_REPLICAS_PATH,
                                labelSelectorPath = KafkaConnectS2I.LABEL_SELECTOR_PATH
                        )
                ),
                additionalPrinterColumns = {
                        @Crd.Spec.AdditionalPrinterColumn(
                                name = "Desired replicas",
                                description = "The desired number of Kafka Connect replicas",
                                jsonPath = ".spec.replicas",
                                type = "integer"
                        ),
                        @Crd.Spec.AdditionalPrinterColumn(
                                name = "Ready",
                                description = "The state of the custom resource",
                                jsonPath = ".status.conditions[?(@.type==\"Ready\")].status",
                                type = "string"
                        )
                }
        )
)
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API,
        refs = {@BuildableReference(ObjectMeta.class)}
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"apiVersion", "kind", "metadata", "spec", "status"})
@EqualsAndHashCode
@Version(Constants.V1BETA2)
@Group(Constants.STRIMZI_GROUP)
@Deprecated
@DeprecatedType(replacedWithType = io.strimzi.api.kafka.model.connect.build.Build.class)
public class KafkaConnectS2I extends CustomResource<KafkaConnectS2ISpec, KafkaConnectS2IStatus> implements Namespaced, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    public static final String SCOPE = "Namespaced";
    public static final String V1ALPHA1 = Constants.V1ALPHA1;
    public static final String V1BETA1 = Constants.V1BETA1;
    public static final String V1BETA2 = Constants.V1BETA2;
    public static final String CONSUMED_VERSION = V1BETA2;
    public static final List<String> VERSIONS = unmodifiableList(asList(V1BETA2, V1BETA1, V1ALPHA1));
    public static final String RESOURCE_KIND = "KafkaConnectS2I";
    public static final String RESOURCE_LIST_KIND = RESOURCE_KIND + "List";
    public static final String RESOURCE_GROUP = Constants.RESOURCE_GROUP_NAME;
    public static final String RESOURCE_PLURAL = "kafkaconnects2is";
    public static final String RESOURCE_SINGULAR = "kafkaconnects2i";
    public static final String CRD_API_VERSION = Constants.V1BETA1_API_VERSION;
    public static final String CRD_NAME = RESOURCE_PLURAL + "." + RESOURCE_GROUP;
    public static final String SHORT_NAME = "kcs2i";
    public static final List<String> RESOURCE_SHORTNAMES = singletonList(SHORT_NAME);
    public static final String SPEC_REPLICAS_PATH = ".spec.replicas";
    public static final String STATUS_REPLICAS_PATH = ".status.replicas";
    public static final String LABEL_SELECTOR_PATH = ".status.labelSelector";

    private String apiVersion;
    private ObjectMeta metadata;
    private KafkaConnectS2ISpec spec;
    private KafkaConnectS2IStatus status;
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
        return metadata;
    }

    @Override
    public void setMetadata(ObjectMeta metadata) {
        this.metadata = metadata;
    }

    @Override
    @Description("The specification of the Kafka Connect Source-to-Image (S2I) cluster.")
    public KafkaConnectS2ISpec getSpec() {
        return spec;
    }

    @Override
    public void setSpec(KafkaConnectS2ISpec spec) {
        this.spec = spec;
    }

    @Override
    @Description("The status of the Kafka Connect Source-to-Image (S2I) cluster.")
    public KafkaConnectS2IStatus getStatus() {
        return status;
    }

    @Override
    public void setStatus(KafkaConnectS2IStatus status) {
        this.status = status;
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

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}
