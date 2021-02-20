/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;
import io.strimzi.api.kafka.model.status.KafkaTopicStatus;
import io.strimzi.crdgenerator.annotations.Crd;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import io.sundr.builder.annotations.BuildableReference;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

@JsonDeserialize
@Crd(
        spec = @Crd.Spec(
                names = @Crd.Spec.Names(
                        kind = KafkaTopic.RESOURCE_KIND,
                        plural = KafkaTopic.RESOURCE_PLURAL,
                        shortNames = {KafkaTopic.SHORT_NAME},
                        categories = {Constants.STRIMZI_CATEGORY}
                ),
                group = KafkaTopic.RESOURCE_GROUP,
                scope = KafkaTopic.SCOPE,
                versions = {
                        @Crd.Spec.Version(name = KafkaTopic.V1BETA2, served = true, storage = false),
                        @Crd.Spec.Version(name = KafkaTopic.V1BETA1, served = true, storage = true),
                        @Crd.Spec.Version(name = KafkaTopic.V1ALPHA1, served = true, storage = false)
                },
                subresources = @Crd.Spec.Subresources(
                        status = @Crd.Spec.Subresources.Status()
                ),
                additionalPrinterColumns = {
                        @Crd.Spec.AdditionalPrinterColumn(
                                name = "Cluster",
                                description = "The name of the Kafka cluster this topic belongs to",
                                jsonPath = ".metadata.labels.strimzi\\.io/cluster",
                                type = "string"
                        ),
                        @Crd.Spec.AdditionalPrinterColumn(
                                name = "Partitions",
                                description = "The desired number of partitions in the topic",
                                jsonPath = ".spec.partitions",
                                type = "integer"
                        ),
                        @Crd.Spec.AdditionalPrinterColumn(
                                name = "Replication factor",
                                description = "The desired number of replicas of each partition",
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
public class KafkaTopic extends CustomResource<KafkaTopicSpec, KafkaTopicStatus> implements Namespaced, UnknownPropertyPreserving {

    private static final long serialVersionUID = 1L;

    public static final String SCOPE = "Namespaced";
    public static final String V1ALPHA1 = Constants.V1ALPHA1;
    public static final String V1BETA1 = Constants.V1BETA1;
    public static final String V1BETA2 = Constants.V1BETA2;
    public static final String CONSUMED_VERSION = V1BETA2;
    public static final List<String> VERSIONS = unmodifiableList(asList(V1BETA2, V1BETA1, V1ALPHA1));
    public static final String RESOURCE_KIND = "KafkaTopic";
    public static final String RESOURCE_LIST_KIND = RESOURCE_KIND + "List";
    public static final String RESOURCE_GROUP = Constants.RESOURCE_GROUP_NAME;
    public static final String RESOURCE_PLURAL = "kafkatopics";
    public static final String RESOURCE_SINGULAR = "kafkatopic";
    public static final String CRD_API_VERSION = Constants.V1BETA1_API_VERSION;
    public static final String CRD_NAME = RESOURCE_PLURAL + "." + RESOURCE_GROUP;
    public static final String SHORT_NAME = "kt";
    public static final List<String> RESOURCE_SHORTNAMES = singletonList(SHORT_NAME);

    private String apiVersion;
    private ObjectMeta metadata;
    private KafkaTopicSpec spec;
    private KafkaTopicStatus status;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

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
        return metadata;
    }

    @Override
    public void setMetadata(ObjectMeta metadata) {
        this.metadata = metadata;
    }

    @Override
    @Description("The specification of the topic.")
    public KafkaTopicSpec getSpec() {
        return spec;
    }

    @Override
    public void setSpec(KafkaTopicSpec spec) {
        this.spec = spec;
    }

    @Override
    @Description("The status of the topic.")
    public KafkaTopicStatus getStatus() {
        return status;
    }

    @Override
    public void setStatus(KafkaTopicStatus status) {
        this.status = status;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : emptyMap();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(1);
        }
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
