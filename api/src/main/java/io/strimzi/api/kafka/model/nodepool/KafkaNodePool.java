/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.nodepool;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Crd;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import io.sundr.builder.annotations.BuildableReference;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents the KafkaNodePool resource
 */
@JsonDeserialize
@Crd(
        spec = @Crd.Spec(
                names = @Crd.Spec.Names(
                        kind = io.strimzi.api.kafka.model.nodepool.KafkaNodePool.RESOURCE_KIND,
                        plural = io.strimzi.api.kafka.model.nodepool.KafkaNodePool.RESOURCE_PLURAL,
                        shortNames = {"knp"},
                        categories = {Constants.STRIMZI_CATEGORY}
                ),
                group = io.strimzi.api.kafka.model.nodepool.KafkaNodePool.RESOURCE_GROUP,
                scope = io.strimzi.api.kafka.model.nodepool.KafkaNodePool.SCOPE,
                versions = {
                    @Crd.Spec.Version(name = Constants.V1BETA2, served = true, storage = true)
                },
                subresources = @Crd.Spec.Subresources(
                        status = @Crd.Spec.Subresources.Status(),
                        scale = @Crd.Spec.Subresources.Scale(
                                specReplicasPath = ".spec.replicas",
                                statusReplicasPath = ".status.replicas",
                                labelSelectorPath = ".status.labelSelector"
                        )
                ),
                additionalPrinterColumns = {
                    @Crd.Spec.AdditionalPrinterColumn(
                                name = "Desired replicas",
                                description = "The desired number of replicas",
                                jsonPath = ".spec.replicas",
                                type = "integer"),
                    @Crd.Spec.AdditionalPrinterColumn(
                                name = "Roles",
                                description = "Roles of the nodes in the pool",
                                jsonPath = ".status.roles",
                                type = "string"),
                    @Crd.Spec.AdditionalPrinterColumn(
                        name = "NodeIds",
                        description = "Node IDs used by Kafka nodes in this pool",
                        jsonPath = ".status.nodeIds",
                        type = "string")
                }
        )
)
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API,
        refs = {@BuildableReference(CustomResource.class)}
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"apiVersion", "kind", "metadata", "spec", "status"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@Version(Constants.V1BETA2)
@Group(Constants.RESOURCE_GROUP_NAME)
public class KafkaNodePool extends CustomResource<KafkaNodePoolSpec, KafkaNodePoolStatus> implements Namespaced, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    public static final String SCOPE = Constants.SCOPE_NAMESPACED;
    public static final List<String> VERSIONS = List.of(Constants.V1BETA2);
    public static final String RESOURCE_KIND = "KafkaNodePool";
    public static final String RESOURCE_LIST_KIND = RESOURCE_KIND + "List";
    public static final String RESOURCE_GROUP = Constants.RESOURCE_GROUP_NAME;
    public static final String RESOURCE_PLURAL = "kafkanodepools";
    public static final String RESOURCE_SINGULAR = "kafkanodepool";

    private Map<String, Object> additionalProperties;

    // Added to avoid duplication during JSON serialization
    @SuppressWarnings({"UnusedDeclaration"})
    private String apiVersion;
    @SuppressWarnings({"UnusedDeclaration"})
    private String kind;

    public KafkaNodePool() {
        super();
    }

    public KafkaNodePool(KafkaNodePoolSpec spec, KafkaNodePoolStatus status) {
        super();
        this.spec = spec;
        this.status = status;
    }

    @Override
    @Description("The specification of the KafkaNodePool.")
    public KafkaNodePoolSpec getSpec() {
        return super.getSpec();
    }

    @Override
    @Description("The status of the KafkaNodePool.")
    public KafkaNodePoolStatus getStatus() {
        return super.getStatus();
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : Map.of();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(2);
        }
        this.additionalProperties.put(name, value);
    }
}
