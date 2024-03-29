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
                        shortNames = {io.strimzi.api.kafka.model.nodepool.KafkaNodePool.SHORT_NAME},
                        categories = {Constants.STRIMZI_CATEGORY}
                ),
                group = io.strimzi.api.kafka.model.nodepool.KafkaNodePool.RESOURCE_GROUP,
                scope = io.strimzi.api.kafka.model.nodepool.KafkaNodePool.SCOPE,
                versions = {
                    @Crd.Spec.Version(name = io.strimzi.api.kafka.model.nodepool.KafkaNodePool.V1BETA2, served = true, storage = true)
                },
                subresources = @Crd.Spec.Subresources(
                        status = @Crd.Spec.Subresources.Status(),
                        scale = @Crd.Spec.Subresources.Scale(
                                specReplicasPath = KafkaNodePool.SPEC_REPLICAS_PATH,
                                statusReplicasPath = KafkaNodePool.STATUS_REPLICAS_PATH,
                                labelSelectorPath = KafkaNodePool.LABEL_SELECTOR_PATH
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

    public static final String SCOPE = "Namespaced";
    public static final String V1BETA2 = Constants.V1BETA2;
    public static final List<String> VERSIONS = List.of(V1BETA2);
    public static final String RESOURCE_KIND = "KafkaNodePool";
    public static final String RESOURCE_LIST_KIND = RESOURCE_KIND + "List";
    public static final String RESOURCE_GROUP = Constants.RESOURCE_GROUP_NAME;
    public static final String RESOURCE_PLURAL = "kafkanodepools";
    public static final String RESOURCE_SINGULAR = "kafkanodepool";
    public static final String CRD_NAME = RESOURCE_PLURAL + "." + RESOURCE_GROUP;
    public static final String SHORT_NAME = "knp";
    public static final String SPEC_REPLICAS_PATH = ".spec.replicas";
    public static final String STATUS_REPLICAS_PATH = ".status.replicas";
    public static final String LABEL_SELECTOR_PATH = ".status.labelSelector";

    private final Map<String, Object> additionalProperties = new HashMap<>(0);

    // Added to avoid duplication during Json serialization
    private String apiVersion;
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
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}