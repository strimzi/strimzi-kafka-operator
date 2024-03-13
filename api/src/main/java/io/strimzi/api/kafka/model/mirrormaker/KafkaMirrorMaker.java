/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.mirrormaker;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;
import io.strimzi.api.annotations.DeprecatedType;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.crdgenerator.annotations.Crd;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import io.sundr.builder.annotations.BuildableReference;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;

@JsonDeserialize
@Crd(
    spec = @Crd.Spec(
        names = @Crd.Spec.Names(
            kind = KafkaMirrorMaker.RESOURCE_KIND,
            plural = KafkaMirrorMaker.RESOURCE_PLURAL,
            shortNames = {KafkaMirrorMaker.SHORT_NAME},
            categories = {Constants.STRIMZI_CATEGORY}
        ),
        group = KafkaMirrorMaker.RESOURCE_GROUP,
        scope = KafkaMirrorMaker.SCOPE,
        versions = {
            @Crd.Spec.Version(name = KafkaMirrorMaker.V1BETA2, served = true, storage = false),
            @Crd.Spec.Version(name = KafkaMirrorMaker.V1BETA1, served = true, storage = true),
            @Crd.Spec.Version(name = KafkaMirrorMaker.V1ALPHA1, served = true, storage = false)
        },
        subresources = @Crd.Spec.Subresources(
            status = @Crd.Spec.Subresources.Status(),
            scale = @Crd.Spec.Subresources.Scale(
                specReplicasPath = KafkaMirrorMaker.SPEC_REPLICAS_PATH,
                statusReplicasPath = KafkaMirrorMaker.STATUS_REPLICAS_PATH,
                labelSelectorPath = KafkaMirrorMaker.LABEL_SELECTOR_PATH
            )
        ),
        additionalPrinterColumns = {
            @Crd.Spec.AdditionalPrinterColumn(
                name = "Desired replicas",
                description = "The desired number of Kafka MirrorMaker replicas",
                jsonPath = ".spec.replicas",
                type = "integer"),
            @Crd.Spec.AdditionalPrinterColumn(
                name = "Consumer Bootstrap Servers",
                description = "The boostrap servers for the consumer",
                jsonPath = ".spec.consumer.bootstrapServers",
                type = "string",
                priority = 1),
            @Crd.Spec.AdditionalPrinterColumn(
                name = "Producer Bootstrap Servers",
                description = "The boostrap servers for the producer",
                jsonPath = ".spec.producer.bootstrapServers",
                type = "string",
                priority = 1),
            @Crd.Spec.AdditionalPrinterColumn(
                name = "Ready",
                description = "The state of the custom resource",
                jsonPath = ".status.conditions[?(@.type==\"Ready\")].status",
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
@Deprecated
@DeprecatedType(replacedWithType = KafkaMirrorMaker2.class)
public class KafkaMirrorMaker extends CustomResource<KafkaMirrorMakerSpec, KafkaMirrorMakerStatus> implements Namespaced, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    public static final String SCOPE = "Namespaced";
    public static final String V1ALPHA1 = Constants.V1ALPHA1;
    public static final String V1BETA1 = Constants.V1BETA1;
    public static final String V1BETA2 = Constants.V1BETA2;
    public static final String CONSUMED_VERSION = V1BETA2;
    public static final List<String> VERSIONS = List.of(V1BETA2, V1BETA1, V1ALPHA1);
    public static final String RESOURCE_KIND = "KafkaMirrorMaker";
    public static final String RESOURCE_LIST_KIND = RESOURCE_KIND + "List";
    public static final String RESOURCE_GROUP = Constants.RESOURCE_GROUP_NAME;
    public static final String RESOURCE_PLURAL = "kafkamirrormakers";
    public static final String RESOURCE_SINGULAR = "kafkamirrormaker";
    public static final String CRD_NAME = RESOURCE_PLURAL + "." + RESOURCE_GROUP;
    public static final String SHORT_NAME = "kmm";
    public static final List<String> RESOURCE_SHORTNAMES = List.of(SHORT_NAME);
    public static final String SPEC_REPLICAS_PATH = ".spec.replicas";
    public static final String STATUS_REPLICAS_PATH = ".status.replicas";
    public static final String LABEL_SELECTOR_PATH = ".status.labelSelector";

    private final Map<String, Object> additionalProperties = new HashMap<>(0);

    // Added to avoid duplication during Json serialization
    private String apiVersion;
    private String kind;

    public KafkaMirrorMaker() {
        super();
    }

    public KafkaMirrorMaker(KafkaMirrorMakerSpec spec, KafkaMirrorMakerStatus status) {
        super();
        this.spec = spec;
        this.status = status;
    }

    @Override
    @Description("The specification of Kafka MirrorMaker.")
    public KafkaMirrorMakerSpec getSpec() {
        return super.getSpec();
    }

    @Override
    @Description("The status of Kafka MirrorMaker.")
    public KafkaMirrorMakerStatus getStatus() {
        return super.getStatus();
    }
    
    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : emptyMap();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}
