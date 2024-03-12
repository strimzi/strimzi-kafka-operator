/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.bridge;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.CustomResourceConditions;
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
import java.util.function.Predicate;

import static java.util.Collections.emptyMap;

@JsonDeserialize
@Crd(
    spec = @Crd.Spec(
        names = @Crd.Spec.Names(
            kind = KafkaBridge.RESOURCE_KIND,
            plural = KafkaBridge.RESOURCE_PLURAL,
            shortNames = {KafkaBridge.SHORT_NAME},
            categories = {Constants.STRIMZI_CATEGORY}
        ),
        group = KafkaBridge.RESOURCE_GROUP,
        scope = KafkaBridge.SCOPE,
        versions = {
            @Crd.Spec.Version(name = KafkaBridge.V1BETA2, served = true, storage = false),
            @Crd.Spec.Version(name = KafkaBridge.V1ALPHA1, served = true, storage = true)
        },
        subresources = @Crd.Spec.Subresources(
            status = @Crd.Spec.Subresources.Status(),
            scale = @Crd.Spec.Subresources.Scale(
                specReplicasPath = KafkaBridge.SPEC_REPLICAS_PATH,
                statusReplicasPath = KafkaBridge.STATUS_REPLICAS_PATH,
                labelSelectorPath = KafkaBridge.LABEL_SELECTOR_PATH
            )
        ),
        additionalPrinterColumns = {
            @Crd.Spec.AdditionalPrinterColumn(
                name = "Desired replicas",
                description = "The desired number of Kafka Bridge replicas",
                jsonPath = ".spec.replicas",
                type = "integer"),
            @Crd.Spec.AdditionalPrinterColumn(
                name = "Bootstrap Servers",
                description = "The boostrap servers",
                jsonPath = ".spec.bootstrapServers",
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
@EqualsAndHashCode
@Version(Constants.V1BETA2)
@Group(Constants.RESOURCE_GROUP_NAME)
@ToString
public class KafkaBridge extends CustomResource<KafkaBridgeSpec, KafkaBridgeStatus> implements Namespaced, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    public static final String SCOPE = "Namespaced";
    public static final String V1BETA2 = Constants.V1BETA2;
    public static final String V1ALPHA1 = Constants.V1ALPHA1;
    public static final String CONSUMED_VERSION = V1BETA2;
    public static final List<String> VERSIONS = List.of(V1BETA2, V1ALPHA1);
    public static final String RESOURCE_KIND = "KafkaBridge";
    public static final String RESOURCE_LIST_KIND = RESOURCE_KIND + "List";
    public static final String RESOURCE_GROUP = Constants.RESOURCE_GROUP_NAME;
    public static final String RESOURCE_PLURAL = "kafkabridges";
    public static final String RESOURCE_SINGULAR = "kafkabridge";
    public static final String CRD_NAME = RESOURCE_PLURAL + "." + RESOURCE_GROUP;
    public static final String SHORT_NAME = "kb";
    public static final List<String> RESOURCE_SHORTNAMES = List.of(SHORT_NAME);
    public static final String SPEC_REPLICAS_PATH = ".spec.replicas";
    public static final String STATUS_REPLICAS_PATH = ".status.replicas";
    public static final String LABEL_SELECTOR_PATH = ".status.labelSelector";

    // Added to avoid duplication during Json serialization
    private String apiVersion;
    private String kind;

    private final Map<String, Object> additionalProperties = new HashMap<>(0);

    public KafkaBridge() {
        super();
    }

    public KafkaBridge(KafkaBridgeSpec spec, KafkaBridgeStatus status) {
        super();
        this.spec = spec;
        this.status = status;
    }
    
    @Override
    @Description("The specification of the Kafka Bridge.")
    public KafkaBridgeSpec getSpec() {
        return super.getSpec();
    }

    @Override
    @Description("The status of the Kafka Bridge.")
    public KafkaBridgeStatus getStatus() {
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

    /**
     * Returns a predicate that determines if KafkaBridge is ready. A KafkaBridge CRD is
     * ready if the observedGeneration of its status is equal to the generation of its metadata
     * and any of the conditions of its status has type:"Ready" and status:"True"
     * <p>
     * See {@link CustomResourceConditions CustomResourceConditions} for explanation/examples
     *
     * @return a predicate that checks if a KafkaBridge is ready
     */
    public static Predicate<KafkaBridge> isReady() {
        return CustomResourceConditions.isReady();
    }

}
