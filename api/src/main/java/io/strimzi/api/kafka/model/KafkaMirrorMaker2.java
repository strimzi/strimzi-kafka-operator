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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;
import io.strimzi.api.kafka.model.status.KafkaMirrorMaker2Status;
import io.strimzi.crdgenerator.annotations.Crd;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import io.sundr.builder.annotations.BuildableReference;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

@JsonDeserialize
@Crd(
    spec = @Crd.Spec(
        names = @Crd.Spec.Names(
            kind = KafkaMirrorMaker2.RESOURCE_KIND,
            plural = KafkaMirrorMaker2.RESOURCE_PLURAL,
            shortNames = {KafkaMirrorMaker2.SHORT_NAME},
            categories = {Constants.STRIMZI_CATEGORY}
        ),
        group = KafkaMirrorMaker2.RESOURCE_GROUP,
        scope = KafkaMirrorMaker2.SCOPE,
        versions = {
            @Crd.Spec.Version(name = KafkaMirrorMaker2.V1BETA2, served = true, storage = false),
            @Crd.Spec.Version(name = KafkaMirrorMaker2.V1ALPHA1, served = true, storage = true)
        },
        subresources = @Crd.Spec.Subresources(
            status = @Crd.Spec.Subresources.Status(),
            scale = @Crd.Spec.Subresources.Scale(
                specReplicasPath = KafkaMirrorMaker2.SPEC_REPLICAS_PATH,
                statusReplicasPath = KafkaMirrorMaker2.STATUS_REPLICAS_PATH,
                labelSelectorPath = KafkaMirrorMaker2.LABEL_SELECTOR_PATH
            )
        ),
        additionalPrinterColumns = {
            @Crd.Spec.AdditionalPrinterColumn(
                name = "Desired replicas",
                description = "The desired number of Kafka MirrorMaker 2.0 replicas",
                jsonPath = ".spec.replicas",
                type = "integer"),
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
        generateBuilderPackage = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API,
        refs = {@BuildableReference(CustomResource.class)}
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "apiVersion", "kind", "metadata", "spec", "status" })
@EqualsAndHashCode
@Version(Constants.V1BETA2)
@Group(Constants.RESOURCE_GROUP_NAME)
@SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
public class KafkaMirrorMaker2 extends CustomResource<KafkaMirrorMaker2Spec, KafkaMirrorMaker2Status> implements Namespaced, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    public static final String SCOPE = "Namespaced";
    public static final String V1BETA2 = Constants.V1BETA2;
    public static final String V1ALPHA1 = Constants.V1ALPHA1;
    public static final String CONSUMED_VERSION = V1BETA2;
    public static final List<String> VERSIONS = List.of(V1BETA2, V1ALPHA1);
    public static final String RESOURCE_KIND = "KafkaMirrorMaker2";
    public static final String RESOURCE_LIST_KIND = RESOURCE_KIND + "List";
    public static final String RESOURCE_GROUP = Constants.RESOURCE_GROUP_NAME;
    public static final String RESOURCE_PLURAL = "kafkamirrormaker2s";
    public static final String RESOURCE_SINGULAR = "kafkamirrormaker2";
    public static final String CRD_NAME = RESOURCE_PLURAL + "." + RESOURCE_GROUP;
    public static final String SHORT_NAME = "kmm2";
    public static final List<String> RESOURCE_SHORTNAMES = List.of(SHORT_NAME);
    public static final String SPEC_REPLICAS_PATH = ".spec.replicas";
    public static final String STATUS_REPLICAS_PATH = ".status.replicas";
    public static final String LABEL_SELECTOR_PATH = ".status.labelSelector";

    private final Map<String, Object> additionalProperties = new HashMap<>(0);

    // Added to avoid duplication during Json serialization
    private String apiVersion;
    private String kind;

    public KafkaMirrorMaker2() {
        super();
    }

    public KafkaMirrorMaker2(KafkaMirrorMaker2Spec spec, KafkaMirrorMaker2Status status) {
        super();
        this.spec = spec;
        this.status = status;
    }

    @Override
    @Description("The specification of the Kafka MirrorMaker 2.0 cluster.")
    public KafkaMirrorMaker2Spec getSpec() {
        return super.getSpec();
    }

    @Override
    @Description("The status of the Kafka MirrorMaker 2.0 cluster.")
    public KafkaMirrorMaker2Status getStatus() {
        return super.getStatus();
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

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    /**
     * Returns a predicate that determines if KafkaMirrorMaker2 is ready. A KafkaMirrorMaker2 CRD is
     * ready if the observedGeneration of its status is equal to the generation of its metadata
     * and any of the conditions of its status has type:"Ready" and status:"True"
     * <p>
     * See {@link CustomResourceConditions CustomResourceConditions} for explanation/examples
     *
     * @return a predicate that checks if a KafkaMirrorMaker2 is ready
     */
    public static Predicate<KafkaMirrorMaker2> isReady() {
        return CustomResourceConditions.isReady();
    }
}
