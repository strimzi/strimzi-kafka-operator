/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.podset;

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
import io.strimzi.crdgenerator.annotations.DescriptionFile;
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
                        kind = StrimziPodSet.RESOURCE_KIND,
                        plural = StrimziPodSet.RESOURCE_PLURAL,
                        shortNames = {StrimziPodSet.SHORT_NAME},
                        categories = {Constants.STRIMZI_CATEGORY}
                ),
                group = StrimziPodSet.RESOURCE_GROUP,
                scope = StrimziPodSet.SCOPE,
                versions = {
                    @Crd.Spec.Version(name = StrimziPodSet.V1BETA2, served = true, storage = true)
                },
                subresources = @Crd.Spec.Subresources(
                    status = @Crd.Spec.Subresources.Status()
                ),
                additionalPrinterColumns = {
                    @Crd.Spec.AdditionalPrinterColumn(
                            name = "Pods",
                            description = "Number of pods managed by the StrimziPodSet",
                            jsonPath = ".status.pods",
                            type = "integer"
                        ),
                    @Crd.Spec.AdditionalPrinterColumn(
                            name = "Ready Pods",
                            description = "Number of ready pods managed by the StrimziPodSet",
                            jsonPath = ".status.readyPods",
                            type = "integer"
                        ),
                    @Crd.Spec.AdditionalPrinterColumn(
                            name = "Current Pods",
                            description = "Number of up-to-date pods managed by the StrimziPodSet",
                            jsonPath = ".status.currentPods",
                            type = "integer"
                        ),
                    @Crd.Spec.AdditionalPrinterColumn(
                            name = "Age",
                            description = "Age of the StrimziPodSet",
                            jsonPath = ".metadata.creationTimestamp",
                            type = "date"
                        )
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
@DescriptionFile
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@Version(Constants.V1BETA2)
@Group(Constants.RESOURCE_CORE_GROUP_NAME)
public class StrimziPodSet extends CustomResource<StrimziPodSetSpec, StrimziPodSetStatus> implements Namespaced, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    public static final String SCOPE = "Namespaced";
    public static final String V1BETA2 = Constants.V1BETA2;
    public static final List<String> VERSIONS = List.of(V1BETA2);
    public static final String RESOURCE_KIND = "StrimziPodSet";
    public static final String RESOURCE_LIST_KIND = RESOURCE_KIND + "List";
    public static final String RESOURCE_GROUP = Constants.RESOURCE_CORE_GROUP_NAME;
    public static final String RESOURCE_PLURAL = "strimzipodsets";
    public static final String RESOURCE_SINGULAR = "strimzipodset";
    public static final String CRD_NAME = RESOURCE_PLURAL + "." + RESOURCE_GROUP;
    public static final String SHORT_NAME = "sps";

    private final Map<String, Object> additionalProperties = new HashMap<>(0);

    // Added to avoid duplication during Json serialization
    private String apiVersion;
    private String kind;

    public StrimziPodSet() {
        super();
    }

    public StrimziPodSet(StrimziPodSetSpec spec, StrimziPodSetStatus status) {
        super();
        this.spec = spec;
        this.status = status;
    }

    @Override
    @Description("The specification of the StrimziPodSet.")
    public StrimziPodSetSpec getSpec() {
        return super.getSpec();
    }

    @Override
    @Description("The status of the StrimziPodSet.")
    public StrimziPodSetStatus getStatus() {
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