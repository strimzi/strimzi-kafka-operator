/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.template;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation of a Pod disruption Budget template template for Strimzi resources.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"metadata", "maxUnavailable"})
@DescriptionFile
@EqualsAndHashCode
@ToString
public class PodDisruptionBudgetTemplate implements HasMetadataTemplate, Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private MetadataTemplate metadata;
    private int maxUnavailable = 1;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Metadata to apply to the `PodDisruptionBudgetTemplate` resource.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public MetadataTemplate getMetadata() {
        return metadata;
    }

    public void setMetadata(MetadataTemplate metadata) {
        this.metadata = metadata;
    }

    @Description("Maximum number of unavailable pods to allow automatic Pod eviction. " +
            "A Pod eviction is allowed when the `maxUnavailable` number of pods or fewer are unavailable after the eviction. " +
            "Setting this value to 0 prevents all voluntary evictions, so the pods must be evicted manually. " +
            "Defaults to 1.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonProperty(defaultValue = "1")
    @Minimum(0)
    public int getMaxUnavailable() {
        return maxUnavailable;
    }

    public void setMaxUnavailable(int maxUnavailable) {
        this.maxUnavailable = maxUnavailable;
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
