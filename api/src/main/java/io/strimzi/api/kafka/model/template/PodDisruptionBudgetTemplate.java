/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.template;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import io.vertx.core.cli.annotations.DefaultValue;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation of a Pod disruption Budget template template for Strimzi resources.
 */
@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "metadata", "maxUnavailable"})
public class PodDisruptionBudgetTemplate implements Serializable {
    private static final long serialVersionUID = 1L;

    private MetadataTemplate metadata;
    private int maxUnavailable = 1;
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Metadata which should be applied to the resource.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public MetadataTemplate getMetadata() {
        return metadata;
    }

    public void setMetadata(MetadataTemplate metadata) {
        this.metadata = metadata;
    }

    @Description("Maximum number of unavailable pods to allow voluntary Pod eviction. " +
            "An Pod eviction is allowed if at most \"maxUnavailable\" pods are unavailable after the eviction. " +
            "Setting this value to 0 will prevent all voluntary evictions and the pods will need to be evicted manually." +
            "Defaults to 1.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @DefaultValue("1")
    @Minimum(0)
    public int getMaxUnavailable() {
        return maxUnavailable;
    }

    public void setMaxUnavailable(int maxUnavailable) {
        this.maxUnavailable = maxUnavailable;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }
}
