/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.template;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Pattern;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

/**
 * Representation for EmptyDir volume in additional volumes. This is used because the Fabric8 EmptyDirVolumeSource does
 * not serialize nicely into the Strimzi CRDs.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({"medium", "sizeLimit"})
@EqualsAndHashCode
@ToString
public class EmptyDirVolume implements UnknownPropertyPreserving {
    private EmptyDirMedium medium;
    private String sizeLimit;
    private Map<String, Object> additionalProperties;

    @Description("Medium represents the type of storage medium should back this volume. " +
            "Valid values are unset or `Memory`.")
    public EmptyDirMedium getMedium() {
        return medium;
    }

    public void setMedium(EmptyDirMedium medium) {
        this.medium = medium;
    }

    @Pattern(Constants.MEMORY_REGEX)
    @Description("The total amount of local storage required for this EmptyDir volume (for example 1Gi).")
    public String getSizeLimit() {
        return sizeLimit;
    }

    public void setSizeLimit(String sizeLimit) {
        this.sizeLimit = sizeLimit;
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
