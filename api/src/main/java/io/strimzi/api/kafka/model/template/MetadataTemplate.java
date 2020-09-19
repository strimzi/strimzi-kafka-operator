/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.template;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Representation of a Metadata template for Strimzi pods, statefulsets, deployments and services.
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "labels", "annotations"})
@DescriptionFile
@EqualsAndHashCode
public class MetadataTemplate implements Serializable, UnknownPropertyPreserving {
    private static final long serialVersionUID = 1L;

    private Map<String, String> labels = new HashMap<>(0);
    private Map<String, String> annotations = new HashMap<>(0);
    private Map<String, Object> additionalProperties = new HashMap<>(0);

    @Description("Labels added to the resource template. " +
            "Can be applied to different resources such as `StatefulSets`, `Deployments`, `Pods`, and `Services`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, String> getLabels() {
        return labels;
    }

    public void setLabels(Map<String, String> labels) {
        this.labels = labels;
    }

    @Description("Annotations added to the resource template. " +
            "Can be applied to different resources such as `StatefulSets`, `Deployments`, `Pods`, and `Services`.")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, String> getAnnotations() {
        return annotations;
    }

    public void setAnnotations(Map<String, String> annotations) {
        this.annotations = annotations;
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
